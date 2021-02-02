package scalers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	v2beta2 "k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type kafkaScaler struct {
	metadata kafkaMetadata
	client   sarama.Client
	admin    sarama.ClusterAdmin
	tsdb     *metricsDB
}

type metricsDB struct {
	ctx             context.Context
	db              map[time.Time]int64
	cleanupInterval time.Duration
	timeWindow      time.Duration
	timer           *time.Timer
	mu              *sync.Mutex
}

func newMetricsDB(ctx context.Context, timeWindow time.Duration, scrapeInterval time.Duration) (*metricsDB, error) {
	db := &metricsDB{
		ctx:             ctx,
		db:              make(map[time.Time]int64, 0),
		cleanupInterval: timeWindow + scrapeInterval,
		timeWindow:      timeWindow,
		timer:           time.NewTimer(timeWindow + scrapeInterval),
		mu:              &sync.Mutex{},
	}

	go db.runCleaner()
	return db, nil
}

func (m *metricsDB) add(value int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	m.db[now] = value
	return nil
}

func (m *metricsDB) cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	before := time.Now().Add(-m.timeWindow)
	for k, _ := range m.db {
		if k.Before(before) {
			delete(m.db, k)
		}
	}
	return nil
}

func (m *metricsDB) getMetricForTimeWindow() (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value := int64(0)
	after := time.Now().Add(-m.timeWindow)
	for k, v := range m.db {
		if k.After(after) {
			value += v
		}
	}
	computedValue := float64(value) / m.timeWindow.Seconds()
	return int64(math.Round(computedValue)), nil
}

func (m *metricsDB) runCleaner() {
	for {
		select {
		case <-m.ctx.Done():
			kafkaLog.Info("Shutting down cleaner go routine")
			return
		case <-m.timer.C:
			kafkaLog.Info("cleanup expired metrics")
			m.cleanup()
			m.timer.Reset(m.cleanupInterval)
		}
	}
}

type kafkaMetadata struct {
	bootstrapServers  []string
	group             string
	topic             string
	lagThreshold      int64
	offsetResetPolicy offsetResetPolicy

	// SASL
	saslType kafkaSaslType
	username string
	password string

	// TLS
	enableTLS bool
	cert      string
	key       string
	ca        string

	// TimeWindow
	timeWindow time.Duration
}

type offsetResetPolicy string

const (
	latest   offsetResetPolicy = "latest"
	earliest offsetResetPolicy = "earliest"
)

type kafkaSaslType string

// supported SASL types
const (
	KafkaSASLTypeNone        kafkaSaslType = "none"
	KafkaSASLTypePlaintext   kafkaSaslType = "plaintext"
	KafkaSASLTypeSCRAMSHA256 kafkaSaslType = "scram_sha256"
	KafkaSASLTypeSCRAMSHA512 kafkaSaslType = "scram_sha512"
)

const (
	lagThresholdMetricName   = "lagThreshold"
	kafkaMetricType          = "External"
	defaultKafkaLagThreshold = 10
	defaultOffsetResetPolicy = latest
	invalidOffset            = -1
)

var kafkaLog = logf.Log.WithName("kafka_scaler")

// NewKafkaScaler creates a new kafkaScaler
func NewKafkaScaler(config *ScalerConfig) (Scaler, error) {
	kafkaMetadata, err := parseKafkaMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing kafka metadata: %s", err)
	}

	client, admin, err := getKafkaClients(kafkaMetadata)
	if err != nil {
		return nil, err
	}

	var tsdb *metricsDB
	if kafkaMetadata.timeWindow != time.Duration(0) {
		tsdb, _ = newMetricsDB(context.Background(), kafkaMetadata.timeWindow, time.Second*30)
	}

	return &kafkaScaler{
		client:   client,
		admin:    admin,
		metadata: kafkaMetadata,
		tsdb:     tsdb,
	}, nil
}

func parseKafkaMetadata(config *ScalerConfig) (kafkaMetadata, error) {
	meta := kafkaMetadata{}
	switch {
	case config.TriggerMetadata["bootstrapServersFromEnv"] != "":
		meta.bootstrapServers = strings.Split(config.ResolvedEnv[config.TriggerMetadata["bootstrapServersFromEnv"]], ",")
	case config.TriggerMetadata["bootstrapServers"] != "":
		meta.bootstrapServers = strings.Split(config.TriggerMetadata["bootstrapServers"], ",")
	default:
		return meta, errors.New("no bootstrapServers given")
	}

	switch {
	case config.TriggerMetadata["consumerGroupFromEnv"] != "":
		meta.group = config.ResolvedEnv[config.TriggerMetadata["consumerGroupFromEnv"]]
	case config.TriggerMetadata["consumerGroup"] != "":
		meta.group = config.TriggerMetadata["consumerGroup"]
	default:
		return meta, errors.New("no consumer group given")
	}

	switch {
	case config.TriggerMetadata["topicFromEnv"] != "":
		meta.topic = config.ResolvedEnv[config.TriggerMetadata["topicFromEnv"]]
	case config.TriggerMetadata["topic"] != "":
		meta.topic = config.TriggerMetadata["topic"]
	default:
		return meta, errors.New("no topic given")
	}

	meta.offsetResetPolicy = defaultOffsetResetPolicy

	if config.TriggerMetadata["offsetResetPolicy"] != "" {
		policy := offsetResetPolicy(config.TriggerMetadata["offsetResetPolicy"])
		if policy != earliest && policy != latest {
			return meta, fmt.Errorf("err offsetResetPolicy policy %s given", policy)
		}
		meta.offsetResetPolicy = policy
	}

	meta.lagThreshold = defaultKafkaLagThreshold

	if val, ok := config.TriggerMetadata[lagThresholdMetricName]; ok {
		t, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return meta, fmt.Errorf("error parsing %s: %s", lagThresholdMetricName, err)
		}
		meta.lagThreshold = t
	}

	meta.saslType = KafkaSASLTypeNone
	if val, ok := config.AuthParams["sasl"]; ok {
		val = strings.TrimSpace(val)
		mode := kafkaSaslType(val)

		if mode == KafkaSASLTypePlaintext || mode == KafkaSASLTypeSCRAMSHA256 || mode == KafkaSASLTypeSCRAMSHA512 {
			if config.AuthParams["username"] == "" {
				return meta, errors.New("no username given")
			}
			meta.username = strings.TrimSpace(config.AuthParams["username"])

			if config.AuthParams["password"] == "" {
				return meta, errors.New("no password given")
			}
			meta.password = strings.TrimSpace(config.AuthParams["password"])
			meta.saslType = mode
		} else {
			return meta, fmt.Errorf("err SASL mode %s given", mode)
		}
	}

	// Parse timeWindow is specified
	if val, ok := config.TriggerMetadata["timeWindow"]; ok {
		parsedSeconds, err := strconv.Atoi(val)
		if err != nil {
			return meta, fmt.Errorf("unable to parse %s to integer. err=%v", val, err.Error())
		}
		meta.timeWindow = time.Duration(parsedSeconds) * time.Second
	}

	meta.enableTLS = false
	if val, ok := config.AuthParams["tls"]; ok {
		val = strings.TrimSpace(val)

		if val == "enable" {
			certGiven := config.AuthParams["cert"] != ""
			keyGiven := config.AuthParams["key"] != ""
			if certGiven && !keyGiven {
				return meta, errors.New("key must be provided with cert")
			}
			if keyGiven && !certGiven {
				return meta, errors.New("cert must be provided with key")
			}
			meta.ca = config.AuthParams["ca"]
			meta.cert = config.AuthParams["cert"]
			meta.key = config.AuthParams["key"]
			meta.enableTLS = true
		} else {
			return meta, fmt.Errorf("err incorrect value for TLS given: %s", val)
		}
	}

	return meta, nil
}

// IsActive determines if we need to scale from zero
func (s *kafkaScaler) IsActive(ctx context.Context) (bool, error) {
	partitions, err := s.getPartitions()
	if err != nil {
		return false, err
	}

	offsets, err := s.getOffsets(partitions)
	if err != nil {
		return false, err
	}

	for _, partition := range partitions {
		lag, err := s.getLagForPartition(partition, offsets)
		if err != nil && lag == invalidOffset {
			return true, nil
		}
		kafkaLog.V(1).Info(fmt.Sprintf("Group %s has a lag of %d for topic %s and partition %d\n", s.metadata.group, lag, s.metadata.topic, partition))

		// Return as soon as a lag was detected for any partition
		if lag > 0 {
			return true, nil
		}
	}

	return false, nil
}

func getKafkaClients(metadata kafkaMetadata) (sarama.Client, sarama.ClusterAdmin, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0

	if metadata.saslType != KafkaSASLTypeNone {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = metadata.username
		config.Net.SASL.Password = metadata.password
	}

	if metadata.enableTLS {
		config.Net.TLS.Enable = true
		tlsConfig, err := kedautil.NewTLSConfig(metadata.cert, metadata.key, metadata.ca)
		if err != nil {
			return nil, nil, err
		}
		config.Net.TLS.Config = tlsConfig
	}

	if metadata.saslType == KafkaSASLTypePlaintext {
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	if metadata.saslType == KafkaSASLTypeSCRAMSHA256 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	}

	if metadata.saslType == KafkaSASLTypeSCRAMSHA512 {
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	}

	client, err := sarama.NewClient(metadata.bootstrapServers, config)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating kafka client: %s", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		if !client.Closed() {
			client.Close()
		}
		return nil, nil, fmt.Errorf("error creating kafka admin: %s", err)
	}

	return client, admin, nil
}

func (s *kafkaScaler) getPartitions() ([]int32, error) {
	topicsMetadata, err := s.admin.DescribeTopics([]string{s.metadata.topic})
	if err != nil {
		return nil, fmt.Errorf("error describing topics: %s", err)
	}
	if len(topicsMetadata) != 1 {
		return nil, fmt.Errorf("expected only 1 topic metadata, got %d", len(topicsMetadata))
	}

	partitionMetadata := topicsMetadata[0].Partitions
	partitions := make([]int32, len(partitionMetadata))
	for i, p := range partitionMetadata {
		partitions[i] = p.ID
	}

	return partitions, nil
}

func (s *kafkaScaler) getOffsets(partitions []int32) (*sarama.OffsetFetchResponse, error) {
	offsets, err := s.admin.ListConsumerGroupOffsets(s.metadata.group, map[string][]int32{
		s.metadata.topic: partitions,
	})

	if err != nil {
		return nil, fmt.Errorf("error listing consumer group offsets: %s", err)
	}

	return offsets, nil
}

func (s *kafkaScaler) getLagForPartition(partition int32, offsets *sarama.OffsetFetchResponse) (int64, error) {
	block := offsets.GetBlock(s.metadata.topic, partition)
	if block == nil {
		kafkaLog.Error(fmt.Errorf("error finding offset block for topic %s and partition %d", s.metadata.topic, partition), "")
		return 0, fmt.Errorf("error finding offset block for topic %s and partition %d", s.metadata.topic, partition)
	}
	consumerOffset := block.Offset
	if consumerOffset == invalidOffset && s.metadata.offsetResetPolicy == latest {
		kafkaLog.V(0).Info(fmt.Sprintf("invalid offset found for topic %s in group %s and partition %d, probably no offset is committed yet", s.metadata.topic, s.metadata.group, partition))
		return invalidOffset, fmt.Errorf("invalid offset found for topic %s in group %s and partition %d, probably no offset is committed yet", s.metadata.topic, s.metadata.group, partition)
	}
	latestOffset, err := s.client.GetOffset(s.metadata.topic, partition, sarama.OffsetNewest)
	if err != nil {
		kafkaLog.Error(err, fmt.Sprintf("error finding latest offset for topic %s and partition %d\n", s.metadata.topic, partition))
		return 0, fmt.Errorf("error finding latest offset for topic %s and partition %d", s.metadata.topic, partition)
	}
	if consumerOffset == invalidOffset && s.metadata.offsetResetPolicy == earliest {
		return latestOffset, nil
	}
	return latestOffset - consumerOffset, nil
}

// Close closes the kafka admin and client
func (s *kafkaScaler) Close() error {
	// underlying client will also be closed on admin's Close() call
	err := s.admin.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *kafkaScaler) GetMetricSpecForScaling() []v2beta2.MetricSpec {
	targetMetricValue := resource.NewQuantity(s.metadata.lagThreshold, resource.DecimalSI)
	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: kedautil.NormalizeString(fmt.Sprintf("%s-%s-%s", "kafka", s.metadata.topic, s.metadata.group)),
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: targetMetricValue,
		},
	}
	metricSpec := v2beta2.MetricSpec{External: externalMetric, Type: kafkaMetricType}
	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *kafkaScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	partitions, err := s.getPartitions()
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, err
	}

	offsets, err := s.getOffsets(partitions)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, err
	}

	totalLag := int64(0)
	for _, partition := range partitions {
		lag, _ := s.getLagForPartition(partition, offsets)

		totalLag += lag
	}

	kafkaLog.V(1).Info(fmt.Sprintf("Kafka scaler: Providing metrics based on totalLag %v, partitions %v, threshold %v", totalLag, len(partitions), s.metadata.lagThreshold))

	// don't scale out beyond the number of partitions
	if (totalLag / s.metadata.lagThreshold) > int64(len(partitions)) {
		totalLag = int64(len(partitions)) * s.metadata.lagThreshold
	}

	// If a timeWindow is provided, let's return a value based on that timeWindow
	var returnWindowSeconds *int64
	if s.metadata.timeWindow != time.Duration(0) {
		s.tsdb.add(totalLag)
		// Compute totalLag base on TimeWindow
		totalLag, _ = s.tsdb.getMetricForTimeWindow()
		tmpReturnWindowSeconds := int64(math.Round(s.tsdb.timeWindow.Seconds()))
		returnWindowSeconds = &tmpReturnWindowSeconds
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName:    metricName,
		Value:         *resource.NewQuantity(totalLag, resource.DecimalSI),
		Timestamp:     metav1.Now(),
		WindowSeconds: returnWindowSeconds,
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

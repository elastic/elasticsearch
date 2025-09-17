/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import co.elastic.elasticsearch.serverless.autoscaling.ServerlessAutoscalingPlugin;
import co.elastic.elasticsearch.serverless.autoscaling.action.GetIndexTierMetrics;
import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.engine.HollowIndexEngine;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler;
import org.elasticsearch.index.shard.ShardFieldStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.AutoscalingIndexingMetricsIT.markNodesForShutdown;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_FIELD_MEMORY_OVERHEAD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_SEGMENT_MEMORY_OVERHEAD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_SHARD_MEMORY_OVERHEAD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.INDEXING_MEMORY_MINIMUM_HEAP_REQUIRED_TO_ACCEPT_LARGE_OPERATIONS_METRIC_NAME;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardsMappingSizeCollector.RETRY_INITIAL_DELAY_SETTING;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static co.elastic.elasticsearch.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING;
import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class AutoscalingMemoryMetricsIT extends AbstractStatelessIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AutoscalingMemoryMetricsIT.class);

    private static final String INDEX_NAME = "test-index-001";
    static final int ACTUAL_METADATA_FIELDS = 5; // _id, _version, _seq_no, _primary_term, _source
    static final int MAPPING_METADATA_FIELDS = 13;

    private static final Settings INDEX_NODE_SETTINGS = Settings.builder()
        // publish metric once per second
        .put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
        .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(
            List.of(TestTelemetryPlugin.class, ShutdownPlugin.class, ServerlessAutoscalingPlugin.class),
            super.nodePlugins()
        );
    }

    public void testCreateIndexWithMapping() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);
        ensureStableCluster(2); // master + index node

        final AtomicInteger transportMetricCounter = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishHeapMemoryMetrics.NAME)) {
                    transportMetricCounter.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final var estimateMemoryUsageBeforeUpdate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
            .estimateTierMemoryUsage();

        final long sizeBeforeIndexCreate = estimateMemoryUsageBeforeUpdate.totalBytes();
        assertThat(sizeBeforeIndexCreate, equalTo(0L));

        final int mappingFieldsCount = randomIntBetween(10, 1000);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
        assertAcked(prepareCreate(INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)));
        assertBusy(() -> {
            // Note that asserting busy here (and in tests below) is needed to ensure that writing thread completed update of
            // MemoryMetricsService
            final var totalIndexMappingSizeAfterUpdate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();

            final long estimateMemoryUsageAfterIndexCreate = totalIndexMappingSizeAfterUpdate.totalBytes();
            final long expectedMemoryOverhead = 1024L * mappingFieldsCount;
            // Note that strict comparison is not possible here (and tests below) because of presence of metadata mapping fields e.g.
            // _index, _source, etc
            // those fields are implementation specific and some of them can be added by plugins, thus it is not possible to rely on its
            // count
            assertThat(estimateMemoryUsageAfterIndexCreate, greaterThan(sizeBeforeIndexCreate + expectedMemoryOverhead));

            // ensure that all expected updates have arrived to master
            assertThat(totalIndexMappingSizeAfterUpdate.metricQuality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testUpdateIndexMapping() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);
        ensureStableCluster(2); // master + index node

        createIndex(INDEX_NAME, 1, 0);
        ensureGreen(INDEX_NAME);

        final var estimateMemoryUsageBeforeUpdate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
            .estimateTierMemoryUsage();

        final long sizeBeforeMappingUpdate = estimateMemoryUsageBeforeUpdate.totalBytes();

        // We need to delay the second update until the mapping got updated to MISSING on the master node
        final CountDownLatch mappingUpdated = new CountDownLatch(1);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishHeapMemoryMetrics.NAME)) {
                    safeAwait(mappingUpdated);
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final int mappingFieldsCount = randomIntBetween(10, 1000);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);

        // Update index mapping
        assertAcked(indicesAdmin().putMapping(new PutMappingRequest(INDEX_NAME).source(indexMapping)).get());
        mappingUpdated.countDown();

        assertBusy(() -> {
            final var estimateMemoryUsageAfterUpdate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterMappingUpdate = estimateMemoryUsageAfterUpdate.totalBytes();
            final long expectedMemoryOverhead = 1024L * mappingFieldsCount;
            assertThat(sizeAfterMappingUpdate, greaterThan(sizeBeforeMappingUpdate + expectedMemoryOverhead));
            assertThat(estimateMemoryUsageAfterUpdate.metricQuality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testDeleteIndex() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);
        ensureStableCluster(2); // master + index node

        final AtomicInteger transportMetricCounter = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishHeapMemoryMetrics.NAME)) {
                    transportMetricCounter.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final var estimateMemoryUsageBeforeUpdate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
            .estimateTierMemoryUsage();
        final long sizeBeforeIndexCreate = estimateMemoryUsageBeforeUpdate.totalBytes();

        final int mappingFieldsCount = randomIntBetween(10, 1000);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);

        assertAcked(prepareCreate(INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());

        // memory goes up
        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)));
        assertBusy(() -> {
            final var estimateMemoryUsageAfterUpdate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexCreate = estimateMemoryUsageAfterUpdate.totalBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(sizeBeforeIndexCreate));  // sanity check
            assertThat(estimateMemoryUsageAfterUpdate.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME).get());

        // memory goes down
        assertBusy(() -> {
            final var estimateMemoryUsageAfterDelete = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexDelete = estimateMemoryUsageAfterDelete.totalBytes();
            assertThat(sizeAfterIndexDelete, equalTo(sizeBeforeIndexCreate));
            assertThat(estimateMemoryUsageAfterDelete.metricQuality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testMovedShardPublication() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);
        startIndexNode(INDEX_NODE_SETTINGS);
        ensureStableCluster(3); // master + 2 index nodes

        final AtomicInteger transportMetricCounter = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishHeapMemoryMetrics.NAME)) {
                    transportMetricCounter.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        // create test index
        final int mappingFieldsCount = randomIntBetween(10, 1000);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
        assertAcked(prepareCreate(INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());
        var testIndex = resolveIndex(INDEX_NAME);

        final String publicationNodeId = internalCluster().clusterService()
            .state()
            .getRoutingTable()
            .shardRoutingTable(INDEX_NAME, 0)
            .primaryShard()
            .currentNodeId();
        final String publicationNodeName = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .get(publicationNodeId)
            .getName();

        // expect metric publication is happening
        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)));

        // ensure that metric was published from the node it is allocated to
        final String initialPublicationNodeId = publicationNodeId;
        assertBusy(() -> {
            var internalMapCheck1 = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class).getShardMemoryMetrics();
            MemoryMetricsService.ShardMemoryMetrics indexMetricCheck1 = internalMapCheck1.get(new ShardId(testIndex, 0));
            assertThat(indexMetricCheck1.getMetricShardNodeId(), equalTo(initialPublicationNodeId));
        });

        internalCluster().stopNode(publicationNodeName);
        // drop node where 0-shard was allocated, expect 0-shard is relocated to another (new) node
        ensureStableCluster(2);
        ensureGreen(INDEX_NAME);

        // find `other` index node where shard was moved to
        final String newPublicationNodeId = internalCluster().clusterService()
            .state()
            .nodes()
            .getDataNodes()
            .keySet()
            .stream()
            .filter(nodeId -> nodeId.equals(initialPublicationNodeId) == false)
            .findFirst()
            .get();
        assertBusy(() -> {
            // ensure that metric was published from the node where 0-shard was moved to
            var internalMapCheck2 = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class).getShardMemoryMetrics();
            MemoryMetricsService.ShardMemoryMetrics indexMetricCheck2 = internalMapCheck2.get(new ShardId(testIndex, 0));
            assertThat(indexMetricCheck2.getMetricShardNodeId(), equalTo(newPublicationNodeId));
        });
    }

    public void testShardMemoryMetricsForHollowEngine() throws Exception {
        startMasterOnlyNode();
        final var indexNodeSettings = Settings.builder()
            .put(INDEX_NODE_SETTINGS)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        String indexNodeA = startIndexNode(indexNodeSettings);
        ensureStableCluster(2);

        var indexName = INDEX_NAME;
        int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, indexSettings(numberOfShards, 0).build());
        ensureGreen(indexName);
        var index = resolveIndex(indexName);

        indexDocs(indexName, randomIntBetween(16, 64));
        flush(indexName);

        Map<ShardId, MemoryMetricsService.ShardMemoryMetrics> originalShardMemoryMetrics = new HashMap<>();
        var metricsService = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));

            assertBusy(() -> {
                var shardMemoryMetric = metricsService.getShardMemoryMetrics().get(indexShard.shardId());
                assertNotNull(shardMemoryMetric);
                assertThat(shardMemoryMetric.getMetricShardNodeId(), equalTo(getNodeId(indexNodeA)));
                originalShardMemoryMetrics.put(indexShard.shardId(), shardMemoryMetric);
            });
        }

        String indexNodeB = startIndexNode(indexNodeSettings);
        hollowShards(indexName, numberOfShards, indexNodeA, indexNodeB);

        // Verify that hollow shards we don't lose shard field stats
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));
            assertBusy(() -> {
                var shardMemoryMetric = metricsService.getShardMemoryMetrics().get(indexShard.shardId());
                assertNotNull(shardMemoryMetric);
                assertThat(shardMemoryMetric.getMetricShardNodeId(), equalTo(getNodeId(indexNodeB)));
                var originalShardMemoryMetric = originalShardMemoryMetrics.get(indexShard.shardId());
                assertThat(shardMemoryMetric.getTotalFields(), equalTo(originalShardMemoryMetric.getTotalFields()));
                assertThat(shardMemoryMetric.getNumSegments(), equalTo(originalShardMemoryMetric.getNumSegments()));
            });
        }

        // Ensure ingestion reaches all shards and unhollow them
        assertBusy(() -> {
            indexDocs(indexName, randomIntBetween(16, 64));
            for (int i = 0; i < numberOfShards; i++) {
                var indexShard = findIndexShard(index, i);
                assertThat(indexShard.getEngineOrNull(), instanceOf(IndexEngine.class));
            }
        });

        // We don't lose stats after unhollowing, too
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeB), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNodeB))));
        ensureGreen(indexName);
        for (int i = 0; i < numberOfShards; i++) {
            var indexShard = findIndexShard(index, i);
            assertThat(indexShard.getEngineOrNull(), instanceOf(HollowIndexEngine.class));
            assertBusy(() -> {
                var shardMemoryMetric = metricsService.getShardMemoryMetrics().get(indexShard.shardId());
                assertNotNull(shardMemoryMetric);
                assertThat(shardMemoryMetric.getMetricShardNodeId(), equalTo(getNodeId(indexNodeA)));
                var originalShardMemoryMetric = originalShardMemoryMetrics.get(indexShard.shardId());
                assertThat(shardMemoryMetric.getTotalFields(), equalTo(originalShardMemoryMetric.getTotalFields()));
                assertThat(shardMemoryMetric.getNumSegments(), equalTo(originalShardMemoryMetric.getNumSegments()));
            });
        }
    }

    public void testScaleUpAndDownOnMultipleIndicesAndNodes() throws Exception {
        startMasterNode();
        int indexNodes = randomIntBetween(1, 10);
        logger.info("---> Number of index nodes: {}", indexNodes);
        List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < indexNodes; i++) {
            nodeNames.add(startIndexNode(INDEX_NODE_SETTINGS));
        }
        ensureStableCluster(1 + indexNodes);

        assertBusy(() -> {
            final var estimateMemoryUsageBeforeIndexCreate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeBeforeIndexCreate = estimateMemoryUsageBeforeIndexCreate.totalBytes();
            // no indices created, thus 0
            assertThat(sizeBeforeIndexCreate, equalTo(0L));
            assertThat(estimateMemoryUsageBeforeIndexCreate.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        final AtomicInteger transportMetricCounter = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishHeapMemoryMetrics.NAME)) {
                    transportMetricCounter.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        final LongAdder minimalEstimatedOverhead = new LongAdder();
        final int numberOfIndices = randomIntBetween(10, 50);
        logger.info("---> Number of indices: {}", numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            final String indexName = "test-index-[" + i + "]";

            final int mappingFieldsCount = randomIntBetween(10, 100);

            // accumulate index mapping size overhead for all indices
            minimalEstimatedOverhead.add(1024L * mappingFieldsCount);

            final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
            int shardsCount = randomIntBetween(1, indexNodes);
            logger.info("---> Number of shards {} in index {}", numberOfIndices, indexName);
            assertAcked(prepareCreate(indexName).setMapping(indexMapping).setSettings(indexSettings(shardsCount, 0).build()).get());
        }

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)), 60, TimeUnit.SECONDS);
        assertBusy(() -> {
            final var estimateMemoryUsageIndexCreate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexCreate = estimateMemoryUsageIndexCreate.totalBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(minimalEstimatedOverhead.sum()));
            assertThat(estimateMemoryUsageIndexCreate.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        for (int i = 0; i < numberOfIndices; i++) {
            final String indexName = "test-index-[" + i + "]";
            assertAcked(indicesAdmin().prepareDelete(indexName).get());
        }

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(2)), 60, TimeUnit.SECONDS);
        assertBusy(() -> {
            final var estimateMemoryUsageAfterIndexDelete = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexDelete = estimateMemoryUsageAfterIndexDelete.totalBytes();
            // back to previous state when no indices existed
            assertThat(sizeAfterIndexDelete, equalTo(0L));
            assertThat(estimateMemoryUsageAfterIndexDelete.metricQuality(), equalTo(MetricQuality.EXACT));
        }, 60, TimeUnit.SECONDS);
    }

    public void testMetricsRemainExactAfterAMasterFailover() throws Exception {
        startMasterNode();
        var masterNode2 = startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);

        final LongAdder minimalEstimatedOverhead = new LongAdder();
        final int numberOfIndices = randomIntBetween(10, 50);
        for (int i = 0; i < numberOfIndices; i++) {
            final String indexName = "test-index-[" + i + "]";

            final int mappingFieldsCount = randomIntBetween(10, 100);

            minimalEstimatedOverhead.add(1024L * mappingFieldsCount);

            final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
            assertAcked(prepareCreate(indexName).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());
        }

        assertBusy(() -> {
            final var estimateMemoryUsageIndexCreate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexCreate = estimateMemoryUsageIndexCreate.totalBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(minimalEstimatedOverhead.sum()));
            assertThat(estimateMemoryUsageIndexCreate.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            var currentMasterNode = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().getMasterNode();
            assertThat(currentMasterNode, is(notNullValue()));
            assertThat(currentMasterNode.getName(), is(equalTo(masterNode2)));
        });

        assertBusy(() -> {
            final var estimateMemoryUsageIndexCreate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexCreate = estimateMemoryUsageIndexCreate.totalBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(minimalEstimatedOverhead.sum()));
            assertThat(estimateMemoryUsageIndexCreate.metricQuality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testMemoryMetricsRemainExactAfterAShardMovesToADifferentNode() throws Exception {
        startMasterNode();
        var indexNode1 = startIndexNode(INDEX_NODE_SETTINGS);
        var indexNode2 = startIndexNode(INDEX_NODE_SETTINGS);

        var primaryShardRelocated = new AtomicBoolean();
        var transportService = (MockTransportService) internalCluster().getCurrentMasterNodeInstance(TransportService.class);
        transportService.addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, (handler, request, channel, task) -> {
            // Ignore memory metrics until the primary shard moves into the new node to ensure that we transition to the new state correctly
            if (primaryShardRelocated.get()) {
                handler.messageReceived(request, channel, task);
            } else {
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            }
        });
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setMapping(createIndexMapping(randomIntBetween(10, 100)))
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build())
                .get()
        );

        assertBusy(() -> {
            final var totalIndexMappingSizeIndexCreate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            assertThat(totalIndexMappingSizeIndexCreate.metricQuality(), not(equalTo(MetricQuality.EXACT)));
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2));
        primaryShardRelocated.set(true);

        assertBusy(() -> {
            final var estimateMemoryUsageIndexCreate = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexCreate = estimateMemoryUsageIndexCreate.totalBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(estimateMemoryUsageIndexCreate.metricQuality(), equalTo(MetricQuality.EXACT));

            // Make sure the index stats are assigned to node2
            assertThat(
                internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                    .getShardMemoryMetrics()
                    .get(new ShardId(resolveIndex(indexName), 0))
                    .getMetricShardNodeId(),
                equalTo(getNodeId(indexNode2))
            );
        });
    }

    public void testMemoryMetricsRemainExactAfterAShardMovesToADifferentNodeAndMappingsChange() throws Exception {
        startMasterNode();
        var indexNode1 = startIndexNode(
            Settings.builder().put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueMillis(50)).build()
        );
        var indexNode2 = startIndexNode(
            Settings.builder().put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build()
        );

        var indexName = randomIdentifier();
        var numberOfFields = randomIntBetween(10, 100);
        assertAcked(
            prepareCreate(indexName).setMapping(createIndexMapping(numberOfFields))
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build())
                .get()
        );
        var index = resolveIndex(indexName);

        assertBusy(() -> {
            var indexMemoryMetrics = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .getShardMemoryMetrics()
                .get(new ShardId(index, 0));
            assertThat(indexMemoryMetrics, is(notNullValue()));
            final long sizeAfterIndexCreate = indexMemoryMetrics.getMappingSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(indexMemoryMetrics.getMetricQuality(), equalTo(MetricQuality.EXACT));
            // We need to ensure that the seq number goes beyond 10 to guarantee that once a new node
            // takes over the new samples are taken into account. We have to wait until seqNo > 10 because
            // assertBusy waits up to 10seconds and indexNode2 publishes a sample every second, meaning that
            // the issue couldn't be reproduced if seqNo < 10.
            assertThat(indexMemoryMetrics.getSeqNo(), is(greaterThan(10L)));
        });

        internalCluster().stopNode(indexNode1);

        // Update index mapping ensuring that we add extra fields
        assertAcked(indicesAdmin().putMapping(new PutMappingRequest(indexName).source(createIndexMapping(numberOfFields + 10))).get());

        assertBusy(() -> {
            var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class).estimateTierMemoryUsage();
            final long sizeAfterIndexCreate = estimateMemoryUsage.totalBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(estimateMemoryUsage.metricQuality(), equalTo(MetricQuality.MINIMUM));
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2));

        assertBusy(() -> {
            var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class).estimateTierMemoryUsage();
            assertThat(estimateMemoryUsage.totalBytes(), greaterThan(0L));
            assertThat(estimateMemoryUsage.metricQuality(), equalTo(MetricQuality.EXACT));
            // Make sure the index stats are assigned to node2
            assertThat(
                internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                    .getShardMemoryMetrics()
                    .get(new ShardId(index, 0))
                    .getMetricShardNodeId(),
                equalTo(getNodeId(indexNode2))
            );
        });
    }

    public void testMemoryMetricsAfterFeatureStateRestore() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);

        final AtomicInteger transportMetricCounter = new AtomicInteger(0);
        for (var transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportPublishHeapMemoryMetrics.NAME)) {
                    transportMetricCounter.incrementAndGet();
                }
                connection.sendRequest(requestId, action, request, options);
            });
        }

        // create test system index with feature state
        final XContentBuilder indexMapping = createIndexMapping(randomIntBetween(10, 1000));
        assertAcked(prepareCreate(SYSTEM_INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());

        // to compare memory metrics before and after snapshot `_restore` call
        final AtomicLong sizeAfterIndexCreate = new AtomicLong();

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)));
        assertBusy(() -> {
            final var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeInBytes = estimateMemoryUsage.totalBytes();
            sizeAfterIndexCreate.set(sizeInBytes);
            assertThat(estimateMemoryUsage.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        // create test repository
        createRepository("test-repo", "fs");

        // snapshot feature state
        final String featureStateName = SystemIndexTestPlugin.class.getSimpleName();
        createSnapshot("test-repo", "test-snapshot", List.of(), List.of(featureStateName));

        final CountDownLatch latch = new CountDownLatch(1);
        final int currentTransportMetricCounter = transportMetricCounter.get();

        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            if (event.metadataChanged()) {
                // When you restore a feature state, Elasticsearch closes and overwrites the feature’s existing indices
                for (Index deletedIndex : event.indicesDeleted()) {
                    if (SYSTEM_INDEX_NAME.equals(deletedIndex.getName())) {
                        latch.countDown();
                    }
                }
            }
        });

        // restore to same index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            "test-repo",
            "test-snapshot"
        ).setIndices("-*").setFeatureStates(featureStateName).setWaitForCompletion(true).get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());

        // wait until feature state index is deleted from metadata
        latch.await(30, TimeUnit.SECONDS);
        assertThat("Feature state index was not deleted", latch.getCount(), equalTo(0L));

        // assert that fresh data points have arrived
        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThan(currentTransportMetricCounter)));
        assertBusy(() -> {
            final var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexReCreate = estimateMemoryUsage.totalBytes();
            assertThat(estimateMemoryUsage.metricQuality(), equalTo(MetricQuality.EXACT));
            // ensure that before and after _restore mapping size matches
            assertThat(sizeAfterIndexReCreate, equalTo(sizeAfterIndexCreate.get()));
        });

        deleteRepository("test-repo");
    }

    public void testMemoryMetricsUpdateMappingInTheMiddleOfSnapshotRestore() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);

        // create test system index with feature state
        final int numberOfFields = randomIntBetween(10, 100);
        final XContentBuilder indexMapping = createIndexMapping(numberOfFields);
        assertAcked(prepareCreate(SYSTEM_INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());

        // to compare memory metrics before and after snapshot `_restore` call
        final AtomicLong sizeAfterIndexCreate = new AtomicLong();
        assertBusy(() -> {
            final var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeInBytes = estimateMemoryUsage.totalBytes();
            assertTrue(sizeInBytes > 0);
            sizeAfterIndexCreate.set(sizeInBytes);
            assertThat(estimateMemoryUsage.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        // create test repository
        createRepository("test-repo", "fs");

        // snapshot feature state
        final String featureStateName = SystemIndexTestPlugin.class.getSimpleName();
        createSnapshot("test-repo", "test-snapshot", List.of(), List.of(featureStateName));

        // Update index mapping ensuring that we add extra fields
        assertAcked(
            indicesAdmin().putMapping(new PutMappingRequest(SYSTEM_INDEX_NAME).source(createIndexMapping(numberOfFields + 10))).get()
        );
        assertBusy(() -> {
            final var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterMappingUpdate = estimateMemoryUsage.totalBytes();
            assertTrue(sizeAfterMappingUpdate > sizeAfterIndexCreate.get());
        });

        // restore to same index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            "test-repo",
            "test-snapshot"
        ).setIndices("-*").setFeatureStates(featureStateName).setWaitForCompletion(true).get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertBusy(() -> {
            final var estimateMemoryUsage = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .estimateTierMemoryUsage();
            final long sizeAfterIndexReCreate = estimateMemoryUsage.totalBytes();
            // ensure that before and after _restore mapping size matches
            assertThat(sizeAfterIndexReCreate, equalTo(sizeAfterIndexCreate.get()));
            assertThat(estimateMemoryUsage.metricQuality(), equalTo(MetricQuality.EXACT));
        });

        deleteRepository("test-repo");
    }

    public void testNoMissedIndexMappingUpdatesOnSlowClusterUpdates() throws Exception {
        var masterNode = startMasterNode();
        startIndexNode();
        ensureStableCluster(2);

        ServiceDisruptionScheme disruption = new SlowClusterStateProcessing(masterNode, random(), 0, 0, 10, 100);
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        List<String> indices = Stream.generate(ESTestCase::randomIdentifier).limit(randomIntBetween(4, 10)).toList();
        Thread backgroundIndexer = new Thread(() -> {
            for (String index : indices) {
                assertAcked(prepareCreate(index).setMapping(createIndexMapping(randomIntBetween(10, 100))).get());
                safeSleep(randomIntBetween(10, 50));
            }
        });
        backgroundIndexer.start();

        assertBusy(() -> {
            Map<String, MetricQuality> indicesMetricQuality = new HashMap<>();
            var metrics = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class).getShardMemoryMetrics();
            for (var e : metrics.entrySet()) {
                indicesMetricQuality.put(e.getKey().getIndexName(), e.getValue().getMetricQuality());
            }
            for (String index : indices) {
                assertEquals(
                    "Mappings for index " + index + " haven't been applied on master node",
                    MetricQuality.EXACT,
                    indicesMetricQuality.get(index)
                );
            }
        }, 30, TimeUnit.SECONDS);

        backgroundIndexer.join();
        disruption.stopDisrupting();
    }

    public void testPublishHeapMemoryMetricsRetriesOnAutoscalingMissedIndicesUpdateException() throws Exception {
        startMasterNode();
        startIndexNode(Settings.builder().put(RETRY_INITIAL_DELAY_SETTING.getKey(), TimeValue.timeValueMillis(100)).build());
        ensureStableCluster(2);

        int failCount = randomIntBetween(2, 5);
        var attempt = new AtomicInteger();
        var transportService = (MockTransportService) internalCluster().getCurrentMasterNodeInstance(TransportService.class);
        transportService.addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, (handler, request, channel, task) -> {
            if (attempt.incrementAndGet() <= failCount) {
                channel.sendResponse(new AutoscalingMissedIndicesUpdateException("could not update mapping sizes"));
            } else {
                handler.messageReceived(request, channel, task);
            }
        });

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setMapping(createIndexMapping(randomIntBetween(10, 20))).get());

        var masterMemoryMetricService = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class);
        final var index = resolveIndex(indexName);
        assertBusy(() -> {
            assertThat(attempt.get(), greaterThan(failCount));
            var indexMemoryMetrics = masterMemoryMetricService.getShardMemoryMetrics().get(new ShardId(index, 0));
            assertNotNull(indexMemoryMetrics);
            assertThat(indexMemoryMetrics.getMappingSizeInBytes(), greaterThan(0L));
            assertThat(indexMemoryMetrics.getMetricQuality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testShardMovesToNewNodeWithSlowClusterUpdate() throws Exception {
        var masterNode = startMasterNode();
        var indexNode1 = startIndexNode();
        var indexNode2 = startIndexNode();
        ensureStableCluster(3);

        var indexNode1Id = getNodeId(indexNode1);
        var indexNode2Id = getNodeId(indexNode2);

        ServiceDisruptionScheme disruption = new SlowClusterStateProcessing(masterNode, random(), 0, 0, 10, 100);
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setMapping(createIndexMapping(randomIntBetween(10, 100)))
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build())
                .get()
        );
        var index = resolveIndex(indexName);

        assertBusy(() -> {
            final MemoryMetricsService.ShardMemoryMetrics indexMappingSize = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getShardMemoryMetrics().get(new ShardId(index, 0));
            assertThat(indexMappingSize.getMetricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(indexMappingSize.getMetricShardNodeId(), equalTo(indexNode1Id));
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2));

        // Make sure the index stats are eventually assigned to node2 despite the slow cluster updates
        assertBusy(() -> {
            final MemoryMetricsService.ShardMemoryMetrics indexMappingSize = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getShardMemoryMetrics().get(new ShardId(index, 0));
            final long sizeAfterIndexCreate = indexMappingSize.getMappingSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(indexMappingSize.getMetricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(indexMappingSize.getMetricShardNodeId(), equalTo(indexNode2Id));
        });

        disruption.stopDisrupting();
    }

    public void testMemoryOverheadSetting() throws Exception {
        var projectType = randomFrom(ProjectType.values());
        startMasterAndIndexNode(
            Settings.builder()
                .put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), projectType)
                .build()
        );
        startSearchNode();
        ensureStableCluster(2);
        var defaultNoOfShards = projectType.getNumberOfShards();
        logger.info("--> Default No. of shards: {}", defaultNoOfShards);
        int noOfIndices = randomIntBetween(5, 10);
        logger.info("--> No. of indices: {}", noOfIndices);
        int totalSegments = 0;
        int totalShards = noOfIndices * defaultNoOfShards;
        int totalSegmentFields = 0;
        int totalMappingFields = 0;
        long id = 0;
        for (int i = 0; i < noOfIndices; i++) {
            int numFields = between(1, 10);
            int numSegments = between(1, 3);
            for (int s = 0; s < numSegments; s++) {
                int numDocs = between(50, 100);
                BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int d = 0; d < numDocs; d++) {
                    Object[] fields = new Object[numFields * 2];
                    for (int f = 0; f < numFields; f++) {
                        fields[2 * f] = "field-" + f;
                        fields[2 * f + 1] = randomAlphanumericOfLength(10);
                    }
                    // Generate an 11-byte _id (see Uid.encodeId)
                    String stringId = String.format(Locale.ROOT, "?id-%06d", id++);
                    bulk.add(client().prepareIndex("index-" + i).setId(stringId).setSource(fields));
                }
                assertNoFailures(bulk.get());

                totalSegmentFields += defaultNoOfShards * (ACTUAL_METADATA_FIELDS + numFields * 2);
                totalSegments += defaultNoOfShards;
            }
            totalMappingFields += MAPPING_METADATA_FIELDS + numFields * 2; // one for text, one for keyword
        }
        var memoryMetricService = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class);
        final int finalNumSegments = totalSegments;
        assertBusy(() -> {
            var metrics = memoryMetricService.getShardMemoryMetrics();
            assertThat(metrics.size(), equalTo(totalShards));
            assertThat(metrics.values().stream().mapToInt(s -> s.getNumSegments()).sum(), equalTo(finalNumSegments));
            if (ShardFieldStats.TRACK_LIVE_DOCS_IN_MEMORY_BYTES.isEnabled()) {
                long actualTotalLiveDocsBytes = metrics.values().stream().mapToLong(s -> s.getLiveDocsBytes()).sum();
                assertThat(actualTotalLiveDocsBytes, equalTo(0L));
            }
        });
        // We use a fixed estimate 1024 bytes per index mapping field
        final long mappingSizeInBytes = totalMappingFields * defaultNoOfShards * 1024L;
        // defaults to the fixed method
        {
            var fixedEstimate = mappingSizeInBytes + totalShards * FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT.getBytes();
            var totalMemoryInBytes = memoryMetricService.getSearchTierMemoryMetrics().totalMemoryInBytes();
            assertThat(totalMemoryInBytes, equalTo(HeapToSystemMemory.tier(fixedEstimate, projectType)));
        }
        // switch to the adaptive method
        {
            var settingsMap = Map.<String, Object>of(FIXED_SHARD_MEMORY_OVERHEAD_SETTING.getKey(), ByteSizeValue.MINUS_ONE);
            double adaptiveExtraOverheadRatio = 0.5; // default value
            if (randomBoolean()) {
                adaptiveExtraOverheadRatio = randomDoubleBetween(0, 1, true);
                settingsMap = new HashMap<>(settingsMap);
                settingsMap.put(ADAPTIVE_EXTRA_OVERHEAD_SETTING.getKey(), adaptiveExtraOverheadRatio);
            }
            assertAcked(
                admin().cluster().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settingsMap)
            );
            assertBusy(() -> assertThat(memoryMetricService.fixedShardMemoryOverhead, equalTo(ByteSizeValue.MINUS_ONE)));
            long adaptiveEstimate = totalShards * ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + totalSegments
                * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes() + totalSegmentFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes();
            long extraForAdaptive = (long) (adaptiveEstimate * adaptiveExtraOverheadRatio);
            var totalMemoryInBytes = memoryMetricService.getSearchTierMemoryMetrics().totalMemoryInBytes();
            assertThat(
                totalMemoryInBytes,
                equalTo(HeapToSystemMemory.tier(mappingSizeInBytes + adaptiveEstimate + extraForAdaptive, projectType))
            );
        }
        // override the fixed shard overhead
        {
            ByteSizeValue newShardFixedMemoryOverhead = ByteSizeValue.ofMb(between(1, 10));
            assertAcked(
                admin().cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Map.of(FIXED_SHARD_MEMORY_OVERHEAD_SETTING.getKey(), newShardFixedMemoryOverhead))
            );
            assertBusy(() -> assertThat(memoryMetricService.fixedShardMemoryOverhead, equalTo(newShardFixedMemoryOverhead)));
            long newFixedEstimate = mappingSizeInBytes + totalShards * newShardFixedMemoryOverhead.getBytes();
            var totalMemoryInBytes = memoryMetricService.getSearchTierMemoryMetrics().totalMemoryInBytes();
            assertThat(totalMemoryInBytes, equalTo(HeapToSystemMemory.tier(newFixedEstimate, projectType)));
        }
        // remove the setting also switch back to the fixed method
        {
            assertAcked(
                admin().cluster()
                    .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(FIXED_SHARD_MEMORY_OVERHEAD_SETTING.getKey()))
            );
            assertBusy(() -> assertThat(memoryMetricService.fixedShardMemoryOverhead, equalTo(FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT)));
            long fixedEstimate = mappingSizeInBytes + totalShards * FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT.getBytes();
            var totalMemoryInBytes = memoryMetricService.getSearchTierMemoryMetrics().totalMemoryInBytes();
            assertThat(totalMemoryInBytes, equalTo(HeapToSystemMemory.tier(fixedEstimate, projectType)));
        }

        // Perform a deleted and check if live docs bytes have increased:
        client().prepareDelete("index-0", String.format(Locale.ROOT, "?id-%06d", 0))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();
        // The delete-op that marks has been stored in a new segment and therefor num segments increases:
        totalSegments++;
        // The delete op in the new segment has an id, which means number of bytes for postings increases by 11:
        final int finalNumSegments2 = totalSegments;
        assertBusy(() -> {
            var metrics = memoryMetricService.getShardMemoryMetrics();
            assertThat(metrics.size(), equalTo(totalShards));
            assertThat(metrics.values().stream().mapToInt(s -> s.getNumSegments()).sum(), equalTo(finalNumSegments2));
            if (ShardFieldStats.TRACK_LIVE_DOCS_IN_MEMORY_BYTES.isEnabled()) {
                long actualTotalLiveDocsBytes = metrics.values().stream().mapToLong(s -> s.getLiveDocsBytes()).sum();
                // Two segments have live docs. The initial segment for index-0 and
                // then the new segment of that index that contains the delete operation.
                // However, assertion live docs here is tricky given that there is no guarantee that there was one segment for index-0
                // before performing the delete operation. So instead asserting the live docs has increased:
                assertThat(actualTotalLiveDocsBytes, greaterThan(0L));
            }
        });
    }

    public void testUpdateMetricsOnRefresh() throws Exception {

        startMasterAndIndexNode();
        startMasterAndIndexNode();
        ensureStableCluster(2);
        // started shards should publish heap memory usage
        var memoryMetricsService = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class);
        final int mappingFieldsCount = randomIntBetween(10, 900);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
        int numberOfShards = between(1, 5);
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(indexMapping)
                .setSettings(
                    indexSettings(numberOfShards, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build()
                )
                .get()
        );
        Index index = resolveIndex(INDEX_NAME);
        assertBusy(() -> {
            for (int id = 0; id < numberOfShards; id++) {
                ShardId shardId = new ShardId(index, id);
                var metric = memoryMetricsService.getShardMemoryMetrics().get(shardId);
                assertThat(metric.getMetricQuality(), equalTo(MetricQuality.EXACT));
                assertThat(metric.getMappingSizeInBytes(), equalTo(1024L * (mappingFieldsCount + MAPPING_METADATA_FIELDS)));
                assertThat(metric.getTotalFields(), equalTo(0));
                assertThat(metric.getNumSegments(), equalTo(0));
            }
        });
        Map<ShardId, MemoryMetricsService.ShardMemoryMetrics> previousShardMetrics = new HashMap<>();
        snapshotMetrics(memoryMetricsService, previousShardMetrics);

        // new segments publishes the estimate heap usage
        int numDocs = randomIntBetween(1, 10);
        ClusterState clusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(clusterState.metadata().indexMetadata(index));
        Map<ShardId, Integer> firstSegmentFields = new HashMap<>();
        {
            BulkRequestBuilder bulk = client().prepareBulk(INDEX_NAME).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < numDocs; i++) {
                String docId = randomAlphaOfLength(5);
                int shardId = indexRouting.getShard(docId, null);
                int actualFields = randomIntBetween(1, mappingFieldsCount);
                Map<String, Object> source = new HashMap<>();
                for (int f = 0; f < actualFields; f++) {
                    source.put("field-" + f, randomAlphaOfLength(5));
                }
                firstSegmentFields.merge(new ShardId(index, shardId), actualFields, Math::max);
                bulk.add(new IndexRequest(INDEX_NAME).id(docId).source(source));
            }
            assertNoFailures(bulk.get());
            assertBusy(() -> {
                for (int id = 0; id < numberOfShards; id++) {
                    ShardId shardId = new ShardId(index, id);
                    var curr = memoryMetricsService.getShardMemoryMetrics().get(shardId);
                    var prev = previousShardMetrics.get(shardId);
                    assertThat(curr.getMetricQuality(), equalTo(MetricQuality.EXACT));
                    assertThat(curr.getMappingSizeInBytes(), equalTo(1024L * (mappingFieldsCount + MAPPING_METADATA_FIELDS)));
                    Integer firstStats = firstSegmentFields.get(shardId);
                    if (firstStats != null) {
                        assertThat(curr.getSeqNo(), greaterThan(prev.getSeqNo()));
                        assertThat(curr.getNumSegments(), equalTo(1));
                        assertThat(curr.getTotalFields(), equalTo(firstStats + ACTUAL_METADATA_FIELDS));
                    } else {
                        // no updates from shards which didn't receive the first bulk
                        assertThat(curr.getSeqNo(), equalTo(prev.getSeqNo()));
                        assertThat(curr.getNumSegments(), equalTo(0));
                        assertThat(curr.getTotalFields(), equalTo(0));
                    }
                }
            });
        }
        snapshotMetrics(memoryMetricsService, previousShardMetrics);

        // new mapping publishes the estimate heap usage
        client().admin().indices().preparePutMapping(INDEX_NAME).setSource("extra-field", "type=keyword").get();
        assertBusy(() -> {
            for (int id = 0; id < numberOfShards; id++) {
                ShardId shardId = new ShardId(index, id);
                var curr = memoryMetricsService.getShardMemoryMetrics().get(shardId);
                var prev = previousShardMetrics.get(shardId);
                assertThat(curr.getSeqNo(), greaterThan(prev.getSeqNo()));
                assertThat(curr.getMappingSizeInBytes(), equalTo(1024L * (mappingFieldsCount + 1 + MAPPING_METADATA_FIELDS)));
                assertThat(curr.getNumSegments(), equalTo(prev.getNumSegments()));
                assertThat(curr.getTotalFields(), equalTo(prev.getTotalFields()));
            }
        });
        snapshotMetrics(memoryMetricsService, previousShardMetrics);

        // new segments publishes the estimate heap usage
        Map<ShardId, Integer> secondSegmentFields = new HashMap<>();
        {
            BulkRequestBuilder bulk = client().prepareBulk(INDEX_NAME).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < numDocs; i++) {
                String docId = randomAlphaOfLength(5);
                int shardId = indexRouting.getShard(docId, null);
                int actualFields = randomIntBetween(1, mappingFieldsCount);
                Map<String, Object> source = new HashMap<>();
                for (int f = 0; f < actualFields; f++) {
                    source.put("field-" + f, randomAlphaOfLength(6));
                }
                secondSegmentFields.merge(new ShardId(index, shardId), actualFields, Math::max);
                bulk.add(new IndexRequest(INDEX_NAME).id(docId).source(source));
            }
            assertNoFailures(bulk.get());
            assertBusy(() -> {
                for (int id = 0; id < numberOfShards; id++) {
                    ShardId shardId = new ShardId(index, id);
                    var curr = memoryMetricsService.getShardMemoryMetrics().get(shardId);
                    var prev = previousShardMetrics.get(shardId);
                    assertThat(curr.getMappingSizeInBytes(), equalTo(1024L * (mappingFieldsCount + 1 + MAPPING_METADATA_FIELDS)));
                    Integer secondStats = secondSegmentFields.get(shardId);
                    if (secondStats != null) {
                        Integer firstStats = firstSegmentFields.get(shardId);
                        int totalSegments = 1;
                        int totalFields = secondStats + ACTUAL_METADATA_FIELDS;
                        if (firstStats != null) {
                            totalSegments++;
                            totalFields += firstStats + ACTUAL_METADATA_FIELDS;
                        }
                        assertThat(curr.getNumSegments(), equalTo(totalSegments));
                        assertThat(curr.getTotalFields(), equalTo(totalFields));
                        assertThat(curr.getSeqNo(), greaterThan(prev.getSeqNo()));
                    } else {
                        // no updates from shards which didn't receive the second bulk
                        assertThat(curr.getSeqNo(), equalTo(prev.getSeqNo()));
                        assertThat(curr.getNumSegments(), equalTo(prev.getNumSegments()));
                        assertThat(curr.getTotalFields(), equalTo(prev.getTotalFields()));
                    }
                }
            });
        }
        snapshotMetrics(memoryMetricsService, previousShardMetrics);

        // force merge will also update the estimate heap usage
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        assertBusy(() -> {
            for (int id = 0; id < numberOfShards; id++) {
                ShardId shardId = new ShardId(index, id);
                var curr = memoryMetricsService.getShardMemoryMetrics().get(shardId);
                var prev = previousShardMetrics.get(shardId);
                assertThat(curr.getMappingSizeInBytes(), equalTo(1024L * (mappingFieldsCount + 1 + MAPPING_METADATA_FIELDS)));
                Integer firstStats = firstSegmentFields.get(shardId);
                Integer secondStats = secondSegmentFields.get(shardId);
                if (firstStats != null && secondStats != null) {
                    int totalFields = Math.max(firstStats, secondStats) + ACTUAL_METADATA_FIELDS;
                    assertThat(curr.getNumSegments(), equalTo(1));
                    assertThat(curr.getTotalFields(), equalTo(totalFields));
                } else {
                    // No updates from shards with a single segment (received zero or one bulk request) because
                    // force-merge is a no-op for these shards.
                    assertThat(curr.getSeqNo(), equalTo(prev.getSeqNo()));
                    assertThat(curr.getNumSegments(), equalTo(prev.getNumSegments()));
                    assertThat(curr.getTotalFields(), equalTo(prev.getTotalFields()));
                }
            }
        });
    }

    public void testUnassignedShardCausesMinimumMemoryMetric() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(10, 20));
        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.EXACT));
        });
        // Doesn't matter if it is a planned removal or not
        if (randomBoolean()) {
            markNodesForShutdown(
                clusterService().state().nodes().getAllNodes().stream().filter(n -> n.getName().equals(indexNode)).toList(),
                Arrays.stream(SingleNodeShutdownMetadata.Type.values()).filter(SingleNodeShutdownMetadata.Type::isRemovalType).toList()
            );
        }
        assertTrue(internalCluster().stopNode(indexNode));
        ensureRed(indexName);
        // The shard is unassigned, so it should cause the memory metrics to be MINIMUM
        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.MINIMUM));
        });
        startIndexNode();
        // The shard is unassigned and memory metrics are still reported as MINIMUM
        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.MINIMUM));
        });
        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.require._name"), indexName);
        ensureGreen(indexName);
        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.EXACT));
        });
    }

    private static MemoryMetrics getMemoryMetrics() {
        return safeGet(
            client().execute(GetIndexTierMetrics.INSTANCE, new GetIndexTierMetrics.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        ).getMetrics().getMemoryMetrics();
    }

    public void testUpdatesFromOldPrimaryNodesAreSilentlyIgnored() {
        final String masterNode = startMasterOnlyNode();
        final String originalIndexingNode = startIndexNode(INDEX_NODE_SETTINGS);
        ensureStableCluster(2); // master + index node

        // create and populate the index
        createIndex(INDEX_NAME, indexSettings(1, 0).build());
        indexDocs(INDEX_NAME, between(50, 100));

        // Create a new indexing node
        final String newIndexingNode = startIndexNode(INDEX_NODE_SETTINGS);
        logger.info("--> New indexing node is {}/{}", newIndexingNode, getNodeId(newIndexingNode));

        // Block metrics publish requests from the original node
        final CountDownLatch allowSendingMetrics = new CountDownLatch(1);
        final CountDownLatch requestIsWaiting = new CountDownLatch(1);
        MockTransportService.getInstance(originalIndexingNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (TransportPublishHeapMemoryMetrics.NAME.equals(action)) {
                requestIsWaiting.countDown();
                safeAwait(allowSendingMetrics);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Wait for a request to be blocked
        safeAwait(requestIsWaiting);
        logger.info("--> A request is blocked");

        // trigger a relocation
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", originalIndexingNode), INDEX_NAME);
        ensureGreen(INDEX_NAME);
        logger.info("--> Relocation is complete");

        // monitor to ensure the blocked publish request is not rejected for retry
        final String originalIndexingNodeId = getNodeId(originalIndexingNode);
        final CountDownLatch acceptedResponseSeen = new CountDownLatch(1);
        final AtomicInteger retryResponsesSeen = new AtomicInteger(0);
        MockTransportService.getInstance(masterNode)
            .addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, (handler, request, channel, task) -> {
                PublishHeapMemoryMetricsRequest publishHeapMemoryMetricsRequest = asInstanceOf(
                    PublishHeapMemoryMetricsRequest.class,
                    request
                );
                // Intercept responses to requests from the prior primary with a known version, to ensure they are not rejected for retry
                if (Objects.equals(originalIndexingNodeId, getSourceNodeId(publishHeapMemoryMetricsRequest))
                    && publishHeapMemoryMetricsRequest.getHeapMemoryUsage().clusterStateVersion() != ClusterState.UNKNOWN_VERSION) {
                    handler.messageReceived(
                        request,
                        new TestTransportChannel(
                            ActionListener.wrap(
                                (response) -> acceptedResponseSeen.countDown(),
                                (exception) -> retryResponsesSeen.incrementAndGet()
                            )
                        ),
                        task
                    );
                }
                handler.messageReceived(request, channel, task);
            });

        // release the blocked publish request(s), wait till we've seen one not rejected
        allowSendingMetrics.countDown();
        safeAwait(acceptedResponseSeen);
        assertEquals(0, retryResponsesSeen.get());
    }

    public void testIndexingOperationsMemoryRequirementsAreTakenIntoAccount() throws Exception {
        var projectType = randomFrom(ProjectType.values());
        var indexingOperationMemoryMetricsEnabled = randomBoolean();
        var nodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings()) // avoid uncontrolled flush for updating posting memory usages
            .put(INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING.getKey(), TimeValue.timeValueSeconds(2))
            .put(INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING.getKey(), indexingOperationMemoryMetricsEnabled)
            .put(ServerlessSharedSettings.PROJECT_TYPE.getKey(), projectType)
            .put(IndexingPressure.MAX_OPERATION_SIZE.getKey(), "1%")
            .build();

        startMasterAndIndexNode(nodeSettings);
        createIndex(
            INDEX_NAME,
            indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );
        ensureGreen(INDEX_NAME);

        var currentHeapSize = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        var currentNodeSize = HeapToSystemMemory.tier(currentHeapSize * 2, projectType);
        var maxOperationSize = (int) (currentHeapSize * 0.01);

        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.nodeMemoryInBytes(), lessThanOrEqualTo(currentNodeSize));
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.EXACT));
        });

        var requestedNodeMemoryBytesBeforeRejection = getMemoryMetrics().nodeMemoryInBytes();

        var operationBeyondMaxSize = randomBoolean();

        var bulkRequest = client().prepareBulk()
            .add(
                client().prepareIndex(INDEX_NAME)
                    .setSource("field", randomUnicodeOfLength(operationBeyondMaxSize ? maxOperationSize + 1 : randomIntBetween(10, 20)))
            );
        var bulkResponse = bulkRequest.get();
        if (operationBeyondMaxSize) {
            assertThat(bulkResponse.hasFailures(), is(true));
        } else {
            assertNoFailures(bulkResponse);
        }

        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(
                memoryMetrics.nodeMemoryInBytes(),
                operationBeyondMaxSize && indexingOperationMemoryMetricsEnabled
                    ? greaterThan(currentNodeSize)
                    : lessThanOrEqualTo(currentNodeSize)
            );
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.EXACT));
        });

        assertBusy(() -> {
            final var memoryMetrics = getMemoryMetrics();
            assertThat(memoryMetrics.nodeMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.nodeMemoryInBytes(), equalTo(requestedNodeMemoryBytesBeforeRejection));
            assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(0L));
            assertThat(memoryMetrics.quality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testMergeMemoryEstimationIsConsideredInNodeMemory() throws Exception {
        var nodeSettings = Settings.builder()
            // Enable using the merge executor service (and disable using the stateless merge scheduler)
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(MemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING.getKey(), true)
            // Avoid accounting for indexing operation memory requirements as they skew the basic memory metrics
            .put(INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING.getKey(), false)
            .put(MergeMemoryEstimateCollector.MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO.getKey(), 0.01)
            .build();
        String indexNode = startMasterAndIndexNode(nodeSettings);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        var latch = new CountDownLatch(1);
        var threadPool = internalCluster().getInstance(ThreadPool.class, indexNode);
        blockMergePool(threadPool, latch);

        var memoryMetricsBefore = getMemoryMetrics();
        assertThat(memoryMetricsBefore.quality(), equalTo(MetricQuality.EXACT));
        assertThat(memoryMetricsBefore.nodeMemoryInBytes(), greaterThan(0L));

        int numDims = randomIntBetween(MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING, MIN_DIMS_FOR_DYNAMIC_FLOAT_MAPPING + 1024);
        int numDocs = randomIntBetween(10, 20);
        var idSupplier = new Supplier<String>() {
            private int id = 0;

            @Override
            public String get() {
                return String.format(Locale.ROOT, "?id-%06d", id++);
            }
        };
        for (int i = 0; i < numDocs; i++) {
            indexDocs(
                indexName,
                randomIntBetween(10, 20),
                UnaryOperator.identity(),
                idSupplier,
                () -> Map.of("vectorField", randomVector(numDims))
            );
            refresh(indexName);
        }
        var mergeFuture = client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).execute();

        // 1 shard, 11 bytes per _id (vectorField doesn't have postings), store min and max
        // x2 - see HeapToSystemMemory.dataNode()
        final long postingsInMemoryBytes = 22 * 2;
        assertBusy(() -> {
            var memoryMetricsDuring = getMemoryMetrics();
            assertThat(memoryMetricsDuring.quality(), equalTo(MetricQuality.EXACT));
            assertThat(
                memoryMetricsDuring.nodeMemoryInBytes(),
                greaterThan(memoryMetricsBefore.nodeMemoryInBytes() + postingsInMemoryBytes)
            );
        });

        latch.countDown();
        assertNoFailures(mergeFuture.actionGet());
        // once the merge is finished a new merge estimate should be published to bring down the node memory metric
        assertBusy(() -> {
            var memoryMetricsAfter = getMemoryMetrics();
            assertThat(memoryMetricsAfter.quality(), equalTo(MetricQuality.EXACT));
            assertThat(memoryMetricsAfter.nodeMemoryInBytes(), equalTo(memoryMetricsBefore.nodeMemoryInBytes() + postingsInMemoryBytes));
        });
    }

    public void testInMemoryPostingsEstimationIsConsideredInNodeMemory() throws Exception {
        var nodeSettings = Settings.builder()
            .put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            // Avoid accounting for indexing operation memory requirements as they skew the basic memory metrics
            .put(INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING.getKey(), false)
            .build();
        String node1 = startMasterAndIndexNode(nodeSettings);
        String node2 = startIndexNode(nodeSettings);

        final String index1 = randomIdentifier();
        final String index2 = randomIdentifier();
        createIndex(index1, indexSettings(1, 0).put("index.routing.allocation.require._name", node1).build());
        createIndex(index2, indexSettings(1, 0).put("index.routing.allocation.require._name", node2).build());

        var clusterState = client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
        var index1Shards = clusterState.getState().routingTable(ProjectId.DEFAULT).allShards(index1);
        assertThat(index1Shards.size(), equalTo(1));
        assertThat(clusterState.getState().getNodes().get(index1Shards.getFirst().currentNodeId()).getName(), equalTo(node1));
        var index2Shards = clusterState.getState().routingTable(ProjectId.DEFAULT).allShards(index2);
        assertThat(index2Shards.size(), equalTo(1));
        assertThat(clusterState.getState().getNodes().get(index2Shards.getFirst().currentNodeId()).getName(), equalTo(node2));

        var memoryMetricsBefore = getMemoryMetrics();
        assertThat(memoryMetricsBefore.quality(), equalTo(MetricQuality.EXACT));
        assertThat(memoryMetricsBefore.nodeMemoryInBytes(), greaterThan(0L));

        var idSupplier = new Supplier<String>() {
            private int id = 0;

            @Override
            public String get() {
                return String.format(Locale.ROOT, "?id-%06d", id++);
            }
        };

        int numFieldsIndex1 = randomIntBetween(10, 100);
        int numFieldsIndex2 = randomIntBetween(10, 100);

        int numDocs = randomIntBetween(10, 50);
        int numSegments = randomIntBetween(1, 3);

        IntFunction<Supplier<Map<String, ?>>> docSupplierSupplier = (int numFields) -> () -> {
            Map<String, Object> doc = new HashMap<>();
            for (int i = 0; i < numFields; i++) {
                doc.put("field=-" + i, randomAlphanumericOfLength(10));
            }
            return doc;
        };

        for (int i = 0; i < numSegments; i++) {
            indexDocs(
                index1,
                numDocs,
                request -> request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                idSupplier,
                docSupplierSupplier.apply(numFieldsIndex1)
            );

            indexDocs(
                index2,
                numDocs,
                request -> request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                idSupplier,
                docSupplierSupplier.apply(numFieldsIndex2)
            );
        }

        // 10 bytes for minTerm, maxTerm per field, field.keyword. 11 bytes for minTerm, maxTerm for _id. Repeated for each segment.
        final long totalPostingsInMemoryBytesNode1 = ((10L * 2L) * (numFieldsIndex1 * 2L) + 22L) * numSegments;
        final long totalPostingsInMemoryBytesNode2 = ((10L * 2L) * (numFieldsIndex2 * 2L) + 22L) * numSegments;

        // x2 - see HeapToSystemMemory.dataNode()
        final long totalPostingsInMemoryBytes = Math.max(totalPostingsInMemoryBytesNode1, totalPostingsInMemoryBytesNode2) * 2;

        assertBusy(() -> {
            var memoryMetricsAfter = getMemoryMetrics();
            assertThat(memoryMetricsAfter.quality(), equalTo(MetricQuality.EXACT));
            assertThat(
                memoryMetricsAfter.nodeMemoryInBytes(),
                equalTo(memoryMetricsBefore.nodeMemoryInBytes() + totalPostingsInMemoryBytes)
            );
        });
    }

    public void testIndexingOperationsMemoryRequirementsArePublishedThroughAPM() throws Exception {
        startMasterAndIndexNode(
            Settings.builder()
                .put(INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING.getKey(), TimeValue.timeValueSeconds(2))
                .put(IndexingPressure.MAX_OPERATION_SIZE.getKey(), "1%")
                .build()
        );
        createIndex(INDEX_NAME, indexSettings(1, 0).build());
        ensureGreen(INDEX_NAME);
        indexDocs(INDEX_NAME, randomIntBetween(10, 20));

        var currentHeapSize = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        var maxOperationSize = (int) (currentHeapSize * 0.01);

        var plugin = findPlugin(internalCluster().getMasterName(), TestTelemetryPlugin.class);
        var latestAPMMetricSample = new AtomicLong(0);
        assertBusy(() -> {
            plugin.collect();
            var measurements = plugin.getLongGaugeMeasurement(INDEXING_MEMORY_MINIMUM_HEAP_REQUIRED_TO_ACCEPT_LARGE_OPERATIONS_METRIC_NAME);
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).getLong(), is(greaterThan(0L)));
            latestAPMMetricSample.set(measurements.get(0).getLong());
        });
        plugin.resetMeter();

        var bulkRequest = client().prepareBulk()
            .add(client().prepareIndex(INDEX_NAME).setSource("field", randomUnicodeOfLength(maxOperationSize + 1)));
        var bulkResponse = bulkRequest.get();
        assertThat(bulkResponse.hasFailures(), is(true));

        // After the rejection, a new memory metric is published
        assertBusy(() -> {
            plugin.resetMeter();
            plugin.collect();
            List<Measurement> measurements = plugin.getLongGaugeMeasurement(
                INDEXING_MEMORY_MINIMUM_HEAP_REQUIRED_TO_ACCEPT_LARGE_OPERATIONS_METRIC_NAME
            );
            assertThat(measurements.size(), equalTo(1));
            assertThat(measurements.get(0).getLong(), is(greaterThan(latestAPMMetricSample.get())));
        });

        // After INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING the sample is stale and the APM metric is not published anymore
        assertBusy(() -> {
            plugin.resetMeter();
            plugin.collect();
            List<Measurement> measurements = plugin.getLongGaugeMeasurement(
                INDEXING_MEMORY_MINIMUM_HEAP_REQUIRED_TO_ACCEPT_LARGE_OPERATIONS_METRIC_NAME
            );
            assertThat(measurements.size(), equalTo(0));
        });
    }

    public static void blockMergePool(ThreadPool threadPool, CountDownLatch finishLatch) {
        final var threadCount = threadPool.info(ThreadPool.Names.MERGE).getMax();
        final var startBarrier = new CyclicBarrier(threadCount + 1);
        final var blockingTask = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            protected void doRun() {
                safeAwait(startBarrier);
                safeAwait(finishLatch);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        };
        for (int i = 0; i < threadCount; i++) {
            threadPool.executor(ThreadPool.Names.MERGE).execute(blockingTask);
        }
        safeAwait(startBarrier);
    }

    private String getSourceNodeId(PublishHeapMemoryMetricsRequest request) {
        return request.getHeapMemoryUsage().shardMappingSizes().values().stream().findAny().map(ShardMappingSize::nodeId).orElse(null);
    }

    static void snapshotMetrics(MemoryMetricsService memoryMetricsService, Map<ShardId, MemoryMetricsService.ShardMemoryMetrics> out) {
        out.clear();
        memoryMetricsService.getShardMemoryMetrics().forEach((shardId, m) -> {
            out.put(
                shardId,
                new MemoryMetricsService.ShardMemoryMetrics(
                    m.getMappingSizeInBytes(),
                    m.getNumSegments(),
                    m.getTotalFields(),
                    m.getPostingsInMemoryBytes(),
                    0L,
                    m.getSeqNo(),
                    m.getMetricQuality(),
                    m.getMetricShardNodeId(),
                    m.getUpdateTimestampNanos()
                )
            );
        });
    }

    private String startMasterNode() {
        return internalCluster().startMasterOnlyNode(
            nodeSettings().put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build()
        );
    }

    private static XContentBuilder createIndexMapping(int fieldCount) {
        try {
            final XContentBuilder sourceMapping = XContentFactory.jsonBuilder();
            sourceMapping.startObject();
            sourceMapping.startObject("properties");
            for (int i = 0; i < fieldCount; i++) {
                sourceMapping.startObject("field-" + i);
                sourceMapping.field("type", "text");
                sourceMapping.endObject();
            }
            sourceMapping.endObject();
            sourceMapping.endObject();
            return sourceMapping;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}

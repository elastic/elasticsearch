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

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.IndicesMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.SHARD_MEMORY_OVERHEAD_DEFAULT;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.SHARD_MEMORY_OVERHEAD_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

public class AutoscalingMemoryMetricsIT extends AbstractStatelessIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AutoscalingMemoryMetricsIT.class);

    private static final String INDEX_NAME = "test-index-001";

    private static final Settings INDEX_NODE_SETTINGS = Settings.builder()
        // publish metric once per second
        .put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
        .build();

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

        final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeBeforeUpdate = internalCluster().getCurrentMasterNodeInstance(
            MemoryMetricsService.class
        ).getTotalIndicesMappingSize();

        final long sizeBeforeIndexCreate = totalIndexMappingSizeBeforeUpdate.getSizeInBytes();
        assertThat(sizeBeforeIndexCreate, equalTo(0L));

        final int mappingFieldsCount = randomIntBetween(10, 1000);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
        assertAcked(prepareCreate(INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)));
        assertBusy(() -> {
            // Note that asserting busy here (and in tests below) is needed to ensure that writing thread completed update of
            // MemoryMetricsService
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeAfterUpdate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();

            final long sizeAfterIndexCreate = totalIndexMappingSizeAfterUpdate.getSizeInBytes();
            final long expectedMemoryOverhead = 1024 * mappingFieldsCount;
            // Note that strict comparison is not possible here (and tests below) because of presence of metadata mapping fields e.g.
            // _index, _source, etc
            // those fields are implementation specific and some of them can be added by plugins, thus it is not possible to rely on its
            // count
            assertThat(sizeAfterIndexCreate, greaterThan(sizeBeforeIndexCreate + expectedMemoryOverhead));

            // ensure that all expected updates have arrived to master
            assertThat(totalIndexMappingSizeAfterUpdate.getMetricQuality(), equalTo(MetricQuality.EXACT));
        });
    }

    public void testUpdateIndexMapping() throws Exception {
        startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);
        ensureStableCluster(2); // master + index node

        createIndex(INDEX_NAME, 1, 0);
        ensureGreen(INDEX_NAME);

        final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeBeforeUpdate = internalCluster().getCurrentMasterNodeInstance(
            MemoryMetricsService.class
        ).getTotalIndicesMappingSize();

        final long sizeBeforeMappingUpdate = totalIndexMappingSizeBeforeUpdate.getSizeInBytes();

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
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeAfterUpdate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterMappingUpdate = totalIndexMappingSizeAfterUpdate.getSizeInBytes();
            final long expectedMemoryOverhead = 1024 * mappingFieldsCount;
            assertThat(sizeAfterMappingUpdate, greaterThan(sizeBeforeMappingUpdate + expectedMemoryOverhead));
            assertThat(totalIndexMappingSizeAfterUpdate.getMetricQuality(), equalTo(MetricQuality.EXACT));
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

        final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeBeforeUpdate = internalCluster().getCurrentMasterNodeInstance(
            MemoryMetricsService.class
        ).getTotalIndicesMappingSize();
        final long sizeBeforeIndexCreate = totalIndexMappingSizeBeforeUpdate.getSizeInBytes();

        final int mappingFieldsCount = randomIntBetween(10, 1000);
        final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);

        assertAcked(prepareCreate(INDEX_NAME).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());

        // memory goes up
        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)));
        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeAfterUpdate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexCreate = totalIndexMappingSizeAfterUpdate.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(sizeBeforeIndexCreate));  // sanity check
            assertThat(totalIndexMappingSizeAfterUpdate.getMetricQuality(), equalTo(MetricQuality.EXACT));
        });

        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME).get());

        // memory goes down
        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeAfterDelete = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexDelete = totalIndexMappingSizeAfterDelete.getSizeInBytes();
            assertThat(sizeAfterIndexDelete, equalTo(sizeBeforeIndexCreate));
            assertThat(totalIndexMappingSizeAfterDelete.getMetricQuality(), equalTo(MetricQuality.EXACT));
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

        final Index testIndex = internalCluster().clusterService().state().metadata().index(INDEX_NAME).getIndex();
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
            Map<Index, MemoryMetricsService.IndexMemoryMetrics> internalMapCheck1 = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getIndicesMemoryMetrics();
            MemoryMetricsService.IndexMemoryMetrics indexMetricCheck1 = internalMapCheck1.get(testIndex);
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
            Map<Index, MemoryMetricsService.IndexMemoryMetrics> internalMapCheck2 = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getIndicesMemoryMetrics();
            MemoryMetricsService.IndexMemoryMetrics indexMetricCheck2 = internalMapCheck2.get(testIndex);
            assertThat(indexMetricCheck2.getMetricShardNodeId(), equalTo(newPublicationNodeId));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1734")
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
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeBeforeIndexCreate = internalCluster()
                .getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .getTotalIndicesMappingSize();
            final long sizeBeforeIndexCreate = totalIndexMappingSizeBeforeIndexCreate.getSizeInBytes();
            // no indices created, thus 0
            assertThat(sizeBeforeIndexCreate, equalTo(0L));
            assertThat(totalIndexMappingSizeBeforeIndexCreate.getMetricQuality(), equalTo(MetricQuality.EXACT));
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
            minimalEstimatedOverhead.add(1024 * mappingFieldsCount);

            final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
            int shardsCount = randomIntBetween(1, indexNodes);
            logger.info("---> Number of shards {} in index {}", numberOfIndices, indexName);
            assertAcked(prepareCreate(indexName).setMapping(indexMapping).setSettings(indexSettings(shardsCount, 0).build()).get());
        }

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(1)), 60, TimeUnit.SECONDS);
        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeIndexCreate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexCreate = totalIndexMappingSizeIndexCreate.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(minimalEstimatedOverhead.sum()));
            assertThat(totalIndexMappingSizeIndexCreate.getMetricQuality(), equalTo(MetricQuality.EXACT));
        });

        for (int i = 0; i < numberOfIndices; i++) {
            final String indexName = "test-index-[" + i + "]";
            assertAcked(indicesAdmin().prepareDelete(indexName).get());
        }

        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThanOrEqualTo(2)), 60, TimeUnit.SECONDS);
        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeAfterIndexDelete = internalCluster()
                .getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .getTotalIndicesMappingSize();
            final long sizeAfterIndexDelete = totalIndexMappingSizeAfterIndexDelete.getSizeInBytes();
            // back to previous state when no indices existed
            assertThat(sizeAfterIndexDelete, equalTo(0L));
            assertThat(totalIndexMappingSizeAfterIndexDelete.getMetricQuality(), equalTo(MetricQuality.EXACT));
        }, 60, TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1801")
    public void testMetricsRemainExactAfterAMasterFailover() throws Exception {
        startMasterNode();
        var masterNode2 = startMasterNode();
        startIndexNode(INDEX_NODE_SETTINGS);

        final LongAdder minimalEstimatedOverhead = new LongAdder();
        final int numberOfIndices = randomIntBetween(10, 50);
        for (int i = 0; i < numberOfIndices; i++) {
            final String indexName = "test-index-[" + i + "]";

            final int mappingFieldsCount = randomIntBetween(10, 100);

            minimalEstimatedOverhead.add(1024 * mappingFieldsCount);

            final XContentBuilder indexMapping = createIndexMapping(mappingFieldsCount);
            assertAcked(prepareCreate(indexName).setMapping(indexMapping).setSettings(indexSettings(1, 0).build()).get());
        }

        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeIndexCreate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexCreate = totalIndexMappingSizeIndexCreate.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(minimalEstimatedOverhead.sum()));
            assertThat(totalIndexMappingSizeIndexCreate.getMetricQuality(), equalTo(MetricQuality.EXACT));
        });

        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            var currentMasterNode = client().admin().cluster().prepareState().get().getState().nodes().getMasterNode();
            assertThat(currentMasterNode, is(notNullValue()));
            assertThat(currentMasterNode.getName(), is(equalTo(masterNode2)));
        });

        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeIndexCreate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexCreate = totalIndexMappingSizeIndexCreate.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(minimalEstimatedOverhead.sum()));
            assertThat(totalIndexMappingSizeIndexCreate.getMetricQuality(), equalTo(MetricQuality.EXACT));
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
            }
        });
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setMapping(createIndexMapping(randomIntBetween(10, 100)))
                .setSettings(indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build())
                .get()
        );

        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeIndexCreate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            assertThat(totalIndexMappingSizeIndexCreate.getMetricQuality(), equalTo(MetricQuality.MISSING));
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2));
        primaryShardRelocated.set(true);

        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics totalIndexMappingSizeIndexCreate = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexCreate = totalIndexMappingSizeIndexCreate.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(totalIndexMappingSizeIndexCreate.getMetricQuality(), equalTo(MetricQuality.EXACT));

            // Make sure the index stats are assigned to node2
            assertThat(
                internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                    .getIndicesMemoryMetrics()
                    .get(resolveIndex(indexName))
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
                .getIndicesMemoryMetrics()
                .get(index);
            assertThat(indexMemoryMetrics, is(notNullValue()));
            final long sizeAfterIndexCreate = indexMemoryMetrics.getSizeInBytes();
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
            var totalIndicesMappingSize = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .getTotalIndicesMappingSize();
            final long sizeAfterIndexCreate = totalIndicesMappingSize.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(totalIndicesMappingSize.getMetricQuality(), equalTo(MetricQuality.MINIMUM));
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2));

        assertBusy(() -> {
            var totalIndicesMappingSize = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .getTotalIndicesMappingSize();
            assertThat(totalIndicesMappingSize.getSizeInBytes(), greaterThan(0L));
            assertThat(totalIndicesMappingSize.getMetricQuality(), equalTo(MetricQuality.EXACT));
            // Make sure the index stats are assigned to node2
            assertThat(
                internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                    .getIndicesMemoryMetrics()
                    .get(index)
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
            final MemoryMetricsService.IndexMemoryMetrics indexMemoryMetrics = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeInBytes = indexMemoryMetrics.getSizeInBytes();
            sizeAfterIndexCreate.set(sizeInBytes);
            assertThat(indexMemoryMetrics.getMetricQuality(), equalTo(MetricQuality.EXACT));
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
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snapshot")
            .setIndices("-*")
            .setFeatureStates(featureStateName)
            .setWaitForCompletion(true)
            .get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());

        // wait until feature state index is deleted from metadata
        latch.await(30, TimeUnit.SECONDS);
        assertThat("Feature state index was not deleted", latch.getCount(), equalTo(0L));

        // assert that fresh data points have arrived
        assertBusy(() -> assertThat(transportMetricCounter.get(), greaterThan(currentTransportMetricCounter)));
        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics indexMemoryMetrics = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexReCreate = indexMemoryMetrics.getSizeInBytes();
            assertThat(indexMemoryMetrics.getMetricQuality(), equalTo(MetricQuality.EXACT));
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
            final MemoryMetricsService.IndexMemoryMetrics indexMemoryMetrics = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeInBytes = indexMemoryMetrics.getSizeInBytes();
            assertTrue(sizeInBytes > 0);
            sizeAfterIndexCreate.set(sizeInBytes);
            assertThat(indexMemoryMetrics.getMetricQuality(), equalTo(MetricQuality.EXACT));
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
            final MemoryMetricsService.IndexMemoryMetrics indexMemoryMetrics = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterMappingUpdate = indexMemoryMetrics.getSizeInBytes();
            assertTrue(sizeAfterMappingUpdate > sizeAfterIndexCreate.get());
        });

        // restore to same index
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snapshot")
            .setIndices("-*")
            .setFeatureStates(featureStateName)
            .setWaitForCompletion(true)
            .get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());

        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics indexMemoryMetrics = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getTotalIndicesMappingSize();
            final long sizeAfterIndexReCreate = indexMemoryMetrics.getSizeInBytes();
            // ensure that before and after _restore mapping size matches
            assertThat(sizeAfterIndexReCreate, equalTo(sizeAfterIndexCreate.get()));
            assertThat(indexMemoryMetrics.getMetricQuality(), equalTo(MetricQuality.EXACT));
        });

        deleteRepository("test-repo");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1734")
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
            var indicesMetricQuality = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class)
                .getIndicesMemoryMetrics()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue().getMetricQuality()));
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

    public void testNoRetriesOnOutOfOrderSeqNos() throws Exception {
        startMasterNode();
        startIndexNode();
        ensureStableCluster(2);

        AtomicInteger numberOfFields = new AtomicInteger(randomIntBetween(10, 20));
        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setMapping(createIndexMapping(numberOfFields.get())).get());

        int attempts = randomIntBetween(5, 10);
        var transportService = (MockTransportService) internalCluster().getCurrentMasterNodeInstance(TransportService.class);
        CountDownLatch requestsAreSubmitted = new CountDownLatch(attempts);
        CountDownLatch requestsAreHandled = new CountDownLatch(attempts);
        List<CheckedRunnable<Exception>> pendingRequests = new ArrayList<>();
        transportService.addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, (handler, request, channel, task) -> {
            assertTrue("No retry requests should be submitted", requestsAreSubmitted.getCount() > 0);
            synchronized (pendingRequests) {
                pendingRequests.add(() -> {
                    handler.messageReceived(request, channel, task);
                    requestsAreHandled.countDown();
                });
            }
            requestsAreSubmitted.countDown();
        });

        for (int i = 0; i < attempts; i++) {
            assertAcked(
                indicesAdmin().putMapping(new PutMappingRequest(indexName).source(createIndexMapping(numberOfFields.addAndGet(10)))).get()
            );
        }

        safeAwait(requestsAreSubmitted);
        // Send requests out of order and verify that all of them get processed without retries
        synchronized (pendingRequests) {
            Collections.shuffle(pendingRequests, random());
            for (var pendingRequest : pendingRequests) {
                pendingRequest.run();
            }
        }
        safeAwait(requestsAreHandled);
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
            final MemoryMetricsService.IndexMemoryMetrics indexMappingSize = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getIndicesMemoryMetrics().get(index);
            assertThat(indexMappingSize.getMetricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(indexMappingSize.getMetricShardNodeId(), equalTo(indexNode1Id));
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2));

        // Make sure the index stats are eventually assigned to node2 despite the slow cluster updates
        assertBusy(() -> {
            final MemoryMetricsService.IndexMemoryMetrics indexMappingSize = internalCluster().getCurrentMasterNodeInstance(
                MemoryMetricsService.class
            ).getIndicesMemoryMetrics().get(index);
            final long sizeAfterIndexCreate = indexMappingSize.getSizeInBytes();
            assertThat(sizeAfterIndexCreate, greaterThan(0L));
            assertThat(indexMappingSize.getMetricQuality(), equalTo(MetricQuality.EXACT));
            assertThat(indexMappingSize.getMetricShardNodeId(), equalTo(indexNode2Id));
        });

        disruption.stopDisrupting();
    }

    public void testSharMemoryOverheadSetting() throws Exception {
        startMasterAndIndexNode(
            Settings.builder()
                .put(PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
                // For now, we assume one shard per index in the memory estimates, so pick a project type
                // that has the default value of 1 for the number of shards.
                .put(
                    ServerlessSharedSettings.PROJECT_TYPE.getKey(),
                    randomFrom(Arrays.stream(ProjectType.values()).filter(t -> t.getNumberOfShards() == 1).toList())
                )
                .build()
        );
        startSearchNode();
        ensureStableCluster(2);
        // Make sure we go beyond min heap size, otherwise they all round up to the same value
        int minNoOfIndices = (int) (HeapToSystemMemory.MIN_HEAP_SIZE / SHARD_MEMORY_OVERHEAD_DEFAULT.getBytes());
        int noOfIndices = randomIntBetween(minNoOfIndices + 1, (int) (minNoOfIndices * 1.5));
        logger.info("--> No. of indices: {}", noOfIndices);
        var bulk = client().prepareBulk();
        IntStream.range(0, noOfIndices).forEach(i -> bulk.add(new IndexRequest("index-" + i).source("field", randomUnicodeOfLength(10))));
        assertNoFailures(bulk.get());
        var memoryMetricService = internalCluster().getCurrentMasterNodeInstance(MemoryMetricsService.class);
        assertBusy(() -> assertThat(memoryMetricService.getIndicesMemoryMetrics().size(), equalTo(noOfIndices)));
        // Shard memory overhead is only considered in the tier memory calculation
        var totalMemoryWithDefaultShardOverhead = memoryMetricService.getMemoryMetrics().totalMemoryInBytes();
        // Considering low number of fields, the memory overhead should be a factor of the shard memory overhead
        assertThat(
            totalMemoryWithDefaultShardOverhead,
            allOf(
                greaterThan(HeapToSystemMemory.dataNode(noOfIndices * SHARD_MEMORY_OVERHEAD_DEFAULT.getBytes())),
                lessThan(HeapToSystemMemory.dataNode((noOfIndices + 1) * SHARD_MEMORY_OVERHEAD_DEFAULT.getBytes()))
            )
        );

        var newShardMemoryOverhead = ByteSizeValue.ofMb(
            randomLongBetween(SHARD_MEMORY_OVERHEAD_DEFAULT.getMb() + 1, SHARD_MEMORY_OVERHEAD_DEFAULT.getMb() * 3)
        );
        assertAcked(
            admin().cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Map.of(SHARD_MEMORY_OVERHEAD_SETTING.getKey(), newShardMemoryOverhead))
        );
        var getSettingsResponse = clusterAdmin().execute(ClusterGetSettingsAction.INSTANCE, new ClusterGetSettingsAction.Request())
            .actionGet();
        assertThat(
            getSettingsResponse.settings().get(SHARD_MEMORY_OVERHEAD_SETTING.getKey()),
            equalTo(newShardMemoryOverhead.getStringRep())
        );
        var totalMemoryWithNewShardOverhead = memoryMetricService.getMemoryMetrics().totalMemoryInBytes();
        // The heap to system memory multiplier changes at {@link HeapToSystemMemory.HEAP_THRESHOLD}, so we only compare the lower bound.
        assertThat(
            totalMemoryWithNewShardOverhead,
            greaterThan(HeapToSystemMemory.dataNode(noOfIndices * newShardMemoryOverhead.getBytes()))
        );
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
                sourceMapping.startObject("field" + i);
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

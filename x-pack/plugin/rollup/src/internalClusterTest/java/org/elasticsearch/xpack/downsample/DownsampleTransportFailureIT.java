/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;
import org.elasticsearch.xpack.rollup.Rollup;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// This needs to be moved to internalClusterTest sourceSet
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1, supportsDedicatedMasters = false)
public class DownsampleTransportFailureIT extends ESIntegTestCase {

    public static final String ROLLUP_INDEXER_SHARD_ACTION = DownsampleIndexerAction.NAME + "[s]";

    private static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    private static class TestClusterHelper {
        private final InternalTestCluster cluster;
        private final String coordinator;
        private final String worker;

        TestClusterHelper(final InternalTestCluster cluster) {
            this.cluster = cluster;
            this.coordinator = randomCoordinator();
            this.worker = randomWorker();
        }

        private String randomCoordinator() {
            return ESTestCase.randomFrom(nonMasterNodes());
        }

        private String randomWorker() {
            assert this.coordinator != null;
            return ESTestCase.randomFrom(
                Arrays.stream(cluster.getNodeNames())
                    .filter(
                        nodeName -> this.cluster.getMasterName().equals(nodeName) == false && this.coordinator.equals(nodeName) == false
                    )
                    .collect(Collectors.toSet())
            );
        }

        private Collection<String> nonMasterNodes() {
            return Arrays.stream(cluster.getNodeNames())
                .filter(nodeName -> this.cluster.getMasterName().equals(nodeName) == false)
                .collect(Collectors.toSet());
        }

        public int size() {
            return this.cluster.size();
        }

        public String masterName() {
            return cluster.getMasterName();
        }

        public Client coordinatorClient() {
            assert this.coordinator != null;
            return ESIntegTestCase.client(this.coordinator);
        }

        public Client masterClient() {
            return ESIntegTestCase.client(this.cluster.getMasterName());
        }

        public MockTransportService masterMockTransportService() {
            return (MockTransportService) ESIntegTestCase.internalCluster()
                .getInstance(TransportService.class, ESIntegTestCase.internalCluster().getMasterName());
        }

        public MockTransportService coordinatorMockTransportService() {
            assert this.coordinator != null;
            return (MockTransportService) ESIntegTestCase.internalCluster().getInstance(TransportService.class, this.coordinator);
        }

        public List<MockTransportService> allMockTransportServices() {
            return Arrays.stream(cluster.getNodeNames())
                .map(nodeName -> (MockTransportService) ESIntegTestCase.internalCluster().getInstance(TransportService.class, nodeName))
                .collect(Collectors.toList());
        }

        public String coordinatorName() {
            assert this.coordinator != null;
            return this.coordinator;
        }

        public String workerName() {
            assert this.worker != null;
            return this.worker;
        }
    }

    private static final int DOWNSAMPLE_ACTION_TIMEOUT_MILLIS = 10_000;
    private static final String SOURCE_INDEX_NAME = "source";
    private static final String TARGET_INDEX_NAME = "target";
    private long startTime;
    private long endTime;
    private TestClusterHelper testCluster;

    private final List<String> DOCUMENTS = new ArrayList<>(
        List.of(
            "{\"@timestamp\": \"2020-09-09T18:03:00\",\"dim1\": \"dim1\",\"dim2\": \"401\",\"gauge\": \"100\",\"counter\": \"100\"}",
            "{\"@timestamp\": \"2020-09-09T18:04:00\",\"dim1\": \"dim1\",\"dim2\": \"402\",\"gauge\": \"101\",\"counter\": \"101\"}",
            "{\"@timestamp\": \"2020-09-09T18:05:00\",\"dim1\": \"dim1\",\"dim2\": \"403\",\"gauge\": \"102\",\"counter\": \"102\"}",
            "{\"@timestamp\": \"2020-09-09T18:06:00\",\"dim1\": \"dim1\",\"dim2\": \"404\",\"gauge\": \"101\",\"counter\": \"103\"}",
            "{\"@timestamp\": \"2020-09-09T18:07:00\",\"dim1\": \"dim1\",\"dim2\": \"405\",\"gauge\": \"103\",\"counter\": \"104\"}",
            "{\"@timestamp\": \"2020-09-09T18:08:00\",\"dim1\": \"dim1\",\"dim2\": \"406\",\"gauge\": \"110\",\"counter\": \"105\"}",
            "{\"@timestamp\": \"2020-09-09T18:09:00\",\"dim1\": \"dim1\",\"dim2\": \"407\",\"gauge\": \"112\",\"counter\": \"106\"}",
            "{\"@timestamp\": \"2020-09-09T18:10:00\",\"dim1\": \"dim1\",\"dim2\": \"408\",\"gauge\": \"111\",\"counter\": \"107\"}",
            "{\"@timestamp\": \"2020-09-09T18:11:00\",\"dim1\": \"dim1\",\"dim2\": \"409\",\"gauge\": \"105\",\"counter\": \"108\"}",
            "{\"@timestamp\": \"2020-09-09T18:12:00\",\"dim1\": \"dim1\",\"dim2\": \"410\",\"gauge\": \"106\",\"counter\": \"109\"}",
            "{\"@timestamp\": \"2020-09-09T18:13:00\",\"dim1\": \"dim1\",\"dim2\": \"411\",\"gauge\": \"107\",\"counter\": \"110\"}",
            "{\"@timestamp\": \"2020-09-09T18:14:00\",\"dim1\": \"dim1\",\"dim2\": \"412\",\"gauge\": \"104\",\"counter\": \"111\"}"
        )
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Rollup.class, AggregateMetricMapperPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return List.of(MockTransportService.TestPlugin.class, TestSeedPlugin.class);
    }

    @Before
    public void setup() throws IOException, ExecutionException, InterruptedException {
        startTime = LocalDateTime.parse("2020-09-09T18:00:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        endTime = LocalDateTime.parse("2020-09-09T18:59:00").atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
        testCluster = new TestClusterHelper(ESIntegTestCase.internalCluster());
        assert testCluster.size() == 3;
        ensureStableCluster(ESIntegTestCase.internalCluster().size());
        createTimeSeriesIndex(SOURCE_INDEX_NAME);
        ensureGreen(SOURCE_INDEX_NAME);
        indexDocuments(SOURCE_INDEX_NAME, DOCUMENTS);
        blockIndexWrites(SOURCE_INDEX_NAME);

        logger.info(
            "Cluster size {}, master node {}, coordinator node {}, worker node {}}",
            testCluster.size(),
            testCluster.masterName(),
            testCluster.coordinatorName(),
            testCluster.workerName()
        );
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("dim1"))
            .put(
                IndexSettings.TIME_SERIES_START_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(startTime).toEpochMilli())
            )
            .put(
                IndexSettings.TIME_SERIES_END_TIME.getKey(),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(Instant.ofEpochMilli(endTime).toEpochMilli())
            )
            .build();
    }

    public XContentBuilder indexMapping() throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        mapping.startObject("@timestamp").field("type", "date").endObject();
        mapping.startObject("dim1").field("type", "keyword").field("time_series_dimension", true).endObject();
        mapping.startObject("dim2").field("type", "long").field("time_series_dimension", true).endObject();
        mapping.startObject("gauge").field("type", "long").field("time_series_metric", "gauge").endObject();
        mapping.startObject("counter").field("type", "double").field("time_series_metric", "counter").endObject();
        mapping.endObject().endObject().endObject();
        return mapping;
    }

    public void indexDocuments(final String indexName, final List<String> documentsJson) {
        final BulkRequestBuilder bulkRequestBuilder = ESIntegTestCase.client().prepareBulk();
        documentsJson.forEach(document -> bulkRequestBuilder.add(new IndexRequest(indexName).source(document, XContentType.JSON)));
        Assert.assertFalse(bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get().hasFailures());
    }

    public void blockIndexWrites(final String indexName) throws ExecutionException, InterruptedException {
        final Settings blockWritesSetting = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();
        Assert.assertTrue(
            ESIntegTestCase.client()
                .admin()
                .indices()
                .updateSettings(new UpdateSettingsRequest(blockWritesSetting, indexName))
                .get()
                .isAcknowledged()
        );
    }

    private void createTimeSeriesIndex(final String indexName) throws IOException {
        Assert.assertTrue(prepareCreate(indexName).setMapping(indexMapping()).get().isShardsAcknowledged());
    }

    private void assertDownsampleFailure(final String nodeName) {
        assertIndexExists(nodeName, SOURCE_INDEX_NAME);
        assertDocumentsExist(nodeName, SOURCE_INDEX_NAME);
        assertIndexDoesNotExist(nodeName, TARGET_INDEX_NAME);
    }

    private void assertDocumentsExist(final String nodeName, final String indexName) {
        final SearchResponse searchResponse = ESIntegTestCase.client(nodeName)
            .prepareSearch(indexName)
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(DOCUMENTS.size())
            .get();
        Assert.assertEquals(DOCUMENTS.size(), searchResponse.getHits().getHits().length);
    }

    private void assertIndexExists(final String nodeName, final String indexName) {
        final GetIndexResponse getIndexResponse = ESIntegTestCase.client(nodeName)
            .admin()
            .indices()
            .prepareGetIndex()
            .addIndices(indexName)
            .addFeatures(GetIndexRequest.Feature.values())
            .get();
        Assert.assertEquals(List.of(indexName), Arrays.stream(getIndexResponse.indices()).toList());
    }

    private void assertIndexDoesNotExist(final String nodeName, final String indexName) {
        final IndexNotFoundException targetIndexNotFoundException = LuceneTestCase.expectThrows(
            IndexNotFoundException.class,
            "Index [" + indexName + "] was not deleted",
            () -> ESIntegTestCase.client(nodeName)
                .admin()
                .indices()
                .prepareGetIndex()
                .addIndices(indexName)
                .addFeatures(GetIndexRequest.Feature.values())
                .get()
        );
        Assert.assertEquals("no such index [" + indexName + "]", targetIndexNotFoundException.getMessage());
    }

    public void testNoDisruption() {
        // GIVEN

        final DownsampleAction.Request downsampleRequest = new DownsampleAction.Request(
            SOURCE_INDEX_NAME,
            TARGET_INDEX_NAME,
            new DownsampleConfig(DateHistogramInterval.MINUTE, TIMEOUT)
        );

        // WHEN nothing happens

        // THEN
        final AcknowledgedResponse downsampleResponse = testCluster.masterClient()
            .execute(DownsampleAction.INSTANCE, downsampleRequest)
            .actionGet(TimeValue.timeValueMillis(DOWNSAMPLE_ACTION_TIMEOUT_MILLIS));
        Assert.assertTrue(downsampleResponse.isAcknowledged());

        assertIndexExists(testCluster.coordinatorName(), SOURCE_INDEX_NAME);
        assertDocumentsExist(testCluster.coordinatorName(), SOURCE_INDEX_NAME);
        assertIndexExists(testCluster.coordinatorName(), TARGET_INDEX_NAME);
        // NOTE: the target downsample index `fixed_interval` matches the source index @timestamp interval.
        // As a result, the target index includes the same number of documents of the source index.
        assertDocumentsExist(testCluster.coordinatorName(), TARGET_INDEX_NAME);
        ensureStableCluster(ESIntegTestCase.internalCluster().size());
    }

    public void testDownsampleActionExceptionDisruption() {
        // GIVEN
        final MockTransportService coordinator = testCluster.coordinatorMockTransportService();
        final DownsampleAction.Request downsampleRequest = new DownsampleAction.Request(
            SOURCE_INDEX_NAME,
            TARGET_INDEX_NAME,
            new DownsampleConfig(DateHistogramInterval.HOUR, TIMEOUT)
        );

        // WHEN (disruption)
        testCluster.allMockTransportServices()
            .forEach(
                mockTransportService -> coordinator.addSendBehavior(
                    mockTransportService,
                    (connection, requestId, action, request, options) -> {
                        if (DownsampleAction.NAME.equals(action)) {
                            logger.info("Simulated disruption: node [" + connection.getNode().getName() + "] action [" + action + "]");
                            throw new ElasticsearchException(
                                "Simulated disruption: node [" + connection.getNode().getName() + "] action [" + action + "]"
                            );
                        }
                        connection.sendRequest(requestId, action, request, options);
                    }
                )
            );

        // THEN
        LuceneTestCase.expectThrows(
            ElasticsearchException.class,
            () -> testCluster.coordinatorClient()
                .execute(DownsampleAction.INSTANCE, downsampleRequest)
                .actionGet(TimeValue.timeValueMillis(DOWNSAMPLE_ACTION_TIMEOUT_MILLIS))
        );

        coordinator.clearAllRules();
        ensureStableCluster(testCluster.size());
        assertDownsampleFailure(testCluster.coordinatorName());
    }

    @AwaitsFix(bugUrl = "need to investigate...")
    public void testRollupIndexerActionExceptionDisruption() {
        // GIVEN
        final MockTransportService master = testCluster.masterMockTransportService();
        final DownsampleAction.Request downsampleRequest = new DownsampleAction.Request(
            SOURCE_INDEX_NAME,
            TARGET_INDEX_NAME,
            new DownsampleConfig(DateHistogramInterval.HOUR, TIMEOUT)
        );

        // WHEN (disruption)
        testCluster.allMockTransportServices()
            .forEach(
                mockTransportService -> master.addSendBehavior(mockTransportService, (connection, requestId, action, request, options) -> {
                    if (ROLLUP_INDEXER_SHARD_ACTION.equals(action)) {
                        logger.info("Simulated disruption: node [" + connection.getNode().getName() + "] action [" + action + "]");
                        throw new ElasticsearchException(
                            "Simulated disruption: node [" + connection.getNode().getName() + "] action [" + action + "]"
                        );
                    }
                    connection.sendRequest(requestId, action, request, options);
                })
            );

        // THEN
        LuceneTestCase.expectThrows(
            ElasticsearchException.class,
            () -> testCluster.coordinatorClient()
                .execute(DownsampleAction.INSTANCE, downsampleRequest)
                .actionGet(TimeValue.timeValueMillis(DOWNSAMPLE_ACTION_TIMEOUT_MILLIS))
        );

        master.clearAllRules();
        ensureStableCluster(testCluster.size());
        assertDownsampleFailure(testCluster.coordinatorName());
    }
}

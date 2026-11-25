/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end tests for {@link HistogramFieldMapper} on the columnar batch-indexing path. The unit tests in
 * {@code HistogramFieldMapperColumnarCompatibilityTests} compare the batch mapper with the row mapper in one process; these cover
 * what only a cluster shows: the replica path, translog replay, and the fallback to the row path after a failed primary item or a
 * chunk the mapper refuses.
 * <p>
 * Each test indexes the same documents into a batch-enabled index and into a twin index with batch indexing disabled, and compares
 * the synthetic source of every document, which is rebuilt from the histogram's doc values, on each shard copy. Lives here rather
 * than in {@code BatchBulkIT} because the server module has no dependency on the analytics plugin.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 1)
public class BatchBulkHistogramIT extends ESIntegTestCase {

    // A data stream's time-series backing index accepts timestamps around the time it is created; a 6000-document bulk at 100ms
    // apart stays well inside that range.
    private final long baseTimestamp = System.currentTimeMillis() - TimeValue.timeValueMinutes(30).millis();

    @ClassRule
    public static TestRule snapshotBuildRule = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
            base.evaluate();
        }
    };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AnalyticsPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .build();
    }

    /**
     * Valid histograms into {@code COLUMNAR} and {@code TIME_SERIES} indices with a replica: the primary and the replica both map the
     * batch, and each must hold what the row path writes for the same documents.
     */
    public void testBatchMatchesRowPathOnPrimaryAndReplica() throws IOException {
        for (IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.TIME_SERIES)) {
            final String batched = "test-histogram-batched-" + mode.getName();
            final String sequential = "test-histogram-sequential-" + mode.getName();
            createHistogramIndex(batched, mode, 1, true, false);
            createHistogramIndex(sequential, mode, 1, false, false);

            final List<String> sources = new ArrayList<>();
            final int numDocs = randomIntBetween(20, 200);
            for (int n = 0; n < numDocs; n++) {
                sources.add(source(mode, n, randomBoolean() ? null : randomHistogram()));
            }

            final BulkResponse fromBatch = withBatchLogging(mockLog -> {
                expectBatchIndexedOnPrimary(mockLog);
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation("no batch fallback", ShardBatchMapper.class.getName(), Level.DEBUG, "*disabled*")
                );
                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation("no chunk fallback", ShardBatchMapper.class.getName(), Level.WARN, "*")
                );
                return bulk(batched, mode, sources);
            });
            assertNoFailures(fromBatch);
            assertNoFailures(bulk(sequential, mode, sources));

            assertCopiesMatchRowPath(batched, sequential, numDocs);
        }
    }

    /**
     * A histogram spelled with dotted keys, {@code {"h.values":[...],"h.counts":[...]}}. The batch path could join the two leaves into
     * one histogram, but the row path parses each key as its own {@code h} object. {@code ShardBatchMapper#resolveMappers} sends such
     * a batch to the row path, so both indices must end up with the same item outcomes and the same documents.
     */
    public void testSplitDottedKeysMatchRowPath() throws IOException {
        final IndexMode mode = IndexMode.TIME_SERIES;
        final String batched = "test-histogram-split-batched";
        final String sequential = "test-histogram-split-sequential";
        createHistogramIndex(batched, mode, 1, true, false);
        createHistogramIndex(sequential, mode, 1, false, false);

        final List<String> sources = new ArrayList<>();
        final int numDocs = randomIntBetween(10, 50);
        for (int n = 0; n < numDocs; n++) {
            sources.add(source(mode, n, randomBoolean() ? "\"h.values\":[1.0,2.0],\"h.counts\":[1,3]" : randomHistogram()));
        }

        final BulkResponse fromBatch = bulk(batched, mode, sources);
        final BulkResponse fromRow = bulk(sequential, mode, sources);
        assertSameItemOutcomes(fromBatch, fromRow);

        assertCopiesMatchRowPath(batched, sequential, successCount(fromRow));
    }

    /**
     * The translog stores a batch as rows, so local recovery replays batch-indexed documents through the row path. Every acknowledged
     * document, including any with split dotted keys, must survive that replay unchanged.
     */
    public void testTranslogRecoveryKeepsAcknowledgedDocuments() throws Exception {
        final IndexMode mode = IndexMode.TIME_SERIES;
        final String index = "test-histogram-translog";
        // No replica, so failing the primary recovers it from its own store and translog rather than from a peer.
        createHistogramIndex(index, mode, 0, true, false);

        final List<String> sources = new ArrayList<>();
        final int numDocs = randomIntBetween(20, 200);
        for (int n = 0; n < numDocs; n++) {
            sources.add(source(mode, n, randomBoolean() && n > 0 ? "\"h.values\":[1.0],\"h.counts\":[1]" : randomHistogram()));
        }
        final int acknowledged = successCount(bulk(index, mode, sources));
        assertThat(acknowledged, greaterThan(0));
        refresh(index);
        final List<Map<String, Object>> before = sources(index, null, acknowledged);

        final IndexShard failed = indexShard(shardRoutingTable(index).primaryShard());
        failed.failShard("test", new IOException("simulated"));
        // Failing the shard is asynchronous, and the recovered primary keeps the allocation id of its in-sync copy, so wait for a new
        // shard instance to have started instead.
        assertBusy(() -> {
            final ShardRouting primary = shardRoutingTable(index).primaryShard();
            assertTrue(primary.assignedToNode());
            final IndexShard recovered = indexShard(primary);
            assertNotNull(recovered);
            assertNotSame(failed, recovered);
            assertThat(recovered.state(), equalTo(IndexShardState.STARTED));
        });
        ensureGreen(index);

        final RecoveryState recovery = indexShard(shardRoutingTable(index).primaryShard()).recoveryState();
        assertThat(recovery.getRecoverySource().getType(), equalTo(RecoverySource.Type.EXISTING_STORE));
        assertThat("recovery must replay the translog", recovery.getTranslog().recoveredOperations(), greaterThan(0));

        refresh(index);
        assertThat(sources(index, null, acknowledged), equalTo(before));
    }

    /**
     * A create version conflict in the middle of a bulk of valid histograms. The primary maps the whole batch, but the replica can only
     * batch the run before the failed item and indexes the rest on the row path; both copies must still match each other and the row
     * path.
     */
    public void testReplicaAfterFailedPrimaryItem() throws IOException {
        final IndexMode mode = IndexMode.COLUMNAR;
        final String batched = "test-histogram-conflict-batched";
        final String sequential = "test-histogram-conflict-sequential";
        createHistogramIndex(batched, mode, 1, true, false);
        createHistogramIndex(sequential, mode, 1, false, false);

        final int numDocs = randomIntBetween(20, 200);
        final int conflictAt = randomIntBetween(1, numDocs - 2);
        final List<String> sources = new ArrayList<>();
        for (int n = 0; n < numDocs; n++) {
            sources.add(source(mode, n, randomHistogram()));
        }
        // Index the conflicting id up front, in both indices, so that the bulk's create for it fails.
        for (String index : List.of(batched, sequential)) {
            assertNoFailures(
                client().bulk(new BulkRequest().add(indexRequest(index, mode, conflictAt, sources.get(conflictAt)))).actionGet()
            );
        }

        final BulkResponse fromBatch = withBatchLogging(mockLog -> {
            expectBatchIndexedOnPrimary(mockLog);
            return bulk(batched, mode, sources);
        });
        final BulkResponse fromRow = bulk(sequential, mode, sources);
        for (BulkResponse response : List.of(fromBatch, fromRow)) {
            for (BulkItemResponse item : response.getItems()) {
                if (item.getItemId() == conflictAt) {
                    assertTrue("create of the existing id must fail", item.isFailed());
                    assertThat(item.getFailure().getStatus(), equalTo(RestStatus.CONFLICT));
                } else {
                    assertFalse(item.getFailureMessage(), item.isFailed());
                }
            }
        }

        assertCopiesMatchRowPath(batched, sequential, numDocs);
    }

    /**
     * One malformed histogram, with {@code ignore_malformed: true}, in a large bulk. The mapper refuses the chunk, which falls back to
     * the row path; the result must match the row path, with the malformed field recorded in {@code _ignored}.
     */
    public void testMalformedHistogramFallsBackUnderIgnoreMalformed() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.TIME_SERIES);
        final String batched = "test-histogram-malformed-batched";
        final String sequential = "test-histogram-malformed-sequential";
        createHistogramIndex(batched, mode, 1, true, true);
        createHistogramIndex(sequential, mode, 1, false, true);

        final int numDocs = randomIntBetween(500, 2000);
        final int malformedAt = randomIntBetween(0, numDocs - 1);
        final List<String> sources = new ArrayList<>();
        for (int n = 0; n < numDocs; n++) {
            sources.add(source(mode, n, n == malformedAt ? malformedHistogram() : randomHistogram()));
        }

        final BulkResponse fromBatch = withBatchLogging(mockLog -> {
            // TODO: a mapper refusing a chunk is an expected fallback, so it should not be logged as a WARN with a stack trace.
            // Once ShardBatchMapper#mapColumnBatch stops doing so, assert that no such WARN appears.
            expectChunkFallback(mockLog);
            return bulk(batched, mode, sources);
        });
        assertNoFailures(fromBatch);
        assertNoFailures(bulk(sequential, mode, sources));

        assertCopiesMatchRowPath(batched, sequential, numDocs);
        for (String index : List.of(batched, sequential)) {
            assertIgnoredHistogramIn(index, malformedAt);
        }
    }

    /**
     * A bulk larger than {@link ShardBatchIndexer#BATCH_CHUNK_SIZE} with one malformed histogram in a later chunk. The earlier chunks
     * are batch-indexed, the refused chunk falls back to the row path, which fails just the malformed item.
     */
    public void testMalformedHistogramInLaterChunk() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.COLUMNAR, IndexMode.TIME_SERIES);
        final String batched = "test-histogram-chunk-batched";
        final String sequential = "test-histogram-chunk-sequential";
        createHistogramIndex(batched, mode, 1, true, false);
        createHistogramIndex(sequential, mode, 1, false, false);

        final int numDocs = ShardBatchIndexer.BATCH_CHUNK_SIZE + randomIntBetween(1, 1000);
        final int malformedAt = randomIntBetween(ShardBatchIndexer.BATCH_CHUNK_SIZE, numDocs - 1);
        final List<String> sources = new ArrayList<>();
        for (int n = 0; n < numDocs; n++) {
            sources.add(source(mode, n, n == malformedAt ? malformedHistogram() : randomHistogram()));
        }

        final BulkResponse fromBatch = withBatchLogging(mockLog -> {
            expectBatchIndexedOnPrimary(mockLog);
            expectChunkFallback(mockLog);
            return bulk(batched, mode, sources);
        });
        final BulkResponse fromRow = bulk(sequential, mode, sources);
        for (BulkResponse response : List.of(fromBatch, fromRow)) {
            for (BulkItemResponse item : response.getItems()) {
                if (item.getItemId() == malformedAt) {
                    assertTrue("the malformed histogram must fail", item.isFailed());
                    assertThat(item.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                } else {
                    assertFalse(item.getFailureMessage(), item.isFailed());
                }
            }
        }

        assertCopiesMatchRowPath(batched, sequential, numDocs - 1);
    }

    /**
     * Creates {@code target}, with one shard and {@code replicas} replicas. A {@code TIME_SERIES} target is a data stream: a time-series
     * index created directly needs a {@code routing_path}, which takes it off the batch path, whereas a data stream derives its tsid from
     * the dimensions in the mapping.
     */
    private void createHistogramIndex(String target, IndexMode mode, int replicas, boolean batchIndexing, boolean ignoreMalformed)
        throws IOException {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", replicas)
            .put(IndexSettings.MODE.getKey(), mode.getName())
            .put(IndexSettings.BATCH_INDEXING.getKey(), batchIndexing)
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .build();
        // A long dimension, as the batch path does not yet take keyword dimensions.
        final String timeSeriesFields = mode == IndexMode.TIME_SERIES ? """
            "@timestamp": { "type": "date" },
            "series_id": { "type": "long", "time_series_dimension": true },
            """ : "";
        final String mapping = Strings.format("""
            {
              "dynamic": "strict",
              "properties": {
                %s
                "n": { "type": "long" },
                "h": { "type": "histogram", "ignore_malformed": %s }
              }
            }
            """, timeSeriesFields, ignoreMalformed);
        if (mode == IndexMode.TIME_SERIES) {
            final var template = new TransportPutComposableIndexTemplateAction.Request(target + "-template");
            template.indexTemplate(
                ComposableIndexTemplate.builder()
                    .indexPatterns(List.of(target))
                    .template(new Template(settings, CompressedXContent.fromJSON(mapping), null))
                    .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                    .build()
            );
            assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, template));
            assertAcked(
                client().execute(
                    CreateDataStreamAction.INSTANCE,
                    new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, target)
                )
            );
        } else {
            assertAcked(prepareCreate(target).setSettings(settings).setMapping(mapping));
        }
        ensureGreen(target);
    }

    /**
     * The JSON source of document {@code n}, with {@code histogramFields} spliced in, or without a histogram if that is {@code null}.
     * A time-series document gets a timestamp unique to {@code n}, so that its {@code _id} is too.
     */
    private String source(IndexMode mode, int n, @Nullable String histogramFields) {
        final StringBuilder source = new StringBuilder("{\"n\":").append(n);
        if (mode == IndexMode.TIME_SERIES) {
            source.append(",\"@timestamp\":").append(baseTimestamp + n * 100L).append(",\"series_id\":1");
        }
        if (histogramFields != null) {
            source.append(',').append(histogramFields);
        }
        return source.append('}').toString();
    }

    /** A valid histogram of up to five centroids, possibly empty, with increasing values and non-negative counts. */
    private static String randomHistogram() {
        final double[] values = new double[randomIntBetween(0, 5)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomDoubleBetween(-100, 100, true);
        }
        Arrays.sort(values);
        final long[] counts = new long[values.length];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = randomLongBetween(0, 1000);
        }
        return "\"h\":{\"values\":" + Arrays.toString(values) + ",\"counts\":" + Arrays.toString(counts) + "}";
    }

    /** Decreasing values: well-formed as columns, so only the mapper can refuse it, and malformed for the row path. */
    private static String malformedHistogram() {
        return "\"h\":{\"values\":[2.0,1.0],\"counts\":[1,1]}";
    }

    private static IndexRequest indexRequest(String index, IndexMode mode, int n, String source) {
        final IndexRequest request = new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(source, XContentType.JSON);
        // A time-series index derives the _id from the dimensions and the timestamp, and refuses one set by the client.
        return mode == IndexMode.TIME_SERIES ? request : request.id("d" + n);
    }

    /** Bulk-indexes {@code sources} into {@code index} through the coordinating-only node, in order, so item {@code n} is document n. */
    private BulkResponse bulk(String index, IndexMode mode, List<String> sources) {
        final BulkRequest request = new BulkRequest();
        for (int n = 0; n < sources.size(); n++) {
            request.add(indexRequest(index, mode, n, sources.get(n)));
        }
        return client(coordinatingNode()).bulk(request).actionGet();
    }

    private String coordinatingNode() {
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().canContainData() == false
                && internalCluster().clusterService(nodeName).localNode().isMasterNode() == false) {
                return nodeName;
            }
        }
        return internalCluster().getNodeNames()[internalCluster().getNodeNames().length - 1];
    }

    /**
     * Asserts that the primary and the replica of {@code batched} each hold the same {@code expectedDocs} documents as
     * {@code sequential}, the twin index written by the row path.
     */
    private void assertCopiesMatchRowPath(String batched, String sequential, int expectedDocs) {
        refresh(batched, sequential);
        final List<Map<String, Object>> expected = sources(sequential, null, expectedDocs);
        final IndexShardRoutingTable routing = shardRoutingTable(batched);
        assertThat("primary of [" + batched + "]", sources(batched, onlyNode(routing.primaryShard()), expectedDocs), equalTo(expected));
        for (ShardRouting replica : routing.replicaShards()) {
            assertThat("replica of [" + batched + "]", sources(batched, onlyNode(replica), expectedDocs), equalTo(expected));
        }
    }

    /** The synthetic source of every document in {@code index}, in document order, asserting there are {@code expectedDocs}. */
    private List<Map<String, Object>> sources(String index, @Nullable String preference, int expectedDocs) {
        final List<Map<String, Object>> sources = new ArrayList<>();
        assertResponse(
            prepareSearch(index).setPreference(preference).setSize(expectedDocs + 1).setTrackTotalHits(true).addSort("n", SortOrder.ASC),
            response -> {
                assertNoFailures(response);
                assertThat("documents in [" + index + "]", response.getHits().getTotalHits().value(), equalTo((long) expectedDocs));
                for (SearchHit hit : response.getHits().getHits()) {
                    sources.add(hit.getSourceAsMap());
                }
            }
        );
        return sources;
    }

    /** Asserts that document {@code n} is the only one in {@code index} whose {@code h} is listed in {@code _ignored}. */
    private void assertIgnoredHistogramIn(String index, int n) {
        assertResponse(
            prepareSearch(index).setQuery(QueryBuilders.termQuery(IgnoredFieldMapper.NAME, "h")).setTrackTotalHits(true),
            response -> {
                assertNoFailures(response);
                assertThat("documents with [h] in [_ignored] in [" + index + "]", response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getAt(0).getSourceAsMap().get("n"), equalTo(n));
            }
        );
    }

    private static void assertSameItemOutcomes(BulkResponse fromBatch, BulkResponse fromRow) {
        assertThat(fromBatch.getItems().length, equalTo(fromRow.getItems().length));
        for (int i = 0; i < fromRow.getItems().length; i++) {
            final BulkItemResponse batchItem = fromBatch.getItems()[i];
            final BulkItemResponse rowItem = fromRow.getItems()[i];
            assertThat("item [" + i + "] failed: " + batchItem.getFailureMessage(), batchItem.isFailed(), equalTo(rowItem.isFailed()));
            if (rowItem.isFailed()) {
                assertThat("status of item [" + i + "]", batchItem.getFailure().getStatus(), equalTo(rowItem.getFailure().getStatus()));
            }
        }
    }

    private static int successCount(BulkResponse response) {
        return (int) Arrays.stream(response.getItems()).filter(item -> item.isFailed() == false).count();
    }

    private IndexShardRoutingTable shardRoutingTable(String target) {
        return clusterService().state().routingTable().index(concreteIndex(target)).shard(0);
    }

    /** {@code target} itself, or its backing index if it is a data stream. */
    private String concreteIndex(String target) {
        return clusterService().state().routingTable().hasIndex(target) ? target : getDataStreamBackingIndexNames(target).getFirst();
    }

    private IndexShard indexShard(ShardRouting shard) {
        return internalCluster().getInstance(IndicesService.class, nodeName(shard)).getShardOrNull(shard.shardId());
    }

    private String nodeName(ShardRouting shard) {
        return clusterService().state().nodes().get(shard.currentNodeId()).getName();
    }

    private static String onlyNode(ShardRouting shard) {
        return "_only_nodes:" + shard.currentNodeId();
    }

    private static void expectBatchIndexedOnPrimary(MockLog mockLog) {
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "batch indexed on primary",
                ShardBatchIndexer.class.getName(),
                Level.TRACE,
                "batch indexed * operations on primary shard *"
            )
        );
    }

    private static void expectChunkFallback(MockLog mockLog) {
        mockLog.addExpectation(
            new MockLog.SeenEventExpectation(
                "chunk refused by the histogram mapper",
                ShardBatchMapper.class.getName(),
                Level.WARN,
                "columnar batch mapping failed on [*], falling back"
            )
        );
    }

    /**
     * Runs {@code body} with the batch indexer's TRACE and the batch mapper's DEBUG logging on, then asserts the expectations it added
     * to the {@link MockLog} it is given.
     */
    private static <T> T withBatchLogging(Function<MockLog, T> body) {
        final Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        final Logger mapperLogger = LogManager.getLogger(ShardBatchMapper.class);
        final Level origBatchLevel = batchLogger.getLevel();
        final Level origMapperLevel = mapperLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        Loggers.setLevel(mapperLogger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class, ShardBatchMapper.class)) {
            final T result = body.apply(mockLog);
            mockLog.assertAllExpectationsMatched();
            return result;
        } finally {
            Loggers.setLevel(batchLogger, origBatchLevel);
            Loggers.setLevel(mapperLogger, origMapperLevel);
        }
    }
}

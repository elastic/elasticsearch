/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.script.MockScriptEngine.mockInlineScript;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class PreResolvedUpdatesTests extends IndexShardTestCase {
    /**
     * Records every pre-resolution so that release can be asserted after each test, and can inject behaviour or
     * failures between pre-resolution and execution.
     */
    private static class TrackingUpdateHelper extends UpdateHelper {
        // copy-on-write so the @After release assertion can safely iterate even if a timed-out bulk is still running
        private final List<PreResolvedUpdate> preResolved = new CopyOnWriteArrayList<>();
        private final Set<String> preResolvedIds = ConcurrentCollections.newConcurrentSet();
        private final AtomicInteger livePrepareCount = new AtomicInteger();
        private final Set<String> livePreparedIds = ConcurrentCollections.newConcurrentSet();
        private CheckedRunnable<Exception> afterPreResolve;
        private RuntimeException failure;

        TrackingUpdateHelper() {
            super(mockScriptService());
        }

        /** Runs the given action after each successful pre-resolution. */
        void doAfterPreResolve(CheckedRunnable<Exception> action) {
            this.afterPreResolve = action;
        }

        /** Makes every subsequent pre-resolution fail. */
        void failPreResolutions() {
            this.failure = new RuntimeException("injected pre-resolution failure");
        }

        List<PreResolvedUpdate> preResolved() {
            return preResolved;
        }

        boolean hasPreResolvedId(String id) {
            return preResolvedIds.contains(id);
        }

        int livePrepareCount() {
            return livePrepareCount.get();
        }

        boolean hasLivePreparedId(String id) {
            return livePreparedIds.contains(id);
        }

        @Override
        public Result prepare(
            UpdateRequest request,
            IndexShard indexShard,
            LongSupplier nowInMillis,
            FetchSourceContext fetchSourceContext,
            SplitShardCountSummary splitShardCountSummary
        ) throws IOException {
            livePrepareCount.incrementAndGet();
            livePreparedIds.add(request.id());
            return super.prepare(request, indexShard, nowInMillis, fetchSourceContext, splitShardCountSummary);
        }

        @Override
        public PreResolvedUpdate preResolve(
            UpdateRequest request,
            IndexShard indexShard,
            LongSupplier nowInMillis,
            FetchSourceContext fetchSourceContext,
            SplitShardCountSummary splitShardCountSummary
        ) {
            if (failure != null) {
                throw failure;
            }
            PreResolvedUpdate result = super.preResolve(request, indexShard, nowInMillis, fetchSourceContext, splitShardCountSummary);
            if (result != null) {
                preResolved.add(result);
                // We clear the request reference once we close the PreResolvedUpdate, that's why we need to keep the ids
                preResolvedIds.add(result.id());
                if (afterPreResolve != null) {
                    try {
                        afterPreResolve.run();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    private static ScriptService mockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = Map.of(
            "ctx._source.scripted = true",
            vars -> ((Map<String, Object>) ((Map<String, Object>) vars.get("ctx")).get("_source")).put("scripted", true),
            "ctx.op = 'delete'",
            vars -> ((Map<String, Object>) vars.get("ctx")).put("op", "delete"),
            "ctx.op = 'noop'",
            vars -> ((Map<String, Object>) vars.get("ctx")).put("op", "noop")
        );
        MockScriptEngine engine = new MockScriptEngine("mock", scripts, Map.of());
        return new ScriptService(
            Settings.EMPTY,
            Map.of(engine.getType(), engine),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
    }

    private final AtomicInteger mappingUpdates = new AtomicInteger();
    private TrackingUpdateHelper updateHelper;
    private ClusterService clusterService;
    private MockTransportService transportService;
    private IndexShard primary;
    private PrefetchCountingDirectory prefetchCountingDirectory;

    @Before
    public void createServices() throws IOException {
        updateHelper = new TrackingUpdateHelper();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        primary = newStartedPrimary();
    }

    @After
    public void closeServices() throws IOException {
        IOUtils.close(() -> closeShards(primary), transportService, clusterService);
    }

    @Override
    protected Store createStore(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        var base = newFSDirectory(shardPath.resolveIndex());
        prefetchCountingDirectory = new PrefetchCountingDirectory(base);
        return createStore(shardPath.getShardId(), indexSettings, prefetchCountingDirectory);
    }

    @After
    public void assertPreResolvedReleased() {
        for (UpdateHelper.PreResolvedUpdate preResolved : updateHelper.preResolved()) {
            assertTrue("pre-resolved update was neither consumed nor released", preResolved.isReleased());
        }
    }

    public void testBulkPreResolvesUpdates() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        indexDoc(primary, "_doc", "1", "{\"foo\" : \"bar\"}");
        primary.refresh("test");
        indexDoc(primary, "_doc", "2", "{\"foo\" : \"bar\"}");

        BulkShardRequest request = request(
            primary,
            new IndexRequest("index").id("9").source("{\"foo\":\"bar\"}", XContentType.JSON),
            update("0", "{\"foo\":\"f1\"}"),
            update("missing"),
            update("0", "{\"bar\":\"b1\"}"),
            update("1"),
            update("2")
        );
        executeBulk(primary, request);

        // "0", "1" and "2" pre-resolve; the duplicate op and the missing document do not. The un-refreshed "2" still
        // yields an index-backed get result: its realtime get has no translog location to read, so it refreshes
        // internally instead of reading the translog.
        assertEquals(3, updateHelper.preResolved().size());
        assertIdsArePreResolved("0", "1", "2");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        assertEquals(DocWriteResponse.Result.CREATED, response(request, 0).getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 1).getResponse().getResult());
        assertTrue(response(request, 2).isFailed());
        assertThat(response(request, 2).getFailure().getCause(), instanceOf(DocumentMissingException.class));
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 3).getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 4).getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 5).getResponse().getResult());

        assertEquals("bar", source(primary, "9").get("foo"));
        Map<String, Object> doc0 = source(primary, "0");
        assertEquals("f1", doc0.get("foo"));
        assertEquals("b1", doc0.get("bar"));
        assertEquals("updated", source(primary, "1").get("foo"));
        assertEquals("updated", source(primary, "2").get("foo"));
        assertFalse(
            primary.getService()
                .getForUpdate(
                    "missing",
                    null,
                    UNASSIGNED_SEQ_NO,
                    UNASSIGNED_PRIMARY_TERM,
                    FetchSourceContext.FETCH_ALL_SOURCE,
                    SplitShardCountSummary.IRRELEVANT
                )
                .isExists()
        );

    }

    public void testSettingDisabledSkipsPreResolution() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0"));
        executeBulk(primary, request, threadPool, false);

        assertEquals(0, updateHelper.preResolved().size());
        assertEquals(1, updateHelper.livePrepareCount());
        assertIdsArePreparedLive("0");
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 0).getResponse().getResult());
        assertEquals(0, prefetchCountingDirectory.prefetchCount());
    }

    public void testPreResolutionFailureFallsBackToLivePath() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        indexDoc(primary, "_doc", "1", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        // the first update pre-resolves, then pre-resolution of the second blows up: the first one's acquired searcher must be
        // released and both updates must succeed on the live path
        updateHelper.doAfterPreResolve(updateHelper::failPreResolutions);
        BulkShardRequest request = request(primary, update("0"), update("1"));
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertIdsArePreResolved("0");
        assertEquals("both updates must have prepared on the live path", 2, updateHelper.livePrepareCount());
        assertIdsArePreparedLive("0", "1");
        // Prefetch is triggered per pre-resolved document, in this case the first update was pre-resolved correctly
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThanOrEqualTo(1));

        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 0).getResponse().getResult());
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 1).getResponse().getResult());
        assertEquals("updated", source(primary, "0").get("foo"));
        assertEquals("updated", source(primary, "1").get("foo"));
    }

    public void testIfSeqNoValidatedAgainstPreResolvedGet() throws Exception {
        var indexed = indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        boolean matching = randomBoolean();
        UpdateRequest update = update("0").setIfSeqNo(matching ? indexed.getSeqNo() : indexed.getSeqNo() + 1)
            .setIfPrimaryTerm(primary.getOperationPrimaryTerm());
        BulkShardRequest request = request(primary, update);
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertIdsArePreResolved("0");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        BulkItemResponse response = response(request, 0);
        if (matching) {
            assertFalse(response.isFailed());
            assertEquals(DocWriteResponse.Result.UPDATED, response.getResponse().getResult());
        } else {
            assertTrue(response.isFailed());
            assertThat(response.getFailure().getCause(), instanceOf(VersionConflictEngineException.class));
        }
    }

    public void testNoopUpdateReleasesPreResolvedGet() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\":\"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0", "{\"foo\":\"bar\"}"));
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertIdsArePreResolved("0");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        assertEquals(DocWriteResponse.Result.NOOP, response(request, 0).getResponse().getResult());
    }

    public void testStalePreResolutionConflictRetriesOnLivePath() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        // a concurrent write right after pre-resolution makes the pre-resolved get stale
        updateHelper.doAfterPreResolve(() -> indexDoc(primary, "_doc", "0", "{\"foo\" : \"concurrent\"}"));
        BulkShardRequest request = request(primary, update("0").retryOnConflict(1));
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertIdsArePreResolved("0");
        assertEquals("the conflict retry must have prepared on the live path", 1, updateHelper.livePrepareCount());
        assertIdsArePreparedLive("0");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 0).getResponse().getResult());
        assertEquals("updated", source(primary, "0").get("foo"));
    }

    public void testUpdatePrecededBySameIdWriteResolvesLive() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"old\"}");
        indexDoc(primary, "_doc", "1", "{\"foo\" : \"old\"}");
        primary.refresh("test");

        // an update preceded by a same-id write of any op type must observe that write: the update's doc matches the
        // pre-bulk source of doc 0, so a stale pre-resolved get would detect a noop and silently drop the update
        BulkShardRequest request = request(
            primary,
            new IndexRequest("index").id("0").source("{\"foo\":\"new\"}", XContentType.JSON),
            update("0", "{\"foo\":\"old\"}"),
            new DeleteRequest("index", "1"),
            update("1", "{\"foo\":\"upserted\"}").docAsUpsert(true)
        );
        executeBulk(primary, request);

        assertEquals("updates preceded by same-id writes must not pre-resolve", 0, updateHelper.preResolved().size());
        assertEquals("both updates must have prepared on the live path", 2, updateHelper.livePrepareCount());
        assertIdsArePreparedLive("0", "1");
        assertFalse(response(request, 0).isFailed());
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 1).getResponse().getResult());
        assertEquals("old", source(primary, "0").get("foo"));
        assertFalse(response(request, 2).isFailed());
        assertEquals(DocWriteResponse.Result.CREATED, response(request, 3).getResponse().getResult());
        assertEquals("upserted", source(primary, "1").get("foo"));
    }

    public void testScriptedUpdateUsesPreResolvedGet() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\":\"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(
            primary,
            new UpdateRequest("index", "0").script(mockInlineScript("ctx._source.scripted = true"))
        );
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertIdsArePreResolved("0");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 0).getResponse().getResult());
        assertEquals(true, source(primary, "0").get("scripted"));
    }

    public void testScriptedDeleteAndNoopConsumePreResolvedGets() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\":\"bar\"}");
        indexDoc(primary, "_doc", "1", "{\"foo\":\"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(
            primary,
            new UpdateRequest("index", "0").script(mockInlineScript("ctx.op = 'delete'")),
            new UpdateRequest("index", "1").script(mockInlineScript("ctx.op = 'noop'"))
        );
        executeBulk(primary, request);

        assertEquals(2, updateHelper.preResolved().size());
        assertIdsArePreResolved("0", "1");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        assertEquals(DocWriteResponse.Result.DELETED, response(request, 0).getResponse().getResult());
        assertFalse(
            primary.getService()
                .getForUpdate(
                    "0",
                    null,
                    UNASSIGNED_SEQ_NO,
                    UNASSIGNED_PRIMARY_TERM,
                    FetchSourceContext.FETCH_ALL_SOURCE,
                    SplitShardCountSummary.IRRELEVANT
                )
                .isExists()
        );
        assertEquals(DocWriteResponse.Result.NOOP, response(request, 1).getResponse().getResult());
    }

    public void testSequenceNumbersDisabledSkipsPreResolution() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
        IndexShard seqNoDisabledPrimary = newStartedShard(true, settings);
        try {
            indexDoc(seqNoDisabledPrimary, "0", "{\"foo\": \"bar\"}");
            BulkShardRequest request = request(seqNoDisabledPrimary, update("0"));
            assertSame(
                PreResolvedUpdates.EMPTY,
                PreResolvedUpdates.resolve(
                    request,
                    seqNoDisabledPrimary,
                    updateHelper,
                    threadPool::absoluteTimeInMillis,
                    FetchSourceContext.FETCH_ALL_SOURCE
                )
            );
            assertEquals(0, prefetchCountingDirectory.prefetchCount());
        } finally {
            closeShards(seqNoDisabledPrimary);
        }
    }

    public void testContainerTakeOnceAndCloseIdempotent() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\":\"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0"));
        PreResolvedUpdates resolved = PreResolvedUpdates.resolve(
            request,
            primary,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            FetchSourceContext.FETCH_ALL_SOURCE
        );
        assertNotSame(PreResolvedUpdates.EMPTY, resolved);
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        try (UpdateHelper.PreResolvedUpdate taken = resolved.take(0)) {
            assertNotNull(taken);
            assertNull("slots are consumed at most once", resolved.take(0));
            resolved.close();
            resolved.close();
            assertFalse("closing the container must not release a taken slot", taken.isReleased());
        }

        assertNull(PreResolvedUpdates.EMPTY.take(0));
        PreResolvedUpdates.EMPTY.close();
    }

    public void testStalePreResolutionConflictFailsWithoutRetries() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        // a write landing after pre-resolution makes the pre-resolved get stale and the update fails with a conflict
        // (with the default retry_on_conflict=0). This race already exists between the get and the write of an
        // update (e.g. a thread preempted right after the get); pre-resolution keeps the same consistency
        // guarantees and just widens the window. This also proves pass 2 consumes the pre-resolved get, if the wiring
        // went dead, the live get would see the write and succeed.
        updateHelper.doAfterPreResolve(() -> indexDoc(primary, "_doc", "0", "{\"foo\" : \"concurrent\"}"));
        BulkShardRequest request = request(primary, update("0"));
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertEquals("without retries nothing prepares on the live path", 0, updateHelper.livePrepareCount());
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        BulkItemResponse response = response(request, 0);
        assertTrue(response.isFailed());
        assertThat(response.getFailure().getCause(), instanceOf(VersionConflictEngineException.class));
        assertEquals("concurrent", source(primary, "0").get("foo"));
    }

    public void testAbortedItemsAreNotPreResolved() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        indexDoc(primary, "_doc", "1", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0"), update("1"));
        request.items()[0].abort("index", new ElasticsearchException("test abort"));
        try (
            PreResolvedUpdates resolved = PreResolvedUpdates.resolve(
                request,
                primary,
                updateHelper,
                threadPool::absoluteTimeInMillis,
                FetchSourceContext.FETCH_ALL_SOURCE
            )
        ) {
            assertNotSame(PreResolvedUpdates.EMPTY, resolved);
            assertNull("aborted items never execute", resolved.get(0));
            assertNotNull(resolved.get(1));
            assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        }
    }

    public void testIdLessIndexOpDoesNotDisablePreResolution() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        // an index op whose id has not been auto-generated yet must neither break pass 1 nor block the update
        BulkShardRequest request = request(primary, new IndexRequest("index").source("{\"foo\":\"bar\"}", XContentType.JSON), update("0"));
        try (
            PreResolvedUpdates resolved = PreResolvedUpdates.resolve(
                request,
                primary,
                updateHelper,
                threadPool::absoluteTimeInMillis,
                FetchSourceContext.FETCH_ALL_SOURCE
            )
        ) {
            assertNotSame(PreResolvedUpdates.EMPTY, resolved);
            assertNull(resolved.get(0));
            assertNotNull(resolved.get(1));
            assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        }
    }

    public void testDuplicateIdObservesEarlierOpInBulk() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"f0\"}");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0", "{\"foo\":\"f1\"}"), update("0", "{\"bar\":\"b1\"}"));
        executeBulk(primary, request);

        assertEquals("the duplicate op must not pre-resolve", 1, updateHelper.preResolved().size());
        assertEquals("the duplicate op must have prepared on the live path", 1, updateHelper.livePrepareCount());
        assertIdsArePreparedLive("0");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        assertFalse(response(request, 0).isFailed());
        assertFalse(response(request, 1).isFailed());
        Map<String, Object> source = source(primary, "0");
        assertEquals("f1", source.get("foo"));
        assertEquals("b1", source.get("bar"));
    }

    public void testMappingUpdateRetryUsesLivePath() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0", "{\"newfield\":\"x\"}"));
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertIdsArePreResolved("0");
        assertEquals("the first attempt must have required a mapping update", 1, mappingUpdates.get());
        assertEquals("the retry must have prepared on the live path", 1, updateHelper.livePrepareCount());
        assertIdsArePreparedLive("0");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));

        assertFalse(response(request, 0).isFailed());
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 0).getResponse().getResult());
        assertEquals("x", source(primary, "0").get("newfield"));
    }

    public void testRoutingPreserved() throws Exception {
        indexDoc(primary, "0", "{\"foo\" : \"bar\"}", XContentType.JSON, "r1");
        primary.refresh("test");

        BulkShardRequest request = request(primary, update("0").routing("r1"));
        executeBulk(primary, request);

        assertEquals(1, updateHelper.preResolved().size());
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        assertEquals(DocWriteResponse.Result.UPDATED, response(request, 0).getResponse().getResult());
        GetResult get = primary.getService()
            .getForUpdate(
                "0",
                "r1",
                UNASSIGNED_SEQ_NO,
                UNASSIGNED_PRIMARY_TERM,
                FetchSourceContext.FETCH_ALL_SOURCE,
                SplitShardCountSummary.IRRELEVANT
            );
        assertEquals("r1", get.getFields().get(RoutingFieldMapper.NAME).getValue());
    }

    public void testUpsertOfMissingDocIsNotPreResolved() {
        BulkShardRequest request = request(primary, update("0", "{\"foo\":\"x\"}").docAsUpsert(true));
        executeBulk(primary, request);

        assertEquals(0, updateHelper.preResolved().size());
        assertEquals(0, prefetchCountingDirectory.prefetchCount());
        assertEquals(DocWriteResponse.Result.CREATED, response(request, 0).getResponse().getResult());
    }

    public void testResumeRejectionFailsBulkAndReleasesSlots() throws Exception {
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        indexDoc(primary, "_doc", "1", "{\"foo\" : \"bar\"}");
        primary.refresh("test");

        // rejects everything but force-executed tasks
        AtomicBoolean rejecting = new AtomicBoolean(false);
        ExecutorService gated = new StoppableExecutorServiceWrapper(threadPool.executor(ThreadPool.Names.WRITE)) {
            @Override
            public void execute(Runnable command) {
                if (rejecting.get() && (command instanceof AbstractRunnable runnable && runnable.isForceExecution()) == false) {
                    throw new EsRejectedExecutionException("test rejection", false);
                }
                super.execute(command);
            }
        };
        // a spy is the only practical way to swap a single executor: ThreadPool has no interface to delegate through
        // and constructing a second real ThreadPool would leak threads alongside the test's own pool
        ThreadPool rejectingThreadPool = spy(threadPool);
        doReturn(gated).when(rejectingThreadPool).executor(ThreadPool.Names.WRITE);

        // item 0 waits for a mapping update; resuming after it is rejected, which fails the whole bulk before item 1
        // executes, so its acquired searcher must be released by the listener chain
        BulkShardRequest request = request(primary, update("0", "{\"newfield\":\"x\"}"), update("1"));
        rejecting.set(true);
        executeBulk(primary, request, rejectingThreadPool, true);

        assertEquals(2, updateHelper.preResolved().size());
        assertIdsArePreResolved("0", "1");
        assertThat(prefetchCountingDirectory.prefetchCount(), greaterThan(0));
        assertTrue(response(request, 0).isFailed());
        assertThat(response(request, 0).getFailure().getCause(), instanceOf(EsRejectedExecutionException.class));
        assertTrue(response(request, 1).isFailed());
        assertThat(response(request, 1).getFailure().getCause(), instanceOf(EsRejectedExecutionException.class));
    }

    private void assertIdsArePreResolved(String... ids) {
        assertThat(ids.length, is(equalTo(updateHelper.preResolved().size())));
        for (String id : ids) {
            assertTrue("Expected to have pre-resolved id " + id, updateHelper.hasPreResolvedId(id));
        }
    }

    private void assertIdsArePreparedLive(String... ids) {
        assertThat(ids.length, is(equalTo(updateHelper.livePrepareCount())));
        for (String id : ids) {
            assertTrue("Expected to have prepared id " + id + " on the live path", updateHelper.hasLivePreparedId(id));
        }
    }

    /** Applies mapping updates directly to the shard instead of going through the master. */
    private class ShardMappingUpdatedAction extends MappingUpdatedAction {
        private final IndexShard primary;

        ShardMappingUpdatedAction(IndexShard primary) {
            super(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings());
            this.primary = primary;
        }

        @Override
        public void updateMappingOnMaster(Index index, CompressedXContent mappingUpdate, ActionListener<Void> listener) {
            mappingUpdates.incrementAndGet();
            try {
                updateMappings(
                    primary,
                    IndexMetadata.builder(primary.indexSettings().getIndexMetadata()).putMapping(mappingUpdate.string()).build()
                );
                // publish a new cluster state so that the observer waiting for the mapping update fires
                ClusterState state = clusterService.state();
                ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).version(state.version() + 1).build());
                listener.onResponse(null);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private IndexShard newStartedPrimary() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("index")
            .putMapping("""
                { "properties": { "foo":  { "type": "text"}, "bar":  { "type": "text"}}}""")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .primaryTerm(0, 1)
            .build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        return primary;
    }

    private static UpdateRequest update(String id, String doc) {
        return new UpdateRequest("index", id).doc(doc, XContentType.JSON);
    }

    private static UpdateRequest update(String id) {
        return update(id, "{\"foo\":\"updated\"}");
    }

    private static BulkShardRequest request(IndexShard primary, DocWriteRequest<?>... requests) {
        BulkItemRequest[] items = new BulkItemRequest[requests.length];
        for (int i = 0; i < requests.length; i++) {
            items[i] = new BulkItemRequest(i, requests[i]);
        }
        return new BulkShardRequest(primary.shardId(), SplitShardCountSummary.IRRELEVANT, WriteRequest.RefreshPolicy.NONE, items);
    }

    private void executeBulk(IndexShard primary, BulkShardRequest request) {
        executeBulk(primary, request, threadPool, true);
    }

    private void executeBulk(IndexShard primary, BulkShardRequest request, ThreadPool actionThreadPool, boolean preResolveEnabled) {
        Settings settings = Settings.builder().put(PreResolvedUpdates.PRE_RESOLVE_BULK_UPDATES.getKey(), preResolveEnabled).build();
        TransportShardBulkAction action = new TransportShardBulkAction(
            settings,
            transportService,
            clusterService,
            null, // indices service is unused: shardOperationOnPrimary is invoked with the shard directly
            actionThreadPool,
            null, // shard state action is unused: no replication or failure reporting in these tests
            new ShardMappingUpdatedAction(primary),
            updateHelper,
            new ActionFilters(Set.of()),
            new IndexingPressure(settings),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            DocumentParsingProvider.EMPTY_INSTANCE,
            BigArrays.NON_RECYCLING_INSTANCE
        );
        PlainActionFuture<TransportReplicationAction.PrimaryResult<BulkShardRequest, BulkShardResponse>> future = new PlainActionFuture<>();
        threadPool.executor(ThreadPool.Names.WRITE).execute(() -> action.shardOperationOnPrimary(request, primary, future));
        safeGet(future);
    }

    private static BulkItemResponse response(BulkShardRequest request, int slot) {
        return request.items()[slot].getPrimaryResponse();
    }

    private Map<String, Object> source(IndexShard primary, String id) throws IOException {
        GetResult doc = primary.getService()
            .getForUpdate(
                id,
                null,
                UNASSIGNED_SEQ_NO,
                UNASSIGNED_PRIMARY_TERM,
                FetchSourceContext.FETCH_ALL_SOURCE,
                SplitShardCountSummary.IRRELEVANT
            );
        return XContentHelper.convertToMap(doc.sourceRef(), false, XContentType.JSON).v2();
    }

    private static class PrefetchCountingDirectory extends FilterDirectory {
        private final AtomicInteger count = new AtomicInteger();

        PrefetchCountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return wrap(super.openInput(name, context));
        }

        private FilterIndexInput wrap(IndexInput delegate) {
            return new FilterIndexInput("prefetch-counting(" + delegate + ")", delegate) {
                @Override
                public void prefetch(long offset, long length) throws IOException {
                    count.incrementAndGet();
                    super.prefetch(offset, length);
                }

                @Override
                public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                    return wrap(super.slice(sliceDescription, offset, length));
                }

                @Override
                public IndexInput slice(String sliceDescription, long offset, long length, IOContext context) throws IOException {
                    return wrap(super.slice(sliceDescription, offset, length, context));
                }
            };
        }

        int prefetchCount() {
            return count.get();
        }
    }
}

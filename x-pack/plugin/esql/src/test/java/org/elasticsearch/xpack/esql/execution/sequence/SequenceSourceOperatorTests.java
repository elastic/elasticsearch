/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverEarlyTerminationException;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/**
 * Delays transport and stage-terminal callbacks deliberately to exercise ownership after the real driver context
 * has finished. Source operators, request breakers, block factories, analyzed plans and local stage drivers are real.
 */
public class SequenceSourceOperatorTests extends ESTestCase {
    /** Satisfied downstream demand preserves execution status and still releases the snapshot and allocations. */
    public void testEarlyTerminationInsideOutputDoesNotRecordFailure() {
        var executionInfo = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        executionInfo.initCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, "local", "a,b");
        try (var harness = new Harness(false, null, executionInfo)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            var checks = new AtomicInteger();
            var earlyTermination = new DriverEarlyTerminationException("downstream has enough rows");
            harness.context.initializeEarlyTerminationChecker(() -> {
                // Pass the check outside getOutput's try block, then stop inside the candidate loop.
                if (checks.incrementAndGet() == 2) {
                    throw earlyTermination;
                }
            });
            assertSame(earlyTermination, expectThrows(DriverEarlyTerminationException.class, harness.source::getOutput));
            assertEquals(2, checks.get());
            var cluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertEquals(EsqlExecutionInfo.Cluster.Status.RUNNING, cluster.getStatus());
            assertTrue(cluster.getFailures().isEmpty());
            assertFalse(executionInfo.isPartial());
            assertNull(harness.source.getOutput());
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testQueuedRenewalCannotReplaceANewerSchedule() {
        var queue = new DeterministicTaskQueue() {
            @Override
            public ThreadPool getThreadPool() {
                return new DeterministicThreadPool(Function.identity()) {
                    @Override
                    public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                        // Model the real scheduler's two phases: its timer queues work on GENERIC, which may run
                        // after cancellation. The ordinary deterministic pool runs commands directly on the timer.
                        return super.schedule(() -> executor.execute(command), delay, executor);
                    }
                };
            }
        };
        try (var harness = new Harness(false, queue)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            queue.advanceTime();
            queue.runRandomTask();
            assertTrue(queue.hasRunnableTasks());
            var response = SearchResponse.emptyResponseBuilder().build();
            try (Releasable ignored = response::decRef) {
                search.listener.onResponse(response);
            }
            queue.runAllRunnableTasks();
            assertTrue("the already-queued renewal was superseded by the successful search", harness.client.pending.isEmpty());
            queue.runTasksUpToTimeInOrder(80_000);
            var renewal = harness.client.take(TransportSearchAction.TYPE);
            assertRenewalRequest((SearchRequest) renewal.request);
            harness.finishDriver();
            renewal.listener.onFailure(new IllegalStateException("cancelled"));
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            queue.runAllTasksInTimeOrder();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testRenewalDuringStageWaitPreservesBlockingAndCompletion() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(true, queue)) {
            harness.deferStage = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertNull(harness.source.getOutput());
            var blocked = harness.source.isBlocked().listener();
            assertFalse(blocked.isDone());
            for (long time : new long[] { 40_000, 80_000, 120_000, 160_000 }) {
                queue.runTasksUpToTimeInOrder(time);
                var renewal = harness.client.take(TransportSearchAction.TYPE);
                assertRenewalRequest((SearchRequest) renewal.request);
                var response = renewalResponse("renewed-" + time);
                try (Releasable ignored = response::decRef) {
                    renewal.listener.onResponse(response);
                }
                assertFalse("successful heartbeats must not turn a blocked stage into a busy loop", blocked.isDone());
                assertTrue(harness.client.pending.isEmpty());
            }
            harness.deferredStage.onResponse(null);
            assertTrue(blocked.isDone());
            assertNull(harness.source.getOutput());
            var next = harness.client.take(TransportSearchAction.TYPE);
            assertEquals(new BytesArray("renewed-160000"), ((SearchRequest) next.request).source().pointInTimeBuilder().getEncodedId());
            harness.finishDriver();
            next.listener.onFailure(new IllegalStateException("closed"));
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            queue.runAllTasksInTimeOrder();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testCloseWaitsForRenewalAndSearchInEitherCallbackOrder() {
        for (boolean renewalFirst : List.of(false, true)) {
            var queue = new DeterministicTaskQueue();
            try (var harness = new Harness(false, queue)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                assertNull(harness.source.getOutput());
                var search = harness.client.take(TransportSearchAction.TYPE);
                queue.runTasksUpToTimeInOrder(40_000);
                var renewal = harness.client.take(TransportSearchAction.TYPE);
                assertRenewalRequest((SearchRequest) renewal.request);
                harness.finishDriver();
                assertFalse(harness.done.isDone());
                Runnable deliverRenewal = () -> {
                    var response = renewalResponse("latest-renewed");
                    try (Releasable ignored = response::decRef) {
                        renewal.listener.onResponse(response);
                    }
                };
                Runnable deliverSearch = () -> search.listener.onFailure(new IllegalStateException("search cancelled"));
                (renewalFirst ? deliverRenewal : deliverSearch).run();
                assertFalse("both outstanding callbacks must release ownership before PIT close", harness.done.isDone());
                assertTrue(harness.client.pending.isEmpty());
                (renewalFirst ? deliverSearch : deliverRenewal).run();
                var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
                assertEquals(new BytesArray("latest-renewed"), ((ClosePointInTimeRequest) close.request).getId());
                close.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
                queue.runAllTasksInTimeOrder();
                assertTrue(harness.client.pending.isEmpty());
            }
        }
    }

    public void testRenewalFailureWakesStageWithoutStealingItsOwnership() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(true, queue)) {
            harness.deferStage = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertNull(harness.source.getOutput());
            var blocked = harness.source.isBlocked().listener();
            queue.runTasksUpToTimeInOrder(40_000);
            harness.client.take(TransportSearchAction.TYPE).listener.onFailure(new IllegalStateException("snapshot renewal failed"));
            assertTrue(blocked.isDone());
            assertThat(
                expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage(),
                containsString("snapshot renewal failed")
            );
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            assertTrue("the stage still owns its bound input until terminal delivery", harness.breaker.getUsed() > 0);
            harness.deferredStage.onResponse(null);
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            queue.runAllTasksInTimeOrder();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    private static void assertRenewalRequest(SearchRequest request) {
        assertEquals(0, request.source().size());
        assertEquals("match_all", request.source().query().getName());
        assertArrayEquals(new String[] { "a", "b" }, request.getPointInTimeAuthorizationIndices().indices());
        assertArrayEquals(new String[] { "a", "b" }, request.getPointInTimeAuthorizationScope().indices());
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), request.getPreFilterShardSize());
        assertFalse(request.allowPartialSearchResults());
    }

    private static SearchResponse renewalResponse(String token) {
        return new SearchResponse(
            SearchHits.empty(Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN),
            InternalAggregations.EMPTY,
            null,
            false,
            null,
            null,
            1,
            null,
            2,
            2,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY,
            new BytesArray(token),
            null,
            null
        );
    }

    public void testLatePitOpenWaitsForCleanupAfterDriverContextFinish() {
        try (var harness = new Harness()) {
            var open = harness.prepareOpen();
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            open.listener.onResponse(openResponse());
            assertFalse("the driver must still own PIT cleanup", harness.done.isDone());
            var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
            assertEquals(new BytesArray("snapshot"), ((ClosePointInTimeRequest) close.request).getId());
            close.listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testLatePitOpenFailureCompletesDriverWithoutCleanup() {
        try (var harness = new Harness()) {
            var open = harness.prepareOpen();
            harness.finishDriver();
            open.listener.onFailure(new IllegalStateException("open failed"));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testLateSourceResolutionDoesNotOpenSnapshot() {
        try (var harness = new Harness()) {
            assertNull(harness.source.getOutput());
            var resolution = harness.client.take(EsqlSearchShardsAction.TYPE);
            harness.finishDriver();
            resolution.listener.onResponse(shards("a"));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testLateSearchResponseReleasesResponseAndClosesSnapshot() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            harness.finishDriver();
            var response = SearchResponse.emptyResponseBuilder().build();
            try (Releasable ignored = response::decRef) {
                search.listener.onResponse(response);
                assertTrue(response.hasReferences());
                assertFalse(harness.done.isDone());
            }
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testLateSearchFailureWaitsForSnapshotCleanupFailure() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            harness.finishDriver();
            search.listener.onFailure(new IllegalStateException("search failed after cancellation"));
            assertFalse(harness.done.isDone());
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onFailure(new IllegalStateException("cleanup failed"));
            harness.assertFinished();
        }
    }

    public void testTimedOutSearchFailsAndReleasesResources() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            var response = new SearchResponse(
                SearchHits.empty(Lucene.TOTAL_HITS_EQUAL_TO_ZERO, Float.NaN),
                InternalAggregations.EMPTY,
                null,
                true,
                null,
                null,
                1,
                null,
                2,
                2,
                0,
                1,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            try (Releasable ignored = response::decRef) {
                search.listener.onResponse(response);
                assertThat(
                    expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage(),
                    containsString("requires a complete search response")
                );
            }
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testSearchResponseAdmissionFailureClosesSnapshot() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            harness.breaker.setLimitAndOverhead(harness.breaker.getUsed() + 128, 1);
            var response = SearchResponse.emptyResponseBuilder().build();
            try (Releasable ignored = response::decRef) {
                search.listener.onResponse(response);
                expectThrows(CircuitBreakingException.class, harness.source::getOutput);
            }
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testNativeByteResponseCannotBypassCoordinatorBreaker() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            SearchHit hit = SearchHit.unpooled(0, "large-value");
            hit.setDocumentField(new DocumentField("payload", List.of(new BytesRef(new byte[1024 * 1024]))));
            var hits = new SearchHits(new SearchHit[] { hit }, null, Float.NaN);
            var response = new SearchResponse(
                hits,
                InternalAggregations.EMPTY,
                null,
                false,
                null,
                null,
                1,
                null,
                2,
                2,
                0,
                1,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            hits.decRef(); // The response owns its independent reference.
            try (Releasable ignored = response::decRef) {
                search.listener.onResponse(response);
                expectThrows(CircuitBreakingException.class, harness.source::getOutput);
            }
            assertFalse(response.hasReferences());
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testSynchronousStageCompletionIsConsumedBeforeAnotherDispatch() {
        try (var harness = new Harness(true)) {
            harness.respond(harness.preparePayloadSearch(), 0);
            assertNull(harness.source.getOutput());
            assertTrue("the synchronous replay completion must be consumed before the next search", harness.client.pending.isEmpty());
            // Only the unqualified common key is retained, so stage 1 contributes matching but needs no payload replay.
            try (Page page = harness.source.getOutput()) {
                assertNotNull(page);
                assertEquals(1, page.getPositionCount());
                for (int column = 0; column < page.getBlockCount(); column++) {
                    BytesRefBlock block = page.getBlock(column);
                    assertEquals("shared", block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                }
            }
            assertEquals(3, harness.stageExecutions);
            assertEquals(1L, harness.source.status().payloadSearchRequests());
            assertEquals(1L, harness.source.status().rowsEmitted());
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testSynchronousStageFailureStopsDispatchImmediately() {
        try (var harness = new Harness(true)) {
            harness.failStage = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertThat(
                expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage(),
                containsString("controlled synchronous stage failure")
            );
            assertEquals(1, harness.stageExecutions);
            assertEquals(1L, harness.source.status().candidateSearchRequests());
            assertEquals(0L, harness.source.status().payloadSearchRequests());
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testCloseWhilePayloadSearchPendingRetainsAdmissionUntilTerminalDelivery() {
        for (boolean failure : List.of(false, true)) {
            try (var harness = new Harness(true)) {
                var search = harness.preparePayloadSearch();
                harness.finishDriver();
                assertFalse(harness.done.isDone());
                harness.blocks.ensureAllBlocksAreReleased();
                assertTrue("the pending payload query must retain its identity admission", harness.breaker.getUsed() > 0);
                if (failure) {
                    search.listener.onFailure(new IllegalStateException("payload search failed after close"));
                } else {
                    harness.respond(search, 0);
                }
                assertEquals("terminal delivery must refund payload query admission", 0, harness.breaker.getUsed());
                assertEquals("closed sources must not plan payload replay", 2, harness.stageExecutions);
                assertFalse(harness.done.isDone());
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
            }
        }
    }

    public void testCloseWhilePayloadReplayPendingRetainsAdmissionUntilTerminalDelivery() {
        for (boolean failure : List.of(false, true)) {
            try (var harness = new Harness(true)) {
                harness.deferStage = 3;
                harness.respond(harness.preparePayloadSearch(), 0);
                assertNull(harness.source.getOutput());
                assertNotNull(harness.deferredStage);
                assertEquals(3, harness.stageExecutions);
                harness.finishDriver();
                assertFalse(harness.done.isDone());
                assertTrue("the pending replay must retain event metadata and bound blocks", harness.breaker.getUsed() > 0);
                if (failure) {
                    harness.deferredStage.onFailure(new IllegalStateException("payload replay failed after close"));
                } else {
                    harness.deferredStage.onResponse(null);
                }
                assertEquals("terminal replay delivery must refund its metadata and blocks", 0, harness.breaker.getUsed());
                harness.blocks.ensureAllBlocksAreReleased();
                assertEquals(0L, harness.source.status().rowsEmitted());
                assertFalse(harness.done.isDone());
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
            }
        }
    }

    private static SearchResponse eventResponse(int stage) {
        String index = stage == 0 ? "a" : "b";
        String timestamp = stage == 0 ? "2026-09-25T21:14:51Z" : "2026-09-25T21:14:52Z";
        SearchHit hit = SearchHit.unpooled(0, "event-" + index);
        hit.shard(new SearchShardTarget("node", new ShardId(index, index + "-uuid", 0), null));
        hit.setDocumentField(new DocumentField("key", List.of(new BytesRef("shared"))));
        hit.setDocumentField(new DocumentField("@timestamp", List.of(java.time.Instant.parse(timestamp).toEpochMilli())));
        Object[] sort = { timestamp, ShardDocSortField.encodeShardAndDoc(stage, 0) };
        hit.sortValues(SearchSortValues.fromFormattedAndRaw(sort, sort));
        var hits = new SearchHits(new SearchHit[] { hit }, null, Float.NaN);
        try {
            return new SearchResponse(
                hits,
                InternalAggregations.EMPTY,
                null,
                false,
                null,
                null,
                1,
                null,
                2,
                2,
                0,
                1,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
        } finally {
            hits.decRef();
        }
    }

    private static OpenPointInTimeResponse openResponse() {
        return new OpenPointInTimeResponse(new BytesArray("snapshot"), 2, 2, 0, 0, SearchResponse.Clusters.EMPTY);
    }

    private static SearchShardsResponse shards(String index) {
        return new SearchShardsResponse(
            List.of(new SearchShardsGroup(new ShardId(index, index + "-uuid", 0), List.of("node"), false, SplitShardCountSummary.UNSET)),
            0,
            List.of(),
            Map.of()
        );
    }

    private static Sequence plan() {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "key",
            new EsField("key", DataType.KEYWORD, Map.of(), true, NONE)
        );
        var analyzer = EsqlTestUtils.analyzer().minimumTransportVersion(TransportVersion.current());
        for (String index : List.of("a", "b")) {
            analyzer.addIndex(new EsIndex(index, mapping, Map.of(index, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        return analyzer.query("""
            sequence by key with {"maxspan":"30s"} (from a | keep key), (from b | keep key) | limit 1
            """).collect(Sequence.class).getFirst();
    }

    private static final class Harness implements Releasable {
        private final TestThreadPool realThreadPool;
        private final ThreadPool threadPool;
        private final PendingClient client;
        private final CircuitBreaker breaker;
        private final MockBlockFactory blocks;
        private final DriverContext context;
        private final SequenceSourceOperator source;
        private final PlainActionFuture<Void> done = new PlainActionFuture<>();
        private int stageExecutions;
        private int failStage = -1;
        private int deferStage = -1;
        private ActionListener<Void> deferredStage;

        private Harness() {
            this(false);
        }

        private Harness(boolean realStages) {
            this(realStages, null);
        }

        private Harness(boolean realStages, DeterministicTaskQueue queue) {
            this(realStages, queue, null);
        }

        private Harness(boolean realStages, DeterministicTaskQueue queue, EsqlExecutionInfo executionInfo) {
            realThreadPool = queue == null ? new TestThreadPool("sequence-callbacks") : null;
            threadPool = queue == null ? realThreadPool : queue.getThreadPool();
            client = new PendingClient(threadPool);
            Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb")
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build();
            breaker = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getBreaker(CircuitBreaker.REQUEST);
            blocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
            context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blocks, null);
            Sequence sequence = plan();
            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader("sequence-test-caller", "owner");
                source = (SequenceSourceOperator) new SequenceSourceOperator.Factory(
                    new SequenceExec(sequence.source(), sequence),
                    client,
                    client,
                    null,
                    EsqlTestUtils.TEST_CFG,
                    (physical, output) -> {
                        if (realStages == false) {
                            throw new AssertionError("stage execution is not expected");
                        }
                        return planStage(physical, output);
                    },
                    (drivers, listener) -> {
                        if (realStages == false) {
                            throw new AssertionError("stage execution is not expected");
                        }
                        executeStage(drivers, listener);
                    },
                    threadPool.getThreadContext().newRestorableContext(true),
                    executionInfo
                ).get(context);
            }
        }

        private List<Driver> planStage(PhysicalPlan physical, Consumer<Page> output) {
            // The production local planner and driver own all source/binding/block references. Only terminal
            // delivery is controlled below, allowing close to run between computation and its parent callback.
            Settings settings = Settings.builder()
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, "0b")
                .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, "0b")
                .build();
            var planner = new LocalExecutionPlanner(
                "sequence-callbacks",
                "",
                null,
                BigArrays.NON_RECYCLING_INSTANCE,
                blocks,
                settings,
                EsqlTestUtils.TEST_CFG,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                ProjectMetadata.builder(ProjectId.DEFAULT).build(),
                new EsPhysicalOperationProviders(
                    FoldContext.small(),
                    EmptyIndexedByShardId.instance(),
                    null,
                    PlannerSettings.DEFAULTS,
                    () -> 0L,
                    QueryWarnings.EMIT
                ),
                null,
                null,
                null,
                0,
                MatcherWatchdog.noop(),
                TransportVersion.current()
            );
            return planner.plan(
                "sequence stage",
                FoldContext.small(),
                PlannerSettings.DEFAULTS,
                new OutputExec(physical, output),
                EmptyIndexedByShardId.instance(),
                true
            ).createDrivers("sequence-callbacks");
        }

        private void executeStage(List<Driver> drivers, ActionListener<Void> listener) {
            assertEquals(1, drivers.size());
            stageExecutions++;
            if (stageExecutions == failStage) {
                drivers.getFirst().abort(new IllegalStateException("controlled synchronous stage failure"), listener);
                return;
            }
            var completed = new PlainActionFuture<Void>();
            Driver.start(threadPool.getThreadContext(), Runnable::run, drivers.getFirst(), Driver.DEFAULT_MAX_ITERATIONS, completed);
            assertTrue("these bounded local identity stages must finish synchronously", completed.isDone());
            completed.actionGet();
            if (stageExecutions == deferStage) {
                assertNull(deferredStage);
                deferredStage = listener;
            } else {
                listener.onResponse(null);
            }
        }

        private void respond(Pending<SearchResponse> pending, int stage) {
            var request = (SearchRequest) pending.request;
            assertArrayEquals(new String[] { "a", "b" }, request.getPointInTimeAuthorizationIndices().indices());
            assertArrayEquals(new String[] { stage == 0 ? "a" : "b" }, request.getPointInTimeAuthorizationScope().indices());
            var response = eventResponse(stage);
            try (Releasable ignored = response::decRef) {
                pending.listener.onResponse(response);
            }
        }

        private Pending<SearchResponse> preparePayloadSearch() {
            prepareOpen().listener.onResponse(openResponse());
            for (int stage = 0; stage < 2; stage++) {
                assertNull(source.getOutput());
                respond(client.take(TransportSearchAction.TYPE), stage);
                assertNull(source.getOutput());
                assertTrue("a synchronous stage completion must be consumed before another search", client.pending.isEmpty());
            }
            assertNull(source.getOutput());
            assertEquals(2, stageExecutions);
            return client.take(TransportSearchAction.TYPE);
        }

        private Pending<OpenPointInTimeResponse> prepareOpen() {
            for (String index : List.of("a", "b")) {
                assertNull(source.getOutput());
                client.take(EsqlSearchShardsAction.TYPE).listener.onResponse(shards(index));
            }
            assertNull(source.getOutput());
            return client.take(TransportOpenPointInTimeAction.TYPE);
        }

        private void finishDriver() {
            source.close();
            context.finish();
            context.waitForAsyncActions(done);
        }

        private void assertFinished() {
            assertTrue(done.isDone());
            done.actionGet();
            assertEquals(0, breaker.getUsed());
            blocks.ensureAllBlocksAreReleased();
        }

        @Override
        public void close() {
            source.close();
            context.finish();
            if (realThreadPool != null) {
                realThreadPool.close();
            }
        }
    }

    private record Pending<Response extends ActionResponse>(
        ActionType<Response> action,
        ActionRequest request,
        ActionListener<Response> listener
    ) {}

    /** Controls only transport delivery, so callback ordering is deterministic rather than dependent on scheduler timing. */
    private static final class PendingClient extends NoOpClient {
        private final ArrayDeque<Pending<?>> pending = new ArrayDeque<>();

        private PendingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertEquals("owner", threadPool().getThreadContext().getHeader("sequence-test-caller"));
            pending.addLast(new Pending<>(action, request, listener));
        }

        private <Response extends ActionResponse> Pending<Response> take(ActionType<Response> action) {
            Pending<?> request = pending.removeFirst();
            assertEquals(action, request.action);
            // The action type and response listener were captured together in doExecute and checked above.
            @SuppressWarnings("unchecked")
            Pending<Response> typed = (Pending<Response>) request;
            return typed;
        }
    }
}

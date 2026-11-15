/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStageProjection;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.junit.BeforeClass;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/**
 * Delays transport and stage-terminal callbacks deliberately to exercise ownership after the real driver context
 * has finished. Source operators, request breakers, block factories, analyzed plans and local stage drivers are real.
 */
public class SequenceSourceOperatorTests extends ESTestCase {
    /** The real operator harness analyzes its plan through the running build's default parser. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    /** The physical hint excludes foreign indices without broadening the logical authorization carrier. */
    public void testSnapshotOpenIntersectsReadAndScopeUnionsAndKeepsAuthorizationScopes() {
        try (var harness = new Harness()) {
            var pending = prepareScopedOpen(
                harness,
                List.of(List.of("a", "b", "foreign"), List.of("b", "foreign")),
                List.of(List.of("alias-a", "-excluded"), List.of("alias-b"))
            );
            var open = (OpenPointInTimeRequest) pending.request;
            try {
                assertArrayEquals(new String[] { "alias-a", "alias-b" }, open.indices());
                assertEquals(new TermsQueryBuilder("_index", List.of("a", "b")), open.indexFilter());
                assertEquals(EsqlTestUtils.TEST_CFG.absoluteStartedTimeInMillis(), open.getAbsoluteStartMillis());
            } finally {
                closePendingOpen(harness, pending);
            }
        }
    }

    /** Crossed scopes still need both physical contexts for the existing per-stage required-field admission checks. */
    public void testSnapshotOpenKeepsCrossedReadAndScopeContexts() {
        try (var harness = new Harness()) {
            var pending = prepareScopedOpen(
                harness,
                List.of(List.of("b"), List.of("a")),
                List.of(List.of("alias-left"), List.of("alias-right"))
            );
            var request = (OpenPointInTimeRequest) pending.request;
            try {
                assertArrayEquals(new String[] { "alias-left", "alias-right" }, request.indices());
                assertEquals(new TermsQueryBuilder("_index", List.of("a", "b")), request.indexFilter());
            } finally {
                closePendingOpen(harness, pending);
            }
        }
    }

    /** The smaller physical hint must leave full positive and branch-local logical authorization carriers unchanged. */
    public void testSnapshotPagingKeepsCompleteAndBranchAuthorizationScopes() {
        try (var harness = new Harness()) {
            var pending = prepareScopedOpen(
                harness,
                List.of(List.of("a", "b", "foreign"), List.of("b", "foreign")),
                List.of(List.of("alias-a", "-excluded"), List.of("alias-b"))
            );
            pending.listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            var request = (SearchRequest) search.request;
            try {
                assertArrayEquals(new String[] { "alias-a", "alias-b" }, request.getPointInTimeAuthorizationIndices().indices());
                assertArrayEquals(new String[] { "alias-a", "-excluded" }, request.getPointInTimeAuthorizationScope().indices());
                assertEquals(Integer.valueOf(Integer.MAX_VALUE), request.getPreFilterShardSize());
            } finally {
                harness.finishDriver();
                search.listener.onFailure(new IllegalStateException("closed after request inspection"));
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
            }
        }
    }

    /** An empty physical read intersection must not become an empty request index list, which can resolve to all indices. */
    public void testSnapshotOpenEmptyReadIntersectionKeepsPositiveExpressions() {
        try (var harness = new Harness()) {
            var pending = prepareScopedOpen(
                harness,
                List.of(List.of("foreign"), List.of("foreign")),
                List.of(List.of("alias-a"), List.of("alias-b"))
            );
            var request = (OpenPointInTimeRequest) pending.request;
            try {
                assertArrayEquals(new String[] { "alias-a", "alias-b" }, request.indices());
                assertEquals(MatchNoneQueryBuilder.class, request.indexFilter().getClass());
            } finally {
                closePendingOpen(harness, pending);
            }
        }
    }

    /** Even a false constant stays on paging so eligible shard contexts still perform their existing required-field policy checks. */
    public void testSnapshotOpenLeavesRequestFiltersOnPaging() {
        List<QueryBuilder> filters = new ArrayList<>();
        filters.add(null);
        filters.addAll(
            List.of(
                new MatchAllQueryBuilder(),
                new MatchNoneQueryBuilder(),
                new MatchNoneQueryBuilder() {},
                new TermsQueryBuilder("_index", List.of("a")),
                new RangeQueryBuilder("@timestamp").gte("now-1d")
            )
        );
        for (QueryBuilder filter : filters) {
            try (var harness = new Harness(false, null, null, null, null, filter)) {
                var pending = harness.prepareOpen();
                var request = (OpenPointInTimeRequest) pending.request;
                boolean openDelivered = false;
                try {
                    assertEquals(new TermsQueryBuilder("_index", List.of("a", "b")), request.indexFilter());
                    assertArrayEquals(new String[] { "a", "b" }, request.indices());
                    openDelivered = true;
                    pending.listener.onResponse(openResponse());
                    assertNull(harness.source.getOutput());
                    var search = harness.client.pending.getFirst();
                    assertEquals(TransportSearchAction.TYPE, search.action);
                    if (filter != null) {
                        Map<String, Object> expected = XContentHelper.convertToMap(
                            JsonXContent.jsonXContent,
                            Strings.toString(filter),
                            false
                        );
                        Map<String, Object> actual = XContentHelper.convertToMap(
                            JsonXContent.jsonXContent,
                            Strings.toString(((SearchRequest) search.request).source().query()),
                            false
                        );
                        assertTrue("the original request filter must remain inside the candidate query", containsQuery(actual, expected));
                    }
                } finally {
                    finishSnapshotRequests(harness, pending, openDelivered);
                }
            }
        }
    }

    private static boolean containsQuery(Object value, Map<String, Object> expected) {
        if (expected.equals(value)) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return map.values().stream().anyMatch(child -> containsQuery(child, expected));
        }
        if (value instanceof List<?> list) {
            return list.stream().anyMatch(child -> containsQuery(child, expected));
        }
        return false;
    }

    /** Assertions can fail before or after open delivery; every already-owned callback must still be drained. */
    private static void finishSnapshotRequests(Harness harness, Pending<OpenPointInTimeResponse> open, boolean openDelivered) {
        harness.finishDriver();
        if (openDelivered == false) {
            open.listener.onResponse(openResponse());
        }
        while (harness.client.pending.isEmpty() == false) {
            var action = harness.client.pending.getFirst().action;
            if (action.equals(TransportOpenPointInTimeAction.TYPE)) {
                harness.client.take(TransportOpenPointInTimeAction.TYPE).listener.onResponse(openResponse());
            } else if (action.equals(TransportSearchAction.TYPE)) {
                harness.client.take(TransportSearchAction.TYPE).listener.onFailure(
                    new IllegalStateException("closed after request inspection")
                );
            } else if (action.equals(TransportClosePointInTimeAction.TYPE)) {
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            } else {
                throw new AssertionError("unexpected pending snapshot action [" + action.name() + "]");
            }
        }
        harness.assertFinished();
    }

    private static Pending<OpenPointInTimeResponse> prepareScopedOpen(
        Harness harness,
        List<List<String>> physicalScopes,
        List<List<String>> authorizationExpressions
    ) {
        for (int branch = 0; branch < physicalScopes.size(); branch++) {
            assertNull(harness.source.getOutput());
            var pending = harness.client.take(EsqlSearchShardsAction.TYPE);
            ((SearchShardsRequest) pending.request).indices(authorizationExpressions.get(branch).toArray(String[]::new));
            pending.listener.onResponse(shards(physicalScopes.get(branch).toArray(String[]::new)));
        }
        assertNull(harness.source.getOutput());
        return harness.client.take(TransportOpenPointInTimeAction.TYPE);
    }

    private static void closePendingOpen(Harness harness, Pending<OpenPointInTimeResponse> pending) {
        harness.finishDriver();
        pending.listener.onResponse(openResponse());
        harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
        harness.assertFinished();
    }

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
            assertNull(harness.source.getOutput());
            assertFalse(harness.source.isFinished());
            assertTrue(harness.client.pending.isEmpty());
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
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(false, queue); var log = pitCleanupLog(3)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            harness.finishDriver();
            search.listener.onFailure(new IllegalStateException("search failed after cancellation"));
            assertFalse(harness.done.isDone());
            for (int attempt = 0; attempt < 3; attempt++) {
                assertFalse(harness.done.isDone());
                var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
                assertEquals(new BytesArray("snapshot"), ((ClosePointInTimeRequest) close.request).getId());
                close.listener.onFailure(new ConnectTransportException(null, "cleanup failed for snapshot"));
                if (attempt < 2) {
                    assertFalse(harness.done.isDone());
                    advancePitCloseRetry(queue);
                } else {
                    queue.runAllTasksInTimeOrder();
                }
            }
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
            log.assertAllExpectationsMatched();
        }
    }

    /** A transient close failure must retain async ownership and the token until the retry completes. */
    public void testPitCloseFailureRetriesBeforeReleasingOwnership() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(false, queue)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            harness.finishDriver();
            var first = harness.client.take(TransportClosePointInTimeAction.TYPE);
            first.listener.onFailure(new ConnectTransportException(null, "transient cleanup failure"));
            assertFalse("a failed close must not release the PIT's async ownership", harness.done.isDone());
            advancePitCloseRetry(queue);
            var retry = harness.client.take(TransportClosePointInTimeAction.TYPE);
            assertEquals(new BytesArray("snapshot"), ((ClosePointInTimeRequest) retry.request).getId());
            assertFalse(harness.done.isDone());
            retry.listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            queue.runAllTasksInTimeOrder();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    /** A partial close response cannot be treated as confirmed cleanup, including on a repeated close request. */
    public void testIncompletePitCloseResponsesRetryAndReleaseAtTheBound() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(false, queue); var log = pitCleanupLog(3)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            harness.finishDriver();
            for (int attempt = 0; attempt < 3; attempt++) {
                var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
                assertEquals(new BytesArray("snapshot"), ((ClosePointInTimeRequest) close.request).getId());
                close.listener.onResponse(new ClosePointInTimeResponse(false, attempt == 0 ? 1 : 0));
                if (attempt < 2) {
                    assertFalse(harness.done.isDone());
                    harness.source.close();
                    assertTrue("finishing twice must not dispatch a duplicate close", harness.client.pending.isEmpty());
                }
                if (attempt < 2) {
                    advancePitCloseRetry(queue);
                } else {
                    queue.runAllTasksInTimeOrder();
                }
            }
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
            log.assertAllExpectationsMatched();
        }
    }

    /** Timer admission and delayed executor rejection must both terminate the retry's async ownership. */
    public void testRejectedPitCloseRetryReleasesOwnership() {
        for (boolean delayed : List.of(false, true)) {
            var queue = new DeterministicTaskQueue() {
                @Override
                public ThreadPool getThreadPool() {
                    return new DeterministicThreadPool(Function.identity()) {
                        @Override
                        public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                            if (delay.millis() == 100) {
                                var rejection = new EsRejectedExecutionException("controlled PIT retry rejection");
                                if (delayed == false) {
                                    throw rejection;
                                }
                                // The queue controls only the executor's rejection callback; the real source still
                                // owns the PIT and its DriverContext until the scheduled callback is delivered.
                                return super.schedule(() -> {
                                    if (command instanceof AbstractRunnable retry) {
                                        retry.onRejection(rejection);
                                    } else {
                                        throw new AssertionError("PIT retries must handle delayed rejection");
                                    }
                                }, delay, executor);
                            }
                            return super.schedule(command, delay, executor);
                        }
                    };
                }
            };
            try (var harness = new Harness(false, queue); var log = pitCleanupLog(1)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                harness.finishDriver();
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onFailure(
                    new ConnectTransportException(null, "cleanup failed")
                );
                if (delayed) {
                    assertFalse(harness.done.isDone());
                }
                queue.runAllTasksInTimeOrder();
                harness.assertFinished();
                assertTrue(harness.client.pending.isEmpty());
                log.assertAllExpectationsMatched();
            }
        }
    }

    /** A close that never answers must not keep the completed driver's ownership indefinitely. */
    public void testHungPitCloseReleasesOwnershipAtDeadlineBound() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(false, queue)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            harness.finishDriver();
            var first = harness.client.take(TransportClosePointInTimeAction.TYPE);
            try {
                assertFalse(harness.done.isDone());
                queue.runAllTasksInTimeOrder();
                assertTrue("three unanswered close attempts must reach a terminal deadline", harness.done.isDone());
                assertEquals("only two retries may follow the first timed-out close", 2, harness.client.pending.size());
                harness.assertFinished();
                first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                first.listener.onFailure(new IllegalStateException("late failure containing snapshot"));
                drainPitCloseCallbacks(harness, queue);
                harness.assertFinished();
            } finally {
                first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                drainPitCloseCallbacks(harness, queue);
            }
        }
    }

    /** Rejection after a deadline timer is admitted must still complete the attempt's ownership. */
    public void testDelayedPitCloseDeadlineRejectionReleasesOwnership() throws Exception {
        try (var deadlinePool = new TestThreadPool("sequence-pit-deadline-rejection")) {
            var captured = new AtomicReference<Runnable>();
            var admitted = new CountDownLatch(1);
            var deadlines = new AtomicInteger();
            // GENERIC force-queues after shutdown, so use this owned pool's fixed WRITE executor to exercise
            // actual rejection. The real timeout scheduler still supplies the context-preserving task.
            var rejectionExecutor = deadlinePool.executor(ThreadPool.Names.WRITE);
            rejectionExecutor.shutdown();
            var queue = new DeterministicTaskQueue() {
                @Override
                public ThreadPool getThreadPool() {
                    return new DeterministicThreadPool(Function.identity()) {
                        @Override
                        public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                            if (delay.millis() == 5_000) {
                                deadlines.incrementAndGet();
                                return deadlinePool.schedule(command, TimeValue.timeValueMillis(1), scheduled -> {
                                    captured.set(scheduled);
                                    admitted.countDown();
                                });
                            }
                            return super.schedule(command, delay, executor);
                        }
                    };
                }
            };
            try (var harness = new Harness(false, queue)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                harness.finishDriver();
                var first = harness.client.take(TransportClosePointInTimeAction.TYPE);
                try {
                    assertTrue("the actual timeout scheduler must reach its target executor", admitted.await(10, TimeUnit.SECONDS));
                    assertEquals("every sent close requires one admitted deadline", 1, deadlines.get());
                    assertTrue("context preservation must retain rejection handling", captured.get() instanceof AbstractRunnable);
                    assertFalse(harness.done.isDone());
                    rejectionExecutor.execute(captured.getAndSet(null));
                    harness.assertFinished();
                    assertTrue("a rejected deadline must not start another attempt", harness.client.pending.isEmpty());
                    first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                    harness.assertFinished();
                } finally {
                    first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                    drainPitCloseCallbacks(harness, queue);
                }
            }
        }
    }

    /** Retrying the same denied request and credentials cannot make authorization succeed. */
    public void testWrappedPermanentPitCloseFailureStopsWithoutRetry() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(false, queue)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            harness.finishDriver();
            var first = harness.client.take(TransportClosePointInTimeAction.TYPE);
            try {
                first.listener.onFailure(
                    new RemoteTransportException(
                        "wrapper containing snapshot",
                        new ElasticsearchSecurityException("authorization denied for snapshot", RestStatus.FORBIDDEN)
                    )
                );
                assertTrue("a permanent close failure must terminate without retrying", harness.done.isDone());
                harness.assertFinished();
                queue.runAllTasksInTimeOrder();
                assertTrue(harness.client.pending.isEmpty());
            } finally {
                first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                drainPitCloseCallbacks(harness, queue);
            }
        }
    }

    /** No request may be dispatched if its deadline cannot be admitted to the scheduler. */
    public void testRejectedPitCloseDeadlineAdmissionDoesNotDispatchRequest() {
        var queue = new DeterministicTaskQueue() {
            @Override
            public ThreadPool getThreadPool() {
                return new DeterministicThreadPool(Function.identity()) {
                    @Override
                    public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
                        if (delay.millis() == 5_000) {
                            throw new EsRejectedExecutionException("deadline admission failed for snapshot");
                        }
                        return super.schedule(command, delay, executor);
                    }
                };
            }
        };
        try (var harness = new Harness(false, queue); var log = pitCleanupLog(1)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            harness.finishDriver();
            harness.assertFinished();
            assertTrue("a request without an admitted deadline must not be sent", harness.client.pending.isEmpty());
            queue.runAllTasksInTimeOrder();
            harness.assertFinished();
            log.assertAllExpectationsMatched();
        }
    }

    /** A timed-out attempt's late callback cannot cancel or complete the next attempt's independent deadline. */
    public void testLatePitCloseCallbackCannotCompleteNewAttempt() {
        for (boolean lateSuccess : List.of(false, true)) {
            var queue = new DeterministicTaskQueue();
            try (var harness = new Harness(false, queue)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                harness.finishDriver();
                var first = harness.client.take(TransportClosePointInTimeAction.TYPE);
                try {
                    queue.advanceTime();
                    queue.runAllRunnableTasks();
                    advancePitCloseRetry(queue);
                    var retry = harness.client.take(TransportClosePointInTimeAction.TYPE);
                    if (lateSuccess) {
                        first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                    } else {
                        first.listener.onFailure(new IllegalArgumentException("late permanent failure for snapshot"));
                    }
                    assertFalse("a previous attempt must not release the current attempt", harness.done.isDone());
                    retry.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                    harness.assertFinished();
                    queue.runAllTasksInTimeOrder();
                    assertTrue("successful close cancels every remaining attempt deadline", harness.client.pending.isEmpty());
                } finally {
                    first.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                    drainPitCloseCallbacks(harness, queue);
                }
            }
        }
    }

    /** Only recognized transient classes justify retrying the same PIT close with the same credentials. */
    public void testKnownTransientPitCloseFailuresRetry() {
        List<Exception> failures = List.of(
            new ConnectTransportException(null, "transient connection failure for snapshot"),
            new ElasticsearchTimeoutException("transient timeout for snapshot"),
            new EsRejectedExecutionException("temporary rejection for snapshot"),
            new ReceiveTimeoutTransportException(DiscoveryNodeUtils.create("node"), "close_pit", "receive timeout for snapshot"),
            new RemoteTransportException("wrapper containing snapshot", new ConnectTransportException(null, "snapshot connection failed"))
        );
        for (Exception failure : failures) {
            var queue = new DeterministicTaskQueue();
            try (var harness = new Harness(false, queue)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                harness.finishDriver();
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onFailure(failure);
                assertFalse(harness.done.isDone());
                advancePitCloseRetry(queue);
                var retry = harness.client.take(TransportClosePointInTimeAction.TYPE);
                assertEquals(new BytesArray("snapshot"), ((ClosePointInTimeRequest) retry.request).getId());
                retry.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
                queue.runAllTasksInTimeOrder();
                assertTrue(harness.client.pending.isEmpty());
            }
        }
    }

    /** Deep wrappers and token-bearing messages must not escape through failure classification or diagnostics. */
    @TestLogging(
        value = "org.elasticsearch.xpack.esql.execution.sequence.SequenceSourceOperator:DEBUG",
        reason = "verify both retry and terminal logs report only sanitized causes"
    )
    public void testPitCloseDiagnosticsNeverIncludeTokenOrExceptionText() {
        Exception authorization = new ElasticsearchSecurityException("denied for snapshot", RestStatus.FORBIDDEN);
        for (int depth = 0; depth < 12; depth++) {
            authorization = new RemoteTransportException("wrapper containing snapshot", authorization);
        }
        var validation = new ActionRequestValidationException();
        validation.addValidationError("invalid PIT snapshot");
        /** Explicit expectations do not derive their classification from the implementation under test. */
        record FailureCase(Exception failure, String reason, String type) {}
        List<FailureCase> failures = List.of(
            new FailureCase(authorization, "authorization_failure", "ElasticsearchSecurityException"),
            new FailureCase(validation, "invalid_request", "ActionRequestValidationException"),
            new FailureCase(new IllegalArgumentException("malformed snapshot"), "invalid_request", "IllegalArgumentException"),
            new FailureCase(new IllegalStateException("invalid state for snapshot"), "invalid_state", "IllegalStateException"),
            new FailureCase(new TaskCancelledException("cancelled snapshot"), "cancelled", "TaskCancelledException"),
            new FailureCase(
                new EsRejectedExecutionException("shutdown for snapshot", true),
                "executor_shutdown",
                "EsRejectedExecutionException"
            ),
            new FailureCase(new Exception("unclassified failure for snapshot"), "unclassified_failure", "Exception")
        );
        for (FailureCase failure : failures) {
            var queue = new DeterministicTaskQueue();
            try (var harness = new Harness(false, queue); var log = MockLog.capture(SequenceSourceOperator.class, ExceptionsHelper.class)) {
                log.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "safe classified cause",
                        SequenceSourceOperator.class.getCanonicalName(),
                        Level.DEBUG,
                        "*reason [" + failure.reason() + "], exception type [" + failure.type() + "], retryable [false]"
                    )
                );
                log.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "safe terminal cause",
                        SequenceSourceOperator.class.getCanonicalName(),
                        Level.WARN,
                        "*reason [" + failure.reason() + "], exception type [" + failure.type() + "]"
                    )
                );
                for (Level level : List.of(Level.DEBUG, Level.WARN)) {
                    log.addExpectation(
                        new MockLog.UnseenEventExpectation(
                            "PIT token must not appear in " + level,
                            SequenceSourceOperator.class.getCanonicalName(),
                            level,
                            "*snapshot*"
                        )
                    );
                    for (Class<?> loggerClass : List.of(SequenceSourceOperator.class, ExceptionsHelper.class)) {
                        log.addExpectation(
                            new MockLog.UnseenEventExpectation(
                                "raw throwable must not appear in cleanup diagnostics",
                                loggerClass.getCanonicalName(),
                                level,
                                "*"
                            ) {
                                @Override
                                public boolean innerMatch(LogEvent event) {
                                    return event.getThrown() != null;
                                }
                            }
                        );
                    }
                }
                log.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "cause unwrapping must not log a deep raw exception",
                        ExceptionsHelper.class.getCanonicalName(),
                        Level.WARN,
                        "*snapshot*"
                    )
                );
                harness.prepareOpen().listener.onResponse(openResponse());
                harness.finishDriver();
                var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
                try {
                    close.listener.onFailure(failure.failure());
                    harness.assertFinished();
                    queue.runAllTasksInTimeOrder();
                    assertTrue(harness.client.pending.isEmpty());
                    log.assertAllExpectationsMatched();
                } finally {
                    close.listener.onResponse(new ClosePointInTimeResponse(true, 2));
                    drainPitCloseCallbacks(harness, queue);
                }
            }
        }
    }

    /** Advance one queued retry without also firing the next unanswered request's deadline. */
    private static void advancePitCloseRetry(DeterministicTaskQueue queue) {
        queue.runAllRunnableTasks();
        assertTrue("the next cleanup retry must be scheduled", queue.hasDeferredTasks());
        queue.advanceTime();
        queue.runAllRunnableTasks();
    }

    /** Complete owned test callbacks even when a pre-fix assertion fails, without leaving a PIT to expire. */
    private static void drainPitCloseCallbacks(Harness harness, DeterministicTaskQueue queue) {
        queue.runAllTasksInTimeOrder();
        while (harness.client.pending.isEmpty() == false) {
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
        }
        queue.runAllTasksInTimeOrder();
    }

    private static MockLog pitCleanupLog(int attempts) {
        MockLog log = MockLog.capture(SequenceSourceOperator.class);
        log.addExpectation(
            new MockLog.SeenEventExpectation(
                "bounded PIT cleanup warning",
                SequenceSourceOperator.class.getCanonicalName(),
                Level.WARN,
                "Could not confirm complete PIT cleanup after [" + attempts + "] attempts for SEQUENCE query*"
            )
        );
        log.addExpectation(
            new MockLog.UnseenEventExpectation(
                "PIT tokens must not appear in cleanup logs",
                SequenceSourceOperator.class.getCanonicalName(),
                Level.WARN,
                "*snapshot*"
            )
        );
        return log;
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
            // Only stage 0 has an output-only computation, so stage 1 needs no payload replay.
            try (Page page = harness.source.getOutput()) {
                assertNotNull(page);
                assertEquals(1, page.getPositionCount());
                assertEquals(2, page.getBlockCount());
                BytesRefBlock key = page.getBlock(0), value = page.getBlock(1);
                assertEquals("shared", key.getBytesRef(key.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                assertEquals("shared/replay", value.getBytesRef(value.getFirstValueIndex(0), new BytesRef()).utf8ToString());
            }
            assertEquals(3, harness.stageExecutions);
            assertEquals(1L, harness.source.status().payloadSearchRequests());
            assertEquals(1L, harness.source.status().rowsEmitted());
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** Only a second-page start matches; its selection plan is reused without reusing either page's values. */
    public void testOnlyMatchOnSecondPageCompilesEachUsedTemplateOnce() {
        try (var harness = new Harness(true)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            int firstStageSearches = 0;
            boolean emitted = false;
            for (int attempt = 0; attempt < 100 && emitted == false; attempt++) {
                try (Page page = harness.source.getOutput()) {
                    if (page != null) {
                        assertEquals(1, page.getPositionCount());
                        BytesRefBlock block = page.getBlock(0);
                        assertEquals("shared", block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                        emitted = true;
                    }
                }
                if (harness.client.pending.isEmpty() == false) {
                    var pending = harness.client.take(TransportSearchAction.TYPE);
                    var request = (SearchRequest) pending.request;
                    String index = request.getPointInTimeAuthorizationScope().indices()[0];
                    int stage = index.equals("a") ? 0 : 1;
                    SearchResponse response;
                    if (stage == 0 && firstStageSearches++ == 0) {
                        response = eventPage(0, 0, 256, "unmatched");
                    } else {
                        response = eventPage(stage, stage == 0 ? 256 : 0, 1, "shared");
                    }
                    harness.respond(pending, stage, response);
                }
            }
            assertTrue("a start present only on the second candidate page must complete", emitted);
            assertEquals(3, firstStageSearches);
            assertEquals(4, harness.stageExecutions);
            assertEquals("selection and payload use separate lazy templates", 3, harness.stagePlans);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** A failing later member identifies the explicit BY name and still releases its PIT, blocks and breaker reservations. */
    public void testMultivaluedKeyNamesMemberAndKey() {
        Sequence sequence = plan("""
            SEQUENCE BY account WITH {"maxspan":"1m"} PRIMARY 1
            (FROM a | RENAME key AS account), (FROM b | RENAME key AS account) | KEEP account | LIMIT 10
            """);
        try (var harness = new Harness(true, null, null, sequence)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertNull(harness.source.getOutput());
            assertNull(harness.source.getOutput());
            SearchResponse response = eventResponse(1);
            response.getHits().getHits()[0].setDocumentField(new DocumentField("key", List.of(new BytesRef("a"), new BytesRef("b"))));
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 1, response);
            // Search delivery starts the stage; its queued completion publishes the encoded key on the next pull.
            assertNull(harness.source.getOutput());
            String message = expectThrows(IllegalArgumentException.class, harness.source::getOutput).getMessage();
            assertThat(message, containsString("SEQUENCE member [2] BY column [account] is multivalued"));
            assertThat(message, containsString("use MV_EXPAND inside the stage"));
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
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

    public void testPlanningFailureReleasesBoundInput() {
        try (var harness = new Harness(true)) {
            harness.failPlan = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertThat(
                expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage(),
                containsString("controlled stage planning failure")
            );
            assertEquals(1, harness.stagePlans);
            assertEquals(0, harness.stageExecutions);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testDriverCreationFailureReleasesBoundInput() {
        try (var harness = new Harness(true)) {
            harness.failDriverCreation = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertThat(
                expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage(),
                containsString("controlled stage driver creation failure")
            );
            assertEquals(1, harness.stagePlans);
            assertEquals(1, harness.driverCreations);
            assertEquals(0, harness.stageExecutions);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    public void testEmptyFirstStageDoesNotCompileUnvisitedStages() {
        try (var harness = new Harness(true)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            var response = SearchResponse.emptyResponseBuilder().build();
            try (Releasable ignored = response::decRef) {
                search.listener.onResponse(response);
            }
            assertNull(harness.source.getOutput());
            assertEquals(0, harness.stagePlans);
            assertEquals(0, harness.stageExecutions);
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

    /** Physical fetch order differs from match order in the second batch, including each shard's first document. */
    public void testPhysicalPayloadOrderPreservesSelectedCellsAcrossBatches() {
        Sequence sequence = plan("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from a metadata _id, _source | keep key, _id, _source, payload),
            (from b metadata _id, _source | keep key, _id, _source, payload)
            | eval other_id=MEMBER(2, _id), other_payload=MEMBER(2, payload)
            | keep key, _id, _source, payload, other_id, other_payload | limit 3
            """);
        // This unit exercises the core source before consumer projection pruning, retaining both stage source slots.
        try (var harness = new Harness(true, null, null, sequence)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            List<Map<String, String>> actual = new ArrayList<>();
            int[] payloadCalls = new int[2];
            for (int attempt = 0; attempt < 100 && actual.size() < 3; attempt++) {
                try (Page page = harness.source.getOutput()) {
                    if (page != null) {
                        for (int row = 0; row < page.getPositionCount(); row++) {
                            Map<String, String> values = new LinkedHashMap<>();
                            for (int column = 0; column < page.getBlockCount(); column++) {
                                BytesRefBlock block = page.getBlock(column);
                                values.put(
                                    sequence.sourceStages().get(column) + ":" + sequence.sourceAttributes().get(column).name(),
                                    block.getBytesRef(block.getFirstValueIndex(row), new BytesRef()).utf8ToString()
                                );
                            }
                            actual.add(values);
                        }
                    }
                }
                if (harness.client.pending.isEmpty() == false) {
                    var pending = harness.client.take(TransportSearchAction.TYPE);
                    var request = (SearchRequest) pending.request;
                    int stage = request.getPointInTimeAuthorizationScope().indices()[0].equals("a") ? 0 : 1;
                    boolean identity = request.source().sorts().size() == 1;
                    List<Integer> events;
                    if (identity) {
                        int batch = payloadCalls[stage]++;
                        assertTrue("only two bounded payload batches are needed", batch < 2);
                        events = batch == 0 ? List.of(0) : List.of(2, 1);
                        assertEquals(ShardDocSortField.NAME, ((FieldSortBuilder) request.source().sorts().getFirst()).getFieldName());
                        assertEquals(SortOrder.ASC, ((FieldSortBuilder) request.source().sorts().getFirst()).order());
                        assertArrayEquals(
                            new Object[] {
                                SequenceSourceOperator.precedingShardDoc(ShardDocSortField.encodeShardAndDoc(stage, batch == 0 ? 2 : 0)) },
                            request.source().searchAfter()
                        );
                        assertEquals(Integer.valueOf(Integer.MAX_VALUE), request.getPreFilterShardSize());
                        assertFalse(request.allowPartialSearchResults());
                        assertNotNull(request.source().pointInTimeBuilder());
                        assertTrue(request.source().fetchSource().fetchSource());
                        var projection = (SequenceDocValuesSearchExt) request.source().ext().getFirst();
                        assertEquals(List.of("payload"), projection.fields().stream().map(field -> field.field).toList());
                    } else {
                        assertFalse(request.source().fetchSource().fetchSource());
                        var projection = (SequenceDocValuesSearchExt) request.source().ext().getFirst();
                        assertEquals(List.of("@timestamp", "key"), projection.fields().stream().map(field -> field.field).toList());
                        events = List.of(0, 1, 2);
                    }
                    harness.respond(pending, stage, orderedEvents(stage, events, identity));
                }
            }
            List<Map<String, String>> expected = new ArrayList<>();
            for (int event = 0; event < 3; event++) {
                expected.add(
                    Map.of(
                        "0:key",
                        "key" + event,
                        "0:_id",
                        "event-a-" + event,
                        "0:_source",
                        eventSource("a", event),
                        "0:payload",
                        "payload-a-" + event,
                        "1:_id",
                        "event-b-" + event,
                        "1:_source",
                        eventSource("b", event),
                        "1:payload",
                        "payload-b-" + event
                    )
                );
            }
            assertEquals(expected, actual);
            assertArrayEquals(new int[] { 2, 2 }, payloadCalls);
            assertEquals(4L, harness.source.status().payloadSearchRequests());
            assertEquals("late raw fields and original source do not need stage replay", 2, harness.stageExecutions);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
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

    public void testMissingPhysicalPayloadUsesControlledReconstructionError() {
        try (var harness = new Harness(true)) {
            var search = harness.preparePayloadSearch();
            harness.respond(search, 0, SearchResponse.emptyResponseBuilder().build());
            assertEquals(
                "SEQUENCE could not reconstruct the selected event from its point-in-time snapshot",
                expectThrows(IllegalArgumentException.class, harness.source::getOutput).getMessage()
            );
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** A hit at the selected physical coordinate must retain the selected index/ID identity. */
    public void testConflictingPhysicalPayloadIdentityRemainsAnInvariantFailure() {
        try (var harness = new Harness(true)) {
            var search = harness.preparePayloadSearch();
            // The ordinary fixture selected event-a; this hit has a different ID at the same shard/document coordinate.
            harness.respond(search, 0, eventPage(0, 0, 1, "shared"));
            assertEquals(
                "SEQUENCE selected PIT ordinal has conflicting source identity",
                expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage()
            );
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testRetainedSelectionAndOriginalIdsNeedNoPayloadSearch() {
        Sequence sequence = plan("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from a metadata _id, _index | keep key, _id, _index),
            (from b metadata _id, _index | keep key, _id, _index)
            | eval other_id=MEMBER(2, _id) | keep key, _id, _index, other_id | limit 1
            """);
        try (var harness = new Harness(true, null, null, sequence)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            boolean emitted = false;
            int searches = 0;
            for (int attempt = 0; attempt < 20 && emitted == false; attempt++) {
                try (Page page = harness.source.getOutput()) {
                    if (page != null) {
                        assertEquals(1, page.getPositionCount());
                        List<String> actual = new ArrayList<>();
                        for (int column = 0; column < page.getBlockCount(); column++) {
                            BytesRefBlock block = page.getBlock(column);
                            actual.add(block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                        }
                        assertEquals(List.of("shared", "event-a", "a", "event-b", "b"), actual);
                        emitted = true;
                    }
                }
                if (harness.client.pending.isEmpty() == false) {
                    assertTrue("only the two candidate searches are needed", searches < 2);
                    harness.respond(harness.client.take(TransportSearchAction.TYPE), searches++);
                }
            }
            assertTrue(emitted);
            assertEquals(2, searches);
            assertEquals(2, harness.stageExecutions);
            assertEquals(0L, harness.source.status().payloadSearchRequests());
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** Optimize the complete statement so MV_EXPAND's copy alias and the outer MEMBER demand both participate. */
    public void testOptimizedExpandedAliasPreservesOriginalMemberPayload() {
        for (boolean replay : List.of(false, true)) {
            assertOptimizedStagePayload(
                "EVAL expanded = payload | MV_EXPAND expanded | KEEP key, payload",
                "payload",
                List.of("left", "right"),
                List.of("left", "right"),
                replay
            );
        }
    }

    /** Handwritten ordinary projection/extraction regressions through the real optimizer and both materialization paths. */
    public void testOptimizedStageShapesPreserveDemandedPayload() {
        for (String[] shape : List.of(
            new String[] {
                "EVAL raw_arg = payload | MV_EXPAND raw_arg | EVAL normalized = TO_LOWER(raw_arg)"
                    + " | WHERE normalized == \"left\" | KEEP key, payload",
                "payload",
                "LEFT" },
            new String[] { "RENAME payload AS kept | KEEP key, kept", "kept", "LEFT" },
            new String[] { "EVAL payload = TO_LOWER(payload) | KEEP key, payload", "payload", "left" },
            new String[] {
                "EVAL copied = payload | DISSECT copied \"%{parsed}\" | WHERE parsed == \"LEFT\" | KEEP key, payload",
                "payload",
                "LEFT" },
            new String[] {
                "EVAL copied = payload | GROK copied \"%{WORD:parsed}\" | WHERE parsed == \"LEFT\" | KEEP key, payload",
                "payload",
                "LEFT" }
        )) {
            for (boolean replay : List.of(false, true)) {
                assertOptimizedStagePayload(shape[0], shape[1], List.of("LEFT"), List.of(shape[2]), replay);
            }
        }
    }

    /** Vary copy depth and projection placement while preserving the original multivalued output demand. */
    public void testGeneratedExpandedCopiesPreserveOriginalPayload() {
        for (int copies = 1; copies <= 2; copies++) {
            for (boolean projectBeforeExpansion : List.of(false, true)) {
                StringBuilder commands = new StringBuilder();
                String previous = "payload";
                for (int copy = 0; copy < copies; copy++) {
                    String name = "expanded_" + copy;
                    if (commands.isEmpty() == false) {
                        commands.append(" | ");
                    }
                    commands.append("EVAL ").append(name).append(" = ").append(previous);
                    previous = name;
                }
                if (projectBeforeExpansion) {
                    commands.append(" | KEEP payload, key, ").append(previous);
                }
                commands.append(" | MV_EXPAND ")
                    .append(previous)
                    .append(" | WHERE TO_LOWER(")
                    .append(previous)
                    .append(") == \"left\" | KEEP key, payload");
                for (boolean replay : List.of(false, true)) {
                    assertOptimizedStagePayload(commands.toString(), "payload", List.of("LEFT", "right"), List.of("LEFT", "right"), replay);
                }
            }
        }
    }

    /** Extraction remains in the optimized stage while the copy/expansion still rewrites the original's wrapper. */
    public void testExpandedDissectCopiesPreserveOriginalPayload() {
        assertExpandedExtractorPreservesPayload("DISSECT", "%{parsed}");
    }

    /** GROK exercises a distinct real extraction operator after expansion, rather than an alias-only variant. */
    public void testExpandedGrokCopiesPreserveOriginalPayload() {
        assertExpandedExtractorPreservesPayload("GROK", "%{WORD:parsed}");
    }

    private void assertExpandedExtractorPreservesPayload(String extractor, String pattern) {
        for (boolean projectBeforeExpansion : List.of(false, true)) {
            String commands = "EVAL expanded = payload"
                + (projectBeforeExpansion ? " | KEEP payload, key, expanded" : "")
                + " | MV_EXPAND expanded | "
                + extractor
                + " expanded \""
                + pattern
                + "\""
                + " | WHERE parsed == \"LEFT\" | KEEP key, payload";
            for (boolean replay : List.of(false, true)) {
                assertOptimizedStagePayload(commands, "payload", List.of("LEFT", "right"), List.of("LEFT", "right"), replay);
            }
        }
    }

    /** Selection exports only the key; replay exports payload first, so their cached key channels must differ. */
    public void testFullReplayUsesItsOwnChannelLayout() {
        for (boolean smallRetainedCapacity : List.of(false, true)) {
            assertOptimizedStagePayload(
                "EVAL payload = TO_LOWER(payload) | KEEP payload, key",
                "payload",
                List.of("LEFT"),
                List.of("left"),
                smallRetainedCapacity
            );
        }
    }

    /** Source-operator inputs report their owning stage position without transporting diagnostic-only plan state. */
    public void testRetentionFailureNamesOwningStageNumber() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key),
            (FROM b | EVAL lower = TO_LOWER(payload) | WHERE lower != "skip" | KEEP key, lower)
            | EVAL original = MEMBER(2, lower) | KEEP key, original | LIMIT 1
            """);
        var stage = (SequenceStage) sequence.children().get(1);
        Attribute current = stage.output().stream().filter(attribute -> attribute.name().equals("lower")).findFirst().orElseThrow();
        Attribute wrongType = new ReferenceAttribute(
            current.source(),
            null,
            current.name(),
            DataType.INTEGER,
            current.nullable(),
            current.id(),
            current.synthetic()
        );
        String message = expectThrows(
            IllegalStateException.class,
            () -> SequenceStageInput.selection(stage, List.of(wrongType), unused -> {}, 2).retention(0)
        ).getMessage();
        assertThat(message, containsString("SEQUENCE stage [2] branch [1] output changed type of column [lower#"));
    }

    /** Correlation keys need the same identity binding as carried values after the alias rewrite. */
    public void testOptimizedExpandedAliasPreservesOriginalKey() {
        LogicalPlan optimized = optimizedStatement("""
            SEQUENCE BY key, payload WITHIN 30 seconds PRIMARY origin
            (FROM a | KEEP key, payload) AS origin,
            (FROM b | EVAL expanded = payload | MV_EXPAND expanded | KEEP key, payload) AS execution
            | EVAL execution_payload = MEMBER(execution, payload)
            | KEEP key, payload, execution_payload | LIMIT 1
            """);
        assertEquals(List.of("key", "payload", "execution_payload"), optimized.output().stream().map(Attribute::name).toList());
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        // Equal correlation keys need only one source slot; the MEMBER consumer aliases that same selected value.
        List<Alias> memberAliases = new ArrayList<>();
        optimized.forEachExpressionDown(Alias.class, alias -> {
            if (alias.name().equals("execution_payload")) {
                memberAliases.add(alias);
            }
        });
        assertEquals(1, memberAliases.size());
        assertTrue(memberAliases.getFirst().child() instanceof Attribute);
        assertEquals(sequence.output().get(1).id(), ((Attribute) memberAliases.getFirst().child()).id());
        for (boolean matching : List.of(false, true)) {
            try (var harness = new Harness(true, null, null, sequence)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                int rows = 0;
                for (int attempt = 0; attempt < 30 && harness.source.isFinished() == false; attempt++) {
                    try (Page page = harness.source.getOutput()) {
                        if (page != null) {
                            rows += page.getPositionCount();
                            assertEquals(1, page.getPositionCount());
                            assertEquals(2, page.getBlockCount());
                            List<String> actual = new ArrayList<>();
                            for (int column = 0; column < page.getBlockCount(); column++) {
                                BytesRefBlock block = page.getBlock(column);
                                assertEquals(1, block.getValueCount(0));
                                actual.add(block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                            }
                            assertEquals(List.of("shared", "LEFT"), actual);
                        }
                    }
                    if (harness.client.pending.isEmpty() == false
                        && harness.client.pending.getFirst().action.equals(TransportSearchAction.TYPE)) {
                        Pending<SearchResponse> search = harness.client.take(TransportSearchAction.TYPE);
                        boolean origin = ((SearchRequest) search.request).getPointInTimeAuthorizationScope().indices()[0].equals("a");
                        respondWithPayload(harness, search, List.of(origin || matching ? "LEFT" : "RIGHT"));
                    }
                }
                assertTrue("the finite key fixture must finish", harness.source.isFinished());
                assertEquals(matching ? 1 : 0, rows);
                harness.finishDriver();
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
            }
        }
    }

    /** A page binding must have one value with the expected identity/type, independent of attribute wrapper class. */
    public void testOutputChannelsRequireUniqueTypedIdentity() {
        Attribute expected = ((SequenceStage) plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1 (FROM a | KEEP key), (FROM b | KEEP key) | LIMIT 1
            """).children().getFirst()).keys().getFirst();
        Attribute carried = new ReferenceAttribute(
            Source.EMPTY,
            null,
            expected.name(),
            expected.dataType(),
            Nullability.TRUE,
            expected.id(),
            false
        );
        assertNotEquals(expected, carried);
        assertEquals(0, SequenceStageInput.outputChannel(List.of(carried), expected, "stage [2]"));
        assertThat(
            expectThrows(IllegalStateException.class, () -> SequenceStageInput.outputChannel(List.of(), expected, "stage [2]"))
                .getMessage(),
            containsString("stage [2] output lost column [key#")
        );
        assertThat(
            expectThrows(
                IllegalStateException.class,
                () -> SequenceStageInput.outputChannel(List.of(expected, carried), expected, "stage [2]")
            ).getMessage(),
            containsString("stage [2] output has duplicate column identity [key#")
        );
        Attribute wrongType = new ReferenceAttribute(
            Source.EMPTY,
            null,
            expected.name(),
            DataType.INTEGER,
            Nullability.TRUE,
            expected.id(),
            false
        );
        assertThat(
            expectThrows(IllegalStateException.class, () -> SequenceStageInput.outputChannel(List.of(wrongType), expected, "stage [2]"))
                .getMessage(),
            containsString("stage [2] output changed type of column [key#")
        );
        Attribute sameName = new ReferenceAttribute(Source.EMPTY, null, "key", expected.dataType());
        assertThat(
            expectThrows(IllegalStateException.class, () -> SequenceStageInput.outputChannel(List.of(sameName), expected, "stage [2]"))
                .getMessage(),
            containsString("stage [2] output lost column [key#")
        );
    }

    private void assertOptimizedStagePayload(
        String commands,
        String member,
        List<String> inputValues,
        List<String> expectedValues,
        boolean replay
    ) {
        Sequence sequence = optimizedPlan(Strings.format("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY origin
            (FROM a | KEEP key) AS origin,
            (FROM b | %s) AS execution
            | EVAL original = MEMBER(execution, %s)
            | KEEP key, original | LIMIT 1
            """, commands, member));
        try (var harness = new Harness(true, null, null, sequence, replay ? 1 : null)) {
            harness.prepareOpen().listener.onResponse(openResponse());
            boolean emitted = false;
            for (int attempt = 0; attempt < 30 && emitted == false; attempt++) {
                try (Page page = harness.source.getOutput()) {
                    if (page != null) {
                        assertEquals(1, page.getPositionCount());
                        assertEquals(2, page.getBlockCount());
                        BytesRefBlock key = page.getBlock(0);
                        assertEquals("shared", key.getBytesRef(key.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                        BytesRefBlock original = page.getBlock(1);
                        assertEquals(expectedValues.size(), original.getValueCount(0));
                        int first = original.getFirstValueIndex(0);
                        List<String> actual = new ArrayList<>();
                        for (int value = 0; value < original.getValueCount(0); value++) {
                            actual.add(original.getBytesRef(first + value, new BytesRef()).utf8ToString());
                        }
                        assertEquals(expectedValues, actual);
                        emitted = true;
                    }
                }
                if (harness.client.pending.isEmpty() == false) {
                    respondWithPayload(harness, harness.client.take(TransportSearchAction.TYPE), inputValues);
                }
            }
            assertTrue("the optimized stage must preserve its original multivalued payload", emitted);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    private static void respondWithPayload(Harness harness, Pending<SearchResponse> pending, List<String> values) {
        int stage = ((SearchRequest) pending.request).getPointInTimeAuthorizationScope().indices()[0].equals("a") ? 0 : 1;
        SearchResponse response = eventPage(stage, 0, 1, "shared");
        SearchHit hit = response.getHits().getHits()[0];
        List<Object> payload = new ArrayList<>(values.size());
        for (String value : values) {
            payload.add(new BytesRef(value));
        }
        hit.setDocumentField(new DocumentField("payload", payload));
        harness.respond(pending, stage, response);
    }

    /** Stage metadata must reach the real matcher without changing retained source identities or cleanup. */
    public void testIncomingLinkLimitIsAppliedBySourceOperator() {
        Sequence base = plan("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from a metadata _id, _index | keep key, _id, _index),
            (from b metadata _id, _index | keep key, _id, _index)
            | eval other_id=MEMBER(2, _id) | keep key, _id, _index, other_id | limit 1
            """);
        for (long limit : new long[] { 0, 999_999_999, 1_000_000_000 }) {
            var stages = new ArrayList<>(base.children());
            var second = (SequenceStage) stages.get(1);
            stages.set(1, new SequenceStage(second.source(), second.child(), second.keys(), limit));
            Sequence sequence = base.replaceChildren(stages);
            try (var harness = new Harness(true, null, null, sequence)) {
                harness.prepareOpen().listener.onResponse(openResponse());
                int rows = 0;
                int searches = 0;
                for (int attempt = 0; attempt < 20 && harness.source.isFinished() == false; attempt++) {
                    try (Page page = harness.source.getOutput()) {
                        if (page != null) {
                            rows += page.getPositionCount();
                            assertEquals(1, page.getPositionCount());
                            List<String> actual = new ArrayList<>();
                            for (int column = 0; column < page.getBlockCount(); column++) {
                                BytesRefBlock block = page.getBlock(column);
                                actual.add(block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                            }
                            assertEquals(List.of("shared", "event-a", "a", "event-b", "b"), actual);
                        }
                    }
                    if (searches < 2 && harness.client.pending.isEmpty() == false) {
                        harness.respond(harness.client.take(TransportSearchAction.TYPE), searches++);
                    }
                }
                assertTrue("the bounded fixture must finish", harness.source.isFinished());
                assertEquals(limit == 999_999_999 ? 0 : 1, rows);
                assertEquals(2, searches);
                assertEquals(0L, harness.source.status().payloadSearchRequests());
                harness.finishDriver();
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
                assertTrue(harness.client.pending.isEmpty());
            }
        }
    }

    public void testCloseWhileDirectPayloadPendingReleasesRetainedCells() {
        Sequence sequence = plan("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from a metadata _source | keep key, _source, payload), (from b | keep key)
            | keep key, _source, payload | limit 1
            """);
        for (boolean failure : List.of(false, true)) {
            try (var harness = new Harness(true, null, null, sequence)) {
                var search = harness.preparePayloadSearch();
                harness.finishDriver();
                assertFalse(harness.done.isDone());
                assertTrue(harness.breaker.getUsed() > 0);
                if (failure) {
                    search.listener.onFailure(new IllegalStateException("direct payload cancelled"));
                } else {
                    harness.respond(search, 0);
                }
                assertEquals(2, harness.stageExecutions);
                assertEquals(0L, harness.source.status().rowsEmitted());
                assertEquals(0, harness.breaker.getUsed());
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
            }
        }
    }

    public void testMixedRetainedOverflowFallsBackAndDropsBackingBeforeDeferredReplayClose() {
        Sequence sequence = plan("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from a | eval selected=TO_UPPER(payload) | where selected!="skip" | keep key,selected),
            (from b | keep key)
            | keep key,selected | limit 3
            """);
        for (boolean failure : List.of(false, true)) {
            try (var harness = new Harness(true, null, null, sequence, 256)) {
                harness.deferStage = 3;
                harness.prepareOpen().listener.onResponse(openResponse());
                int emitted = 0;
                int payloadSearches = 0;
                for (int attempt = 0; attempt < 100 && harness.deferredStage == null; attempt++) {
                    try (Page page = harness.source.getOutput()) {
                        if (page != null) {
                            emitted += page.getPositionCount();
                        }
                    }
                    if (harness.client.pending.isEmpty() == false) {
                        var pending = harness.client.take(TransportSearchAction.TYPE);
                        var request = (SearchRequest) pending.request;
                        int stage = request.getPointInTimeAuthorizationScope().indices()[0].equals("a") ? 0 : 1;
                        boolean identity = request.source().sorts().size() == 1;
                        List<Integer> events;
                        if (identity) {
                            assertEquals(0, stage);
                            payloadSearches++;
                            events = List.of(2, 1);
                        } else {
                            events = List.of(0, 1, 2);
                        }
                        harness.respond(
                            pending,
                            stage,
                            orderedEvents(stage, events, identity, event -> event == 2 ? "x".repeat(8192) : "small-" + event)
                        );
                    }
                }
                assertNotNull(harness.deferredStage);
                assertEquals("the first small match materializes directly", 1, emitted);
                assertEquals("the mixed second batch uses one full-replay fetch", 1, payloadSearches);
                assertEquals(3, harness.stageExecutions);
                assertEquals(3L, harness.source.status().completedMatches());
                harness.finishDriver();
                assertFalse(harness.done.isDone());
                assertTrue("the deferred replay still owns its real input/output blocks", harness.breaker.getUsed() > 0);
                if (failure) {
                    harness.deferredStage.onFailure(new IllegalStateException("mixed fallback cancelled"));
                } else {
                    harness.deferredStage.onResponse(null);
                }
                assertEquals(0, harness.breaker.getUsed());
                harness.blocks.ensureAllBlocksAreReleased();
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
            }
        }
    }

    /** Genuine millis extrema must retain their rows and advance the document tie after a full raw page. */
    public void testRawMillisExtremaAdvanceAcrossPages() {
        for (long timestamp : List.of(Long.MIN_VALUE, Long.MAX_VALUE)) {
            assertRawTimestampPages(timestamp, timestamp, DataType.DATETIME);
        }
    }

    /** Adjacent extrema have nonnull ISO display strings that are still unsuitable as date search_after input. */
    public void testAdjacentMillisExtremaUseNumericContinuation() {
        for (long timestamp : List.of(Long.MIN_VALUE + 1, Long.MAX_VALUE - 1)) {
            assertRawTimestampPages(timestamp, timestamp, DataType.DATETIME);
        }
    }

    /** The final nanos value must follow its one-nanosecond predecessor without being interpreted as millis. */
    public void testRawNanosMaximumAdvanceAcrossPages() {
        assertRawTimestampPages(Long.MAX_VALUE - 1, Long.MAX_VALUE, DataType.DATE_NANOS);
    }

    private void assertRawTimestampPages(long startTimestamp, long endTimestamp, DataType timestampType) {
        Sequence sequence = plan("""
            SEQUENCE BY key WITH {"maxspan":"2ms"} PRIMARY 1
            (FROM a | KEEP key), (FROM b | KEEP key) | KEEP key | LIMIT 1000
            """, timestampType);
        try (var harness = new Harness(true, null, null, sequence)) {
            var open = harness.prepareOpen();
            open.listener.onResponse(openResponse());
            List<String> actual = new ArrayList<>();
            List<Object[]> continuations = new ArrayList<>();
            List<String> laterQueries = new ArrayList<>();
            Pending<SearchResponse> held = null;
            try {
                for (int attempt = 0; attempt < 2000 && actual.size() < 257; attempt++) {
                    try (Page page = harness.source.getOutput()) {
                        if (page != null) {
                            BytesRefBlock key = page.getBlock(0);
                            for (int row = 0; row < page.getPositionCount(); row++) {
                                actual.add(key.getBytesRef(key.getFirstValueIndex(row), new BytesRef()).utf8ToString());
                            }
                        }
                    }
                    if (harness.client.pending.isEmpty() == false) {
                        held = harness.client.take(TransportSearchAction.TYPE);
                        SearchRequest request = (SearchRequest) held.request;
                        assertArrayEquals(new String[] { "a", "b" }, request.getPointInTimeAuthorizationIndices().indices());
                        assertEquals(2, request.source().sorts().size());
                        int stage = request.getPointInTimeAuthorizationScope().indices()[0].equals("a") ? 0 : 1;
                        Object[] after = request.source().searchAfter();
                        if (after != null) {
                            continuations.add(new Object[] { stage, after[0], after[1] });
                        }
                        if (stage == 1) {
                            laterQueries.add(Strings.toString(request.source().query()));
                        }
                        int first = after == null ? 0 : ShardDocSortField.decodeDoc(((Number) after[1]).longValue()) + 1;
                        int count = Math.min(256, 257 - first);
                        assertTrue("continuation must advance without truncating or duplicating a page", first >= 0 && count >= 0);
                        SearchResponse response = boundaryPage(
                            stage,
                            first,
                            count,
                            stage == 0 ? startTimestamp : endTimestamp,
                            timestampType
                        );
                        try (Releasable ignored = response::decRef) {
                            Pending<SearchResponse> deliver = held;
                            held = null;
                            deliver.listener.onResponse(response);
                        }
                    }
                }
                java.util.Set<String> expected = new java.util.HashSet<>();
                for (int row = 0; row < 257; row++)
                    expected.add("boundary-" + (1000 + row));
                assertEquals(257, actual.size());
                assertEquals(expected, new java.util.HashSet<>(actual));
                assertEquals(2, continuations.size());
                for (Object[] after : continuations) {
                    long expectedTimestamp = ((Integer) after[0]) == 0 ? startTimestamp : endTimestamp;
                    assertEquals(Long.class, after[1].getClass());
                    assertEquals(expectedTimestamp, after[1]);
                    assertEquals(Long.class, after[2].getClass());
                }
                if (timestampType == DataType.DATETIME) {
                    for (String query : laterQueries) {
                        assertTrue(query, query.contains(SequenceRepeatabilityQueryBuilder.NAME));
                        assertFalse("out-of-format derived lower endpoints must stay open: " + query, query.contains("\"gte\""));
                    }
                }
            } finally {
                if (held != null) {
                    held.listener.onFailure(new IllegalStateException("closed after boundary request inspection"));
                }
                finishSnapshotRequests(harness, open, true);
            }
        }
    }

    private static SearchResponse boundaryPage(int stage, int first, int count, long timestamp, DataType timestampType) {
        SearchResponse response = eventPage(stage, first, count, "unused");
        boolean success = false;
        try {
            java.time.Instant instant = timestampType == DataType.DATE_NANOS
                ? java.time.Instant.ofEpochSecond(0, timestamp)
                : java.time.Instant.ofEpochMilli(timestamp);
            SearchHit[] hits = response.getHits().getHits();
            for (int row = 0; row < hits.length; row++) {
                int doc = first + row;
                hits[row].setDocumentField(new DocumentField("key", List.of(new BytesRef("boundary-" + (1000 + doc)))));
                hits[row].setDocumentField(new DocumentField("@timestamp", List.of(timestamp)));
                long shardDoc = ShardDocSortField.encodeShardAndDoc(stage, doc);
                Object displayed = timestamp == Long.MIN_VALUE || timestamp == Long.MAX_VALUE ? null : instant.toString();
                hits[row].sortValues(
                    SearchSortValues.fromFormattedAndRaw(new Object[] { displayed, shardDoc }, new Object[] { timestamp, shardDoc })
                );
            }
            success = true;
            return response;
        } finally {
            if (success == false) response.decRef();
        }
    }

    /** Raw EOF is insufficient while the final start page still owns unstarted computation. */
    public void testUpperTimeBoundWaitsForFinalStageZeroComputation() {
        assertUpperTimeBoundPublicationBarrier(true);
    }

    /** A terminal stage callback queues publication; a following driver pull must publish rows before deriving U. */
    public void testUpperTimeBoundWaitsForQueuedStageZeroPublication() {
        assertUpperTimeBoundPublicationBarrier(false);
    }

    private void assertUpperTimeBoundPublicationBarrier(boolean deferComputation) {
        assertUpperTimeBoundPublicationBarrier(deferComputation, false);
    }

    /** Latest-valid bounds wait for both real computation and publication of every final first-stage page. */
    public void testLatestValidUpperTimeBoundWaitsForStageZeroPublication() {
        assertUpperTimeBoundPublicationBarrier(true, true);
        assertUpperTimeBoundPublicationBarrier(false, true);
    }

    private void assertUpperTimeBoundPublicationBarrier(boolean deferComputation, boolean latestValid) {
        Sequence sequence = plan("""
            SEQUENCE BY key WITH {"maxspan":"10ms"} PRIMARY 1
            (FROM a | MV_EXPAND key | EVAL accepted=LENGTH(key)+LENGTH(payload) | WHERE accepted==7 | KEEP key),
            (FROM b | KEEP key) | KEEP key | LIMIT 10
            """);
        try (var harness = latestValid ? new Harness(sequence, null, latestConfig(sequence)) : new Harness(true, null, null, sequence)) {
            var open = harness.prepareOpen();
            open.listener.onResponse(openResponse());
            Pending<SearchResponse> later = null;
            try {
                harness.deferStage = 1;
                if (deferComputation) {
                    harness.deferStageStart = 1;
                }
                assertNull(harness.source.getOutput());
                harness.respond(harness.client.take(TransportSearchAction.TYPE), 0, upperBoundPage(0, 100, 200));
                assertNull(harness.source.getOutput());
                assertEquals(1, harness.stageExecutions);
                assertTrue("raw EOF must not dispatch a later reader before selection finishes", harness.client.pending.isEmpty());
                assertNull(harness.source.getOutput());
                assertTrue(harness.client.pending.isEmpty());
                if (deferComputation) {
                    assertNotNull(harness.deferredStart);
                    assertNull(harness.deferredStage);
                    Runnable start = harness.deferredStart;
                    harness.deferredStart = null;
                    start.run();
                }
                assertNotNull(harness.deferredStage);
                assertTrue("finished computation is not published selection", harness.client.pending.isEmpty());
                ActionListener<Void> terminal = harness.deferredStage;
                harness.deferredStage = null;
                terminal.onResponse(null);
                // Do not pull getOutput here: pending is now false but Completion has not published the surviving100ms row.
                assertTrue("the queued final callback must not dispatch from its listener", harness.client.pending.isEmpty());
                assertNull(harness.source.getOutput());
                later = harness.client.take(TransportSearchAction.TYPE);
                SearchRequest request = (SearchRequest) later.request;
                assertEquals(0L, harness.source.status().maxPendingCandidates());
                // The only survivor is100ms, although raw page progress reached200ms and expanded a discarded key.
                Pending<SearchResponse> deliver = later;
                later = null;
                harness.respond(deliver, 1, upperBoundPage(1, 105));
                assertNull(harness.source.getOutput());
                try (Page page = harness.source.getOutput()) {
                    assertNotNull(page);
                    assertEquals(1, page.getPositionCount());
                    BytesRefBlock key = page.getBlock(0);
                    assertEquals("shared", key.getBytesRef(key.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                }
                // Keep the actual result/ownership control before the expected BEFORE failure on missing U.
                assertTimestampUpperBound(request, java.time.Instant.parse("2026-09-25T21:14:51Z").plusMillis(110).toString());
            } finally {
                if (later != null) {
                    later.listener.onFailure(new IllegalStateException("closed after upper-bound request inspection"));
                }
                if (harness.deferredStart != null) {
                    Runnable start = harness.deferredStart;
                    harness.deferredStart = null;
                    start.run();
                }
                if (harness.deferredStage != null) {
                    ActionListener<Void> terminal = harness.deferredStage;
                    harness.deferredStage = null;
                    terminal.onResponse(null);
                }
                finishSnapshotRequests(harness, open, true);
            }
        }
    }

    private static void assertTimestampUpperBound(SearchRequest request, String expected) {
        Map<String, Object> query = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            Strings.toString(request.source().query()),
            false
        );
        assertTrue("missing inclusive timestamp upper bound [" + expected + "]: " + query, containsTimestampUpperBound(query, expected));
    }

    /** The demand carrier exports a canonical union while proving only optional late-output bindings. */
    public void testLatestValidMandatoryProjectionKeepsDirectAndReplayExportsSeparate() {
        for (boolean direct : List.of(false, true)) {
            Sequence sequence = optimizedPlan("""
                SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
                (FROM a | EVAL selected=TO_UPPER(payload), demanded=TO_LOWER(payload), late=CONCAT(key,"/late")
                  | WHERE selected!="SKIP" | KEEP selected,key,demanded,late),
                (FROM b | KEEP key) | KEEP selected,key,demanded,late | LIMIT 10
                """);
            var stage = (SequenceStage) sequence.children().getFirst();
            Attribute demanded = stageAttribute(sequence, 0, "demanded");
            Attribute optional = stageAttribute(sequence, 0, direct ? "selected" : "late");
            Attribute key = stageAttribute(sequence, 0, "key");
            List<Long> admissions = new ArrayList<>();
            var selection = SequenceStageInput.selection(
                stage,
                List.of(optional),
                List.of(new SequenceLatestValidConfig.ValueDemand(demanded), new SequenceLatestValidConfig.ValueDemand(key)),
                admissions::add,
                1
            );
            assertTrue(admissions.isEmpty());
            var retention = selection.retention(0);
            assertEquals(direct, retention.direct());
            assertEquals(1, admissions.size());
            assertEquals(direct ? 1 : 0, retention.bindings().size());
            List<Attribute> output = selection.output(0);
            List<Attribute> expected = stage.output()
                .stream()
                .filter(
                    attribute -> attribute.id().equals(demanded.id())
                        || attribute.id().equals(key.id())
                        || (direct && attribute.id().equals(optional.id()))
                )
                .toList();
            assertEquals(expected, output.subList(0, output.size() - 1));
            for (Attribute attribute : expected) {
                assertSame(attribute, output.get(SequenceStageInput.outputChannel(output, attribute, "stage [1]")));
            }
            selection.output(0);
            selection.source(0);
            assertEquals("one lazy proof per stable branch", 1, admissions.size());
        }
    }

    /** An internal demand cannot recover a field removed by a stage's final projection. */
    public void testLatestValidMandatoryDemandRequiresCurrentTypedOutput() {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key,payload), (FROM b | KEEP key) | KEEP key,payload | LIMIT 1
            """);
        Attribute payload = stageAttribute(sequence, 0, "payload");
        Attribute wrongType = new ReferenceAttribute(
            payload.source(),
            null,
            payload.name(),
            DataType.INTEGER,
            payload.nullable(),
            payload.id(),
            payload.synthetic()
        );
        var badType = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(wrongType)), List.of()),
            List.of()
        );
        String changed = expectThrows(IllegalStateException.class, () -> new Harness(sequence, null, badType)).getMessage();
        assertThat(changed, containsString("SEQUENCE stage [1] output changed type of column [payload#"));
        Sequence removed = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | DROP payload | KEEP key), (FROM b | KEEP key) | KEEP key | LIMIT 1
            """);
        Attribute removedPayload = SequenceStageProjection.eventSources((SequenceStage) removed.children().getFirst())
            .getFirst()
            .output()
            .stream()
            .filter(attribute -> attribute.name().equals("payload"))
            .findFirst()
            .orElseThrow();
        var missing = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(removedPayload)), List.of()),
            List.of()
        );
        String lost = expectThrows(IllegalStateException.class, () -> new Harness(removed, null, missing)).getMessage();
        assertThat(lost, containsString("SEQUENCE stage [1] output lost column [payload#"));
    }

    /** Long middle-stage streams do not move U beyond the last unused stage-1 timestamp. */
    public void testLatestValidLongMiddleTailKeepsStartBound() {
        Sequence sequence = latestThreeStagePlan();
        try (var harness = new Harness(sequence, null, latestConfig(sequence))) {
            harness.prepareLatestOpen();
            AtomicInteger middleReads = new AtomicInteger();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                if (index.equals("a")) {
                    return latestPage("a", 0, List.of(new LatestRow(0, 0, "shared", List.of())));
                }
                if (index.equals("b")) {
                    assertTimestampUpperBound(request, latestTime(10_000));
                    int read = middleReads.incrementAndGet();
                    assertTrue("the fixed start bounds middle-stage paging", read <= 2);
                    List<LatestRow> events = new ArrayList<>();
                    if (read == 1) {
                        for (int row = 0; row < 256; row++) {
                            events.add(new LatestRow(8_000 + row, row, "shared", List.of()));
                        }
                    }
                    return latestPage("b", 1, events);
                }
                return latestPage("c", 2, List.of());
            });
            assertTrue(rows.isEmpty());
            assertEquals(2, middleReads.get());
            assertEquals(4L, harness.source.status().candidateSearchRequests());
            finishLatest(harness);
        }
    }

    /** Used rows retain ring slots/high-water state but cannot keep fetching a middle-stage tail. */
    public void testLatestValidUsedStartMarkersStopReads() {
        Sequence sequence = latestThreeStagePlan();
        try (var harness = new Harness(sequence, null, latestConfig(sequence))) {
            harness.prepareLatestOpen();
            AtomicInteger middleReads = new AtomicInteger();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                if (index.equals("a")) {
                    return latestPage("a", 0, List.of(new LatestRow(0, 0, "shared", List.of())));
                }
                if (index.equals("b")) {
                    assertEquals("used markers must not request a second middle page", 1, middleReads.incrementAndGet());
                    List<LatestRow> events = new ArrayList<>();
                    for (int row = 0; row < 256; row++) {
                        events.add(new LatestRow(1 + row * 10L, row, "shared", List.of()));
                    }
                    return latestPage("b", 1, events);
                }
                return latestPage("c", 2, List.of(new LatestRow(2, 0, "shared", List.of())));
            });
            assertEquals(List.of(List.of(List.of("shared"))), rows);
            assertEquals(1, middleReads.get());
            assertEquals(3L, harness.source.status().candidateSearchRequests());
            assertTrue("the high water includes rows that become markers", harness.source.status().maxPendingCandidates() >= 2);
            finishLatest(harness);
        }
    }

    /** Buffered stage-1 rows still extend U and keep the reader alive after the first start has been used. */
    public void testLatestValidBufferedStartsSurviveFirstMatch() {
        Sequence sequence = latestThreeStagePlan();
        try (var harness = new Harness(sequence, null, latestConfig(sequence))) {
            harness.prepareLatestOpen();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                int stage = index.equals("a") ? 0 : index.equals("b") ? 1 : 2;
                if (stage > 0) {
                    assertTimestampUpperBound(request, latestTime(10_100));
                }
                return latestPage(
                    index,
                    stage,
                    List.of(new LatestRow(stage, 0, "shared", List.of()), new LatestRow(100 + stage, 1, "shared", List.of()))
                );
            });
            assertEquals(List.of(List.of(List.of("shared")), List.of(List.of("shared"))), rows);
            assertEquals(3L, harness.source.status().candidateSearchRequests());
            finishLatest(harness);
        }
    }

    /** With unformattable dates the raw-tail guard must stop even when compute exports no later rows. */
    public void testLatestValidRawTailRejectedByEvalWhereStillStops() {
        Sequence sequence = plan("""
            SEQUENCE BY key WITH {"maxspan":"10ms"} PRIMARY 1
            (FROM a | KEEP key),
            (FROM b | EVAL accepted=TO_LOWER(payload) | WHERE accepted=="keep" | KEEP key),
            (FROM c | KEEP key) | KEEP key | LIMIT 10
            """);
        try (var harness = new Harness(sequence, null, latestConfig(sequence))) {
            harness.prepareLatestOpen();
            AtomicInteger middleReads = new AtomicInteger();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                if (index.equals("c")) {
                    return latestPage("c", 2, List.of());
                }
                int stage = index.equals("a") ? 0 : 1;
                if (stage == 1) {
                    assertEquals("filtered raw progress must end this reader before another request", 1, middleReads.incrementAndGet());
                }
                SearchResponse response = boundaryPage(stage, 0, stage == 0 ? 1 : 256, Long.MIN_VALUE + stage * 11L, DataType.DATETIME);
                for (SearchHit hit : response.getHits().getHits()) {
                    hit.setDocumentField(new DocumentField("payload", List.of(new BytesRef("drop"))));
                }
                return response;
            });
            assertTrue(rows.isEmpty());
            assertEquals(1, middleReads.get());
            assertEquals(3L, harness.source.status().candidateSearchRequests());
            finishLatest(harness);
        }
    }

    /** Mandatory-only raw inputs join candidate repeatability checks without widening optional replay or KEEP. */
    public void testLatestValidMandatoryOnlyInputKeepsRepeatabilityGuard() {
        Sequence analyzed = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a METADATA _source | EVAL captured=TO_LOWER(payload) | RENAME captured AS visible
              | KEEP key,visible,_source),
            (FROM b | KEEP key) | KEEP key | LIMIT 10
            """);
        // The constructor-only demand is not visible to the optimizer yet. Prune the real public bindings while
        // preserving the stage's final output, so visible is a valid internal demand and _source is truly unused.
        Sequence sequence = analyzed.pruneOutput(
            AttributeSet.of(analyzed.output().stream().filter(attribute -> attribute.name().equals("key")).toList())
        );
        assertEquals(List.of("key"), sequence.sourceAttributes().stream().map(Attribute::name).toList());
        var stage = (SequenceStage) sequence.children().getFirst();
        var optionalReplay = new SequenceStageInput(stage, sequence.sourceAttributes());
        assertEquals(List.of("key"), optionalReplay.source(0).output().stream().map(Attribute::name).toList());
        var config = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "visible"))), List.of()),
            List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), selected -> {
                assertEquals(List.of("left"), strings(selected.block(0, 0), 0));
                return true;
            }))
        );
        try (var harness = new Harness(sequence, null, config)) {
            harness.prepareLatestOpen();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                assertEquals("this key-only fixture must not perform payload hydration", 2, request.source().sorts().size());
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                Map<String, Object> query = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    Strings.toString(request.source().query()),
                    false
                );
                assertTrue(query.get(SequenceRepeatabilityQueryBuilder.NAME) instanceof Map<?, ?>);
                Map<?, ?> guard = (Map<?, ?>) query.get(SequenceRepeatabilityQueryBuilder.NAME);
                List<String> expected = index.equals("a") ? List.of("@timestamp", "key", "payload") : List.of("@timestamp", "key");
                assertEquals(expected, guard.get("fields"));
                var projection = (SequenceDocValuesSearchExt) request.source().ext().getFirst();
                assertEquals(expected, projection.fields().stream().map(field -> field.field).toList());
                assertFalse("undemanded _source in KEEP must remain pruned", request.source().fetchSource().fetchSource());
                return latestPage(
                    index,
                    index.equals("a") ? 0 : 1,
                    List.of(new LatestRow(index.equals("a") ? 0 : 1, 0, "shared", List.of("LEFT")))
                );
            });
            assertEquals(List.of(List.of(List.of("shared"))), rows);
            assertEquals(0L, harness.source.status().payloadSearchRequests());
            finishLatest(harness);
        }
    }

    /** Mandatory cells remain available through direct retention, full replay and optional envelope overflow. */
    public void testLatestValidMandatoryValuesThroughEvalKeepRenameAndReplay() {
        for (boolean direct : List.of(false, true)) {
            for (Integer capacity : new Integer[] { null, 1 }) {
                Sequence sequence = optimizedPlan(direct ? """
                    SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
                    (FROM a | EVAL captured=TO_LOWER(payload) | RENAME captured AS visible
                      | WHERE visible!="skip" | KEEP key,visible),
                    (FROM b | KEEP key) | KEEP key,visible | LIMIT 10
                    """ : """
                    SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
                    (FROM a | EVAL captured=TO_LOWER(payload), late=CONCAT(key,"/late")
                      | RENAME captured AS visible | WHERE visible!="skip" | KEEP payload,key,visible,late),
                    (FROM b | KEEP key) | KEEP key,visible,late | LIMIT 10
                    """);
                var calls = new AtomicInteger();
                SequenceLatestValidConfig config = SequenceLatestValidConfig.defaults(
                    List.of(
                        List.of(
                            new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "visible")),
                            new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "key"))
                        ),
                        List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 1, "key")))
                    ),
                    List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), selected -> {
                        calls.incrementAndGet();
                        assertEquals(List.of("left"), strings(selected.block(0, 0), 0));
                        assertEquals(List.of("shared"), strings(selected.block(0, 1), 0));
                        assertEquals(List.of("shared"), strings(selected.block(1, 0), 0));
                        return true;
                    }))
                );
                try (var harness = new Harness(sequence, capacity, config)) {
                    harness.prepareLatestOpen();
                    List<List<List<String>>> rows = drainLatest(harness, request -> {
                        String index = request.getPointInTimeAuthorizationScope().indices()[0];
                        return latestPage(
                            index,
                            index.equals("a") ? 0 : 1,
                            List.of(new LatestRow(index.equals("a") ? 0 : 1, 0, "shared", List.of("LEFT")))
                        );
                    });
                    assertEquals(1, rows.size());
                    assertTrue(rows.getFirst().contains(List.of("shared")));
                    assertTrue(rows.getFirst().contains(List.of("left")));
                    if (direct == false) {
                        assertTrue(rows.getFirst().contains(List.of("shared/late")));
                    }
                    assertTrue(calls.get() > 0);
                    assertEquals(direct && capacity == null ? 0L : 1L, harness.source.status().payloadSearchRequests());
                    finishLatest(harness);
                }
            }
        }
    }

    /** Branch siblings retain global arrival order and expansion identity while sharing both primary and final documents. */
    public void testLatestValidExpandedSiblingBranchesSharePrimaryDocument() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM (FROM a METADATA _id | EVAL branch="first" | MV_EXPAND payload | KEEP key,payload,branch,_id),
                  (FROM a METADATA _id | EVAL branch="second" | MV_EXPAND payload | KEEP key,payload,branch,_id)),
            (FROM b METADATA _id | MV_EXPAND payload | KEEP key,payload,_id)
            | EVAL final_payload=MEMBER(2,payload), final_id=MEMBER(2,_id)
            | KEEP key,payload,branch,_id,final_payload,final_id | LIMIT 10
            """);
        var config = SequenceLatestValidConfig.defaults(
            List.of(
                List.of(
                    new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "payload")),
                    new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "branch"))
                ),
                List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 1, "payload")))
            ),
            List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), selected -> {
                assertTrue(List.of("first", "second").contains(strings(selected.block(0, 1), 0).getFirst()));
                return strings(selected.block(0, 0), 0).equals(strings(selected.block(1, 0), 0));
            }))
        );
        for (Integer capacity : new Integer[] { null, 1 }) {
            try (var harness = new Harness(sequence, capacity, config)) {
                harness.prepareLatestOpen();
                List<List<List<String>>> rows = drainLatest(harness, request -> {
                    String index = request.getPointInTimeAuthorizationScope().indices()[0];
                    return latestPage(
                        index,
                        index.equals("a") ? 0 : 1,
                        List.of(new LatestRow(index.equals("a") ? 0 : 1, 0, "shared", List.of("x", "y")))
                    );
                });
                assertEquals(2, rows.size());
                for (int row = 0; row < rows.size(); row++) {
                    List<String> expected = List.of(row == 0 ? "x" : "y");
                    assertEquals(expected, sourceCell(sequence, rows.get(row), 0, "payload"));
                    assertEquals(expected, sourceCell(sequence, rows.get(row), 1, "payload"));
                    assertEquals(List.of("first"), sourceCell(sequence, rows.get(row), 0, "branch"));
                    assertEquals(List.of("event-a-0"), sourceCell(sequence, rows.get(row), 0, "_id"));
                    assertEquals(List.of("event-b-0"), sourceCell(sequence, rows.get(row), 1, "_id"));
                }
                assertEquals(2L, harness.source.status().completedMatches());
                assertEquals(3L, harness.source.status().candidateSearchRequests());
                assertEquals(capacity == null ? 0L : 4L, harness.source.status().payloadSearchRequests());
                finishLatest(harness);
            }
        }
    }

    /** Predicate failure and cancellation release transferred values and all unconsumed buffered owners. */
    public void testLatestValidMandatoryPredicateFailureAndCancellationReleaseValues() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key,payload), (FROM b | KEEP key) | KEEP key,payload | LIMIT 10
            """);
        for (boolean cancelled : List.of(false, true)) {
            var config = SequenceLatestValidConfig.defaults(
                List.of(List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "payload"))), List.of()),
                List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), selected -> {
                    assertEquals(List.of("kept"), strings(selected.block(0, 0), 0));
                    if (cancelled) {
                        throw new TaskCancelledException("controlled mandatory cancellation");
                    }
                    throw new IllegalStateException("controlled mandatory predicate failure");
                }))
            );
            try (var harness = new Harness(sequence, null, config)) {
                harness.prepareLatestOpen();
                RuntimeException failure = expectThrows(RuntimeException.class, () -> drainLatest(harness, request -> {
                    String index = request.getPointInTimeAuthorizationScope().indices()[0];
                    return latestPage(
                        index,
                        index.equals("a") ? 0 : 1,
                        List.of(
                            new LatestRow(index.equals("a") ? 0 : 1, 0, "shared", List.of("kept")),
                            new LatestRow(100, 1, "shared", List.of("buffered"))
                        )
                    );
                }));
                assertThat(
                    failure.getMessage(),
                    containsString(cancelled ? "controlled mandatory cancellation" : "controlled mandatory predicate failure")
                );
                finishLatest(harness);
            }
        }
    }

    /** Mandatory block admission can fail while its producing page still belongs to the stage completion. */
    public void testLatestValidMandatoryCopyBreakerFailureReleasesStagePage() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key,payload), (FROM b | KEEP key) | KEEP key,payload | LIMIT 1
            """);
        var config = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "payload"))), List.of()),
            List.of()
        );
        try (var harness = new Harness(sequence, null, config)) {
            harness.prepareLatestOpen();
            assertNull(harness.source.getOutput());
            var pending = harness.client.take(TransportSearchAction.TYPE);
            deliverLatest(pending, latestPage("a", 0, List.of(new LatestRow(0, 0, "shared", List.of("x".repeat(65_536))))));
            assertNull(harness.source.getOutput());
            assertEquals(1, harness.stageExecutions);
            assertTrue(harness.client.pending.isEmpty());
            long originalLimit = harness.breaker.getLimit();
            harness.breaker.setLimitAndOverhead(harness.breaker.getUsed() + 4096, 1);
            try {
                CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, harness.source::getOutput);
                assertTrue(
                    "the failure must reach mandatory capture rather than transport admission",
                    java.util.Arrays.stream(failure.getStackTrace())
                        .anyMatch(frame -> frame.getClassName().equals(SequenceCandidateValues.class.getName()))
                );
            } finally {
                harness.breaker.setLimitAndOverhead(originalLimit, 1);
            }
            finishLatest(harness);
        }
    }

    /** A pending full replay keeps its own cells alive after source close also closes the matcher. */
    public void testLatestValidDeferredReplayAfterMatcherCloseReleasesMandatoryValues() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | EVAL visible=TO_LOWER(payload), late=CONCAT(key,"/late") | KEEP key,visible,late),
            (FROM b | KEEP key) | KEEP key,visible,late | LIMIT 1
            """);
        var config = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "visible"))), List.of()),
            List.of()
        );
        for (boolean failed : List.of(false, true)) {
            try (var harness = new Harness(sequence, 1, config)) {
                harness.deferStage = 3;
                harness.prepareLatestOpen();
                for (int attempt = 0; attempt < 30 && harness.deferredStage == null; attempt++) {
                    assertNull(harness.source.getOutput());
                    if (harness.client.pending.isEmpty() == false) {
                        var pending = harness.client.take(TransportSearchAction.TYPE);
                        var request = (SearchRequest) pending.request;
                        String index = request.getPointInTimeAuthorizationScope().indices()[0];
                        deliverLatest(
                            pending,
                            latestPage(
                                index,
                                index.equals("a") ? 0 : 1,
                                List.of(new LatestRow(index.equals("a") ? 0 : 1, 0, "shared", List.of("LEFT")))
                            )
                        );
                    }
                }
                assertNotNull(harness.deferredStage);
                assertEquals(1L, harness.source.status().completedMatches());
                harness.finishDriver();
                assertFalse(harness.done.isDone());
                assertTrue(harness.breaker.getUsed() > 0);
                ActionListener<Void> terminal = harness.deferredStage;
                harness.deferredStage = null;
                if (failed) {
                    terminal.onFailure(new TaskCancelledException("closed latest-valid replay"));
                } else {
                    terminal.onResponse(null);
                }
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 3));
                harness.assertFinished();
            }
        }
    }

    /** A later merged row expires old markers before the second batch reconstructs their selected payload. */
    public void testLatestValidLatePayloadReplaySurvivesUsedRecordExpiry() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITH {"maxspan":"10s"} PRIMARY 1
            (FROM a | EVAL visible=TO_LOWER(payload), late=CONCAT(key,"/late") | KEEP key,visible,late),
            (FROM b | KEEP key), (FROM c | KEEP key) | KEEP key,visible,late | LIMIT 10
            """);
        var config = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "visible"))), List.of(), List.of()),
            List.of(new SequenceLatestValidConfig.PredicateGroup(2, List.of(0), selected -> {
                assertEquals(List.of("value"), strings(selected.block(0, 0), 0));
                return true;
            }))
        );
        try (var harness = new Harness(sequence, 1, config)) {
            harness.prepareLatestOpen();
            AtomicInteger payloadReads = new AtomicInteger();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                if (request.source().sorts().size() == 1) {
                    int doc = payloadReads.getAndIncrement();
                    assertTrue(doc < 2);
                    assertEquals(doc + 1, harness.source.status().completedMatches());
                    return latestPage("a", 0, List.of(new LatestRow(doc * 10L, doc, "shared", List.of("VALUE"))));
                }
                return switch (index) {
                    case "a" -> latestPage(
                        "a",
                        0,
                        List.of(
                            new LatestRow(0, 0, "shared", List.of("VALUE")),
                            new LatestRow(10, 1, "shared", List.of("VALUE")),
                            new LatestRow(200_000, 2, "future", List.of("future"))
                        )
                    );
                    case "b" -> latestPage(
                        "b",
                        1,
                        List.of(
                            new LatestRow(1, 0, "shared", List.of()),
                            new LatestRow(11, 1, "shared", List.of()),
                            new LatestRow(100_000, 2, "other", List.of())
                        )
                    );
                    case "c" -> latestPage(
                        "c",
                        2,
                        List.of(new LatestRow(2, 0, "shared", List.of()), new LatestRow(12, 1, "shared", List.of()))
                    );
                    default -> throw new AssertionError("unexpected fixture source " + index);
                };
            });
            assertEquals(2, rows.size());
            assertEquals(2, payloadReads.get());
            for (List<List<String>> row : rows) {
                assertEquals(List.of("value"), sourceCell(sequence, row, 0, "visible"));
                assertEquals(List.of("shared/late"), sourceCell(sequence, row, 0, "late"));
            }
            finishLatest(harness);
        }
    }

    /** Factory equality and description still use exactly the original nine record components. */
    public void testLatestValidFactorySelectionPreservesDefaultRecordContract() {
        Sequence sequence = latestThreeStagePlan();
        try (var harness = new Harness(sequence, null, latestConfig(sequence))) {
            var factory = harness.operatorFactory;
            assertEquals(9, SequenceSourceOperator.Factory.class.getRecordComponents().length);
            var copy = new SequenceSourceOperator.Factory(
                factory.plan(),
                factory.client(),
                factory.cleanupClient(),
                factory.task(),
                factory.configuration(),
                factory.stagePlanner(),
                factory.stageExecutor(),
                factory.requestContext(),
                factory.executionInfo()
            );
            assertEquals(factory, copy);
            assertEquals(factory.hashCode(), copy.hashCode());
            assertEquals("SequenceSource[stages=3]", factory.describe());
            try (var legacy = factory.get(harness.context)) {
                assertEquals("SequenceSource[stages=3]", copy.describe());
                assertEquals(0L, ((SequenceSourceOperator) legacy).status().maxPendingCandidates());
            }
            finishLatest(harness);
        }
    }

    private static Sequence latestThreeStagePlan() {
        return optimizedPlan("""
            SEQUENCE BY key WITH {"maxspan":"10s"} PRIMARY 1
            (FROM a | KEEP key), (FROM b | KEEP key), (FROM c | KEEP key) | KEEP key | LIMIT 10
            """);
    }

    /** Own WHERE values stay private while MEMBER continues to name the earlier stage's final processed output. */
    public void testCompiledConditionsKeepOwnValuesThroughTrailingProjectionsAndReplay() {
        for (String projection : List.of("KEEP key,payload", "DROP own", "RENAME own AS renamed | KEEP key,renamed")) {
            String exported = projection.startsWith("RENAME") ? "renamed" : "payload";
            Sequence sequence = optimizedPlan("""
                SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
                (FROM a | EVAL artifact=TO_LOWER(payload) | KEEP key,artifact) AS download,
                (FROM b | MV_EXPAND payload | EVAL own=TO_LOWER(payload)
                  | WHERE own == MEMBER(download,artifact) | %s) AS execution
                | EVAL observed=MEMBER(execution,%s) | KEEP key,artifact,observed | LIMIT 10
                """.formatted(projection, exported));
            assertTrue(sequence.hasConditions());
            assertFalse(sequence.children().get(1).output().stream().anyMatch(attribute -> attribute.name().equals("own")));
            for (Integer capacity : new Integer[] { null, 1 }) {
                try (var harness = new Harness(sequence, capacity)) {
                    expectThrows(IllegalStateException.class, () -> harness.operatorFactory.get(harness.context));
                    harness.prepareLatestOpen();
                    List<List<List<String>>> rows = drainLatest(harness, request -> {
                        String index = request.getPointInTimeAuthorizationScope().indices()[0];
                        return latestPage(
                            index,
                            index.equals("a") ? 0 : 1,
                            List.of(
                                new LatestRow(
                                    index.equals("a") ? 0 : 1,
                                    0,
                                    "shared",
                                    index.equals("a") ? List.of("LEFT") : List.of("right", "LEFT")
                                )
                            )
                        );
                    });
                    assertEquals(1, rows.size());
                    assertEquals(List.of("left"), sourceCell(sequence, rows.getFirst(), 0, "artifact"));
                    assertEquals(List.of(exported.equals("renamed") ? "left" : "LEFT"), sourceCell(sequence, rows.getFirst(), 1, exported));
                    assertEquals(capacity == null, harness.source.status().payloadSearchRequests() == 0);
                    finishLatest(harness);
                }
            }
        }
    }

    /** A later public name may refer to another identity without changing the private value captured at WHERE. */
    public void testCompiledConditionUsesWhereIdentityWhenTrailingRenameReusesItsName() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | EVAL artifact=TO_LOWER(payload) | KEEP key,artifact),
            (FROM b | EVAL own=TO_LOWER(payload), published=CONCAT(key,"/public")
              | WHERE own == MEMBER(1,artifact) | DROP own | RENAME published AS own | KEEP key,own)
            | EVAL observed=MEMBER(2,own) | KEEP key,artifact,observed | LIMIT 10
            """);
        try (var harness = new Harness(sequence, null)) {
            harness.prepareLatestOpen();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                return latestPage(
                    index,
                    index.equals("a") ? 0 : 1,
                    List.of(new LatestRow(index.equals("a") ? 0 : 1, 0, "shared", List.of("LEFT")))
                );
            });
            assertEquals(1, rows.size());
            assertEquals(List.of("left"), sourceCell(sequence, rows.getFirst(), 0, "artifact"));
            assertEquals(List.of("shared/public"), sourceCell(sequence, rows.getFirst(), 1, "own"));
            finishLatest(harness);
        }
    }

    /** All ordinary entries reject test provenance before compilation, cursor preparation or transport work. */
    public void testLatestValidSourceAdmissionRejectsEveryTestConfiguration() {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1 (FROM a | KEEP key),(FROM b | KEEP key)
            | KEEP key | LIMIT 10
            """);
        try (var harness = new Harness(true, null, null, sequence)) {
            var demands = latestConfig(sequence).mandatoryDemands();
            for (var config : List.of(
                SequenceLatestValidConfig.unboundedForTests(demands, List.of()),
                SequenceLatestValidConfig.boundedForTests(16, 1, demands, List.of()),
                SequenceLatestValidConfig.boundedForTests(16, SequenceLatestValidConfig.SEARCH_STEP_LIMIT, demands, List.of()),
                SequenceLatestValidConfig.boundedForTests(1025, SequenceLatestValidConfig.SEARCH_STEP_LIMIT, demands, List.of())
            )) {
                long before = harness.breaker.getUsed();
                expectThrows(IllegalArgumentException.class, () -> harness.operatorFactory.get(harness.context, config));
                expectThrows(
                    IllegalArgumentException.class,
                    () -> new SequenceSourceOperator(harness.operatorFactory, harness.context, TimeValue.timeValueMinutes(2), 1, config)
                );
                expectThrows(
                    IllegalArgumentException.class,
                    () -> new SequenceSourceOperator(
                        harness.operatorFactory,
                        harness.context,
                        TimeValue.timeValueMinutes(2),
                        1,
                        config,
                        null
                    )
                );
                assertEquals(before, harness.breaker.getUsed());
                assertTrue(harness.client.pending.isEmpty());
                assertEquals(0, harness.stagePlans);
            }
            long before = harness.breaker.getUsed();
            expectThrows(IllegalArgumentException.class, () -> harness.operatorFactory.getWithConditions(harness.context, 1025, null));
            assertEquals(before, harness.breaker.getUsed());
            assertTrue(harness.client.pending.isEmpty());
            assertEquals(0, harness.stagePlans);
            assertNull(harness.source.status().latestValidCounters());
        }
        Sequence excessivePlan = plan("""
            SEQUENCE BY key WITH {"maxspan":"30s","pairing":"latest_valid","max_candidates":1025} PRIMARY 1
            (FROM a | KEEP key),(FROM b | KEEP key) | KEEP key | LIMIT 10
            """);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new Harness(excessivePlan, null, latestConfig(excessivePlan))).getMessage(),
            containsString("max_candidates [1025]")
        );
    }

    /** A secondary close failure cannot replace the original constructor admission failure or prevent refunds. */
    public void testLatestValidConstructorKeepsPrimaryBreakerFailureWhenConditionCleanupThrows() {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1 (FROM a | KEEP key),(FROM b | KEEP key)
            | KEEP key | LIMIT 10
            """);
        var config = latestConfig(sequence);
        try (var harness = new Harness(true, null, null, sequence)) {
            long baseline = harness.breaker.getUsed();
            long metadata;
            try (
                var probe = new SequenceLatestValidMatcher(
                    2,
                    TimeValue.timeValueSeconds(30),
                    LocalCircuitBreaker.forAsyncIo(harness.context.breaker()),
                    () -> false,
                    SequenceKeyCodec::compare,
                    null,
                    config
                )
            ) {
                metadata = harness.breaker.getUsed() - baseline;
                assertTrue("the real matcher must own metadata before source admission", metadata > 0);
            }
            assertEquals(baseline, harness.breaker.getUsed());
            long limit = harness.breaker.getLimit();
            double overhead = harness.breaker.getOverhead();
            AtomicInteger closes = new AtomicInteger();
            // This callback isolates a failing resource close; native compiler ownership is covered by its own real evaluator tests.
            Releasable cleanup = () -> {
                closes.incrementAndGet();
                throw new IllegalStateException("secondary condition cleanup failure");
            };
            harness.breaker.setLimitAndOverhead(baseline + metadata, 1);
            try {
                CircuitBreakingException failure = expectThrows(
                    CircuitBreakingException.class,
                    () -> new SequenceSourceOperator(
                        harness.operatorFactory,
                        harness.context,
                        TimeValue.timeValueMinutes(2),
                        1,
                        config,
                        cleanup
                    )
                );
                assertThat(failure.getMessage(), containsString("[esql_sequence_reader]"));
            } finally {
                harness.breaker.setLimitAndOverhead(limit, overhead);
                assertEquals(1, closes.get());
                assertEquals("failed source and matcher metadata must both be refunded", baseline, harness.breaker.getUsed());
                assertTrue(harness.client.pending.isEmpty());
                assertEquals(0, harness.stagePlans);
                harness.blocks.ensureAllBlocksAreReleased();
                harness.finishDriver();
                harness.assertFinished();
            }
        }
    }

    /** Early finish reports only processed work; profiling and repeated closure never publish another summary. */
    public void testLatestValidBoundSummaryWaitsForTerminalAndHonorsWarningsMode() {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1 (FROM a | KEEP key),(FROM b | KEEP key)
            | KEEP key | LIMIT 10
            """);
        var config = SequenceLatestValidConfig.bounded(1, latestConfig(sequence).mandatoryDemands(), List.of());
        for (var mode : DriverContext.WarningsMode.values()) {
            try (var harness = new Harness(true, null, null, sequence, null, null, config, false, mode)) {
                harness.prepareLatestOpen();
                for (int attempt = 0; attempt < 100
                    && harness.source.status().latestValidCounters().candidatesDroppedAtBound() == 0; attempt++) {
                    assertNull(harness.source.getOutput());
                    if (harness.client.pending.isEmpty() == false) {
                        var pending = harness.client.take(TransportSearchAction.TYPE);
                        String index = ((SearchRequest) pending.request).getPointInTimeAuthorizationScope().indices()[0];
                        List<LatestRow> rows = new ArrayList<>();
                        if (index.equals("a")) {
                            for (int row = 0; row < 160; row++) {
                                rows.add(new LatestRow(row, row, "shared", List.of("0")));
                            }
                        } else {
                            rows.add(new LatestRow(1000, 0, "shared", List.of("1")));
                        }
                        deliverLatest(pending, latestPage(index, index.equals("a") ? 0 : 1, rows));
                    }
                }
                var before = harness.source.status().latestValidCounters();
                assertEquals(127, before.candidatesDroppedAtBound());
                assertEquals(1, before.keysThatHitBound());
                assertEquals(1, before.largestRingFill());
                assertFalse(harness.source.isFinished());
                assertTrue(harness.observedWarnings.isEmpty());
                harness.source.status();
                assertTrue(harness.observedWarnings.isEmpty());
                harness.source.finish();
                harness.source.finish();
                harness.source.close();
                assertEquals(before, harness.source.status().latestValidCounters());
                assertEquals(
                    mode == DriverContext.WarningsMode.COLLECT
                        ? List.of("SEQUENCE latest_valid dropped [127] candidates at max_candidates [1] across [1] key lifetimes")
                        : List.of(),
                    harness.observedWarnings
                );
                finishLatest(harness);
                assertEquals(harness.observedWarnings, harness.context.warnings());
            }
        }
    }

    /** Saturating native scalar warnings cannot spend either terminal summary allowance. */
    public void testLatestValidConditionWarningsAndBothLimitSummariesHaveIndependentBudgets() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key,payload),(FROM b | KEEP key),(FROM c | KEEP key),
            (FROM d | KEEP key),(FROM e | KEEP key),
            (FROM f | WHERE TO_LONG(payload) == TO_LONG(MEMBER(1,payload)) | KEEP key)
            | KEEP key,payload | LIMIT 10
            """);
        try (var harness = new Harness(sequence, null)) {
            harness.prepareLatestOpen();
            List<List<List<String>>> result = drainLatest(harness, request -> {
                assertTrue("summaries and condition warnings wait for terminal closure", harness.observedWarnings.isEmpty());
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                int stage = index.charAt(0) - 'a';
                int perKey = stage == 0 ? 17 : 16;
                List<LatestRow> rows = new ArrayList<>();
                for (int key = 0; key < 2; key++) {
                    if (stage == 5) {
                        rows.add(new LatestRow(162 + key, key, "key" + key, List.of("1")));
                    } else {
                        for (int row = 0; row < perKey; row++) {
                            long time = stage == 0 ? key * 17L + row : 34 + (stage - 1) * 32L + key * 16L + row;
                            rows.add(
                                new LatestRow(time, key * perKey + row, "key" + key, List.of(stage == 0 ? "bad-" + key + "-" + row : "0"))
                            );
                        }
                    }
                }
                return latestPage(index, stage, rows);
            });
            assertEquals(List.of(), result);
            var counters = harness.source.status().latestValidCounters();
            assertEquals(2, counters.candidatesDroppedAtBound());
            assertEquals(2, counters.keysThatHitBound());
            assertEquals(2, counters.searchesAbandonedAtWorkLimit());
            assertEquals(2 * SequenceLatestValidConfig.SEARCH_STEP_LIMIT, counters.searchSteps());
            assertEquals(SequenceLatestValidConfig.SEARCH_STEP_LIMIT, counters.maxSearchStepsPerFinalRow());
            assertEquals(16, counters.largestRingFill());
            assertEquals(0, counters.candidatesUsedWithoutReport());
            assertEquals(21, harness.observedWarnings.stream().filter(warning -> warning.startsWith("SEQUENCE ") == false).count());
            assertEquals(
                List.of(
                    "SEQUENCE latest_valid dropped [2] candidates at max_candidates [16] across [2] key lifetimes",
                    "SEQUENCE latest_valid abandoned [2] final-row searches at the work limit [131072]"
                ),
                harness.observedWarnings.stream().filter(warning -> warning.startsWith("SEQUENCE ")).toList()
            );
            harness.source.status();
            harness.source.finish();
            finishLatest(harness);
            assertEquals(counters, harness.source.status().latestValidCounters());
            assertEquals(23, harness.context.warnings().size());
        }
    }

    private static SequenceLatestValidConfig latestConfig(Sequence sequence) {
        List<List<SequenceLatestValidConfig.ValueDemand>> demands = new ArrayList<>();
        for (int stage = 0; stage < sequence.children().size(); stage++) {
            demands.add(List.of());
        }
        return SequenceLatestValidConfig.defaults(demands, List.of());
    }

    private static Attribute stageAttribute(Sequence sequence, int stage, String name) {
        return sequence.children()
            .get(stage)
            .output()
            .stream()
            .filter(attribute -> attribute.name().equals(name))
            .findFirst()
            .orElseThrow();
    }

    private static List<String> sourceCell(Sequence sequence, List<List<String>> row, int stage, String name) {
        for (int column = 0; column < sequence.sourceAttributes().size(); column++) {
            if (sequence.sourceStages().get(column) == stage && sequence.sourceAttributes().get(column).name().equals(name)) {
                return row.get(column);
            }
        }
        throw new AssertionError("missing selected output " + stage + "/" + name);
    }

    private static List<String> strings(org.elasticsearch.compute.data.Block source, int row) {
        BytesRefBlock block = (BytesRefBlock) source;
        List<String> result = new ArrayList<>();
        for (int value = 0; value < block.getValueCount(row); value++) {
            result.add(block.getBytesRef(block.getFirstValueIndex(row) + value, new BytesRef()).utf8ToString());
        }
        return result;
    }

    private static List<List<List<String>>> drainLatest(Harness harness, Function<SearchRequest, SearchResponse> responses) {
        List<List<List<String>>> result = new ArrayList<>();
        for (int attempt = 0; attempt < 1000 && harness.source.isFinished() == false; attempt++) {
            try (Page page = harness.source.getOutput()) {
                if (page != null) {
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        List<List<String>> cells = new ArrayList<>();
                        for (int column = 0; column < page.getBlockCount(); column++) {
                            cells.add(strings(page.getBlock(column), row));
                        }
                        result.add(cells);
                    }
                }
            }
            if (harness.client.pending.isEmpty() == false && harness.client.pending.getFirst().action.equals(TransportSearchAction.TYPE)) {
                var pending = harness.client.take(TransportSearchAction.TYPE);
                deliverLatest(pending, responses.apply((SearchRequest) pending.request));
            }
        }
        assertTrue("the bounded transport fixture must finish", harness.source.isFinished());
        return result;
    }

    private static void finishLatest(Harness harness) {
        harness.finishDriver();
        if (harness.client.pending.isEmpty() == false) {
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 3));
        }
        harness.assertFinished();
        assertTrue(harness.client.pending.isEmpty());
    }

    private static void deliverLatest(Pending<SearchResponse> pending, SearchResponse response) {
        try (Releasable ignored = response::decRef) {
            if (((SearchRequest) pending.request).source().sorts().size() == 1) {
                for (SearchHit hit : response.getHits().getHits()) {
                    Object[] sort = { hit.getRawSortValues()[1] };
                    hit.sortValues(SearchSortValues.fromFormattedAndRaw(sort, sort));
                }
            }
            pending.listener.onResponse(response);
        }
    }

    private static String latestTime(long millis) {
        return java.time.Instant.parse("2026-09-25T21:14:51Z").plusMillis(millis).toString();
    }

    /** Transport fixtures describe raw documents; matching runs through the production source and stage drivers. */
    private record LatestRow(long millis, int doc, String key, List<String> payload) {}

    private static SearchResponse latestPage(String index, int shard, List<LatestRow> rows) {
        SearchHit[] selected = new SearchHit[rows.size()];
        for (int row = 0; row < rows.size(); row++) {
            LatestRow event = rows.get(row);
            SearchHit hit = SearchHit.unpooled(event.doc(), "event-" + index + "-" + event.doc());
            hit.shard(new SearchShardTarget("node", new ShardId(index, index + "-uuid", 0), null));
            hit.setDocumentField(new DocumentField("key", event.key() == null ? List.of() : List.of(new BytesRef(event.key()))));
            hit.setDocumentField(
                new DocumentField("payload", event.payload().stream().map(value -> (Object) new BytesRef(value)).toList())
            );
            var instant = java.time.Instant.parse(latestTime(event.millis()));
            hit.setDocumentField(new DocumentField("@timestamp", List.of(instant.toEpochMilli())));
            long ordinal = ShardDocSortField.encodeShardAndDoc(shard, event.doc());
            hit.sortValues(
                SearchSortValues.fromFormattedAndRaw(
                    new Object[] { instant.toString(), ordinal },
                    new Object[] { instant.toEpochMilli(), ordinal }
                )
            );
            selected[row] = hit;
        }
        var hits = new SearchHits(selected, null, Float.NaN);
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
                3,
                3,
                0,
                1,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
        } finally {
            hits.decRef();
        }
    }

    private static boolean containsTimestampUpperBound(Object value, String expected) {
        if (value instanceof Map<?, ?> map) {
            if (map.get("range") instanceof Map<?, ?> ranges && ranges.get("@timestamp") instanceof Map<?, ?> range) {
                if (expected.equals(range.get("lte"))) {
                    assertFalse("time horizon must remain inclusive", range.containsKey("lt"));
                    return true;
                }
            }
            return map.values().stream().anyMatch(child -> containsTimestampUpperBound(child, expected));
        }
        if (value instanceof List<?> list) {
            return list.stream().anyMatch(child -> containsTimestampUpperBound(child, expected));
        }
        return false;
    }

    private static SearchResponse upperBoundPage(int stage, long... offsets) {
        SearchResponse response = eventPage(stage, 0, offsets.length, "shared");
        boolean success = false;
        try {
            var base = java.time.Instant.parse("2026-09-25T21:14:51Z");
            SearchHit[] hits = response.getHits().getHits();
            for (int row = 0; row < hits.length; row++) {
                var time = base.plusMillis(offsets[row]);
                hits[row].setDocumentField(new DocumentField("@timestamp", List.of(time.toEpochMilli())));
                if (stage == 0) {
                    hits[row].setDocumentField(
                        new DocumentField(
                            "key",
                            row == 0 ? List.of(new BytesRef("shared"), new BytesRef("discard")) : List.of(new BytesRef("filtered"))
                        )
                    );
                    hits[row].setDocumentField(new DocumentField("payload", List.of(new BytesRef(row == 0 ? "7" : "0"))));
                }
                Object[] sort = { time.toString(), ShardDocSortField.encodeShardAndDoc(stage, row) };
                Object[] raw = { time.toEpochMilli(), sort[1] };
                hits[row].sortValues(SearchSortValues.fromFormattedAndRaw(sort, raw));
            }
            success = true;
            return response;
        } finally {
            if (success == false) {
                response.decRef();
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
        Object[] raw = { java.time.Instant.parse(timestamp).toEpochMilli(), sort[1] };
        hit.sortValues(SearchSortValues.fromFormattedAndRaw(sort, raw));
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

    private static SearchResponse eventPage(int stage, int first, int count, String key) {
        String index = stage == 0 ? "a" : "b";
        var base = java.time.Instant.parse("2026-09-25T21:14:51Z");
        SearchHit[] selected = new SearchHit[count];
        for (int row = 0; row < count; row++) {
            int doc = first + row;
            var timestamp = base.plusMillis((stage == 0 ? 0 : 1000) + doc);
            SearchHit hit = SearchHit.unpooled(doc, "event-" + index + "-" + doc);
            hit.shard(new SearchShardTarget("node", new ShardId(index, index + "-uuid", 0), null));
            hit.setDocumentField(new DocumentField("key", List.of(new BytesRef(key))));
            hit.setDocumentField(new DocumentField("@timestamp", List.of(timestamp.toEpochMilli())));
            Object[] sort = { timestamp.toString(), ShardDocSortField.encodeShardAndDoc(stage, doc) };
            Object[] raw = { timestamp.toEpochMilli(), sort[1] };
            hit.sortValues(SearchSortValues.fromFormattedAndRaw(sort, raw));
            selected[row] = hit;
        }
        var hits = new SearchHits(selected, null, Float.NaN);
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

    private static String eventSource(String index, int event) {
        return "{\"event\":\"" + index + event + "\",\"values\":[" + event + "," + (event + 10) + "]}";
    }

    private static SearchResponse orderedEvents(int stage, List<Integer> events, boolean identity) {
        String index = stage == 0 ? "a" : "b";
        return orderedEvents(stage, events, identity, event -> "payload-" + index + "-" + event);
    }

    private static SearchResponse orderedEvents(int stage, List<Integer> events, boolean identity, Function<Integer, String> payload) {
        String index = stage == 0 ? "a" : "b";
        var base = java.time.Instant.parse("2026-09-25T21:14:51Z");
        SearchHit[] selected = new SearchHit[events.size()];
        for (int row = 0; row < events.size(); row++) {
            int event = events.get(row);
            int doc = 2 - event;
            var timestamp = base.plusSeconds(event * 2L + stage);
            SearchHit hit = SearchHit.unpooled(doc, "event-" + index + "-" + event);
            hit.shard(new SearchShardTarget("node", new ShardId(index, index + "-uuid", 0), null));
            hit.setDocumentField(new DocumentField("key", List.of(new BytesRef("key" + event))));
            hit.setDocumentField(new DocumentField("payload", List.of(new BytesRef(payload.apply(event)))));
            hit.sourceRef(new BytesArray(eventSource(index, event)));
            long shardDoc = ShardDocSortField.encodeShardAndDoc(stage, doc);
            Object[] sort;
            if (identity) {
                sort = new Object[] { shardDoc };
            } else {
                hit.setDocumentField(new DocumentField("@timestamp", List.of(timestamp.toEpochMilli())));
                sort = new Object[] { timestamp.toString(), shardDoc };
            }
            Object[] raw = identity ? sort : new Object[] { timestamp.toEpochMilli(), shardDoc };
            hit.sortValues(SearchSortValues.fromFormattedAndRaw(sort, raw));
            selected[row] = hit;
        }
        var hits = new SearchHits(selected, null, Float.NaN);
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

    private static SearchShardsResponse shards(String... indices) {
        List<SearchShardsGroup> groups = new ArrayList<>();
        for (String index : indices) {
            groups.add(new SearchShardsGroup(new ShardId(index, index + "-uuid", 0), List.of("node"), false, SplitShardCountSummary.UNSET));
        }
        return new SearchShardsResponse(groups, 0, List.of(), Map.of());
    }

    /** Lifecycle tests deliberately demand one output-only computation to keep the full-replay path covered. */
    private static Sequence replayPlan() {
        return plan("""
            sequence by key with {"maxspan":"30s"} PRIMARY 1
            (from a | eval replay_payload=CONCAT(key,"/replay") | keep key,replay_payload), (from b | keep key)
            | keep key,replay_payload | limit 1
            """);
    }

    private static Sequence plan(String query) {
        return plan(query, DataType.DATETIME);
    }

    private static Sequence plan(String query, DataType timestampType) {
        return analyzedPlan(query, timestampType).collect(Sequence.class).getFirst();
    }

    private static Sequence optimizedPlan(String query) {
        return optimizedStatement(query).collect(Sequence.class).getFirst();
    }

    private static LogicalPlan optimizedStatement(String query) {
        return new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        ).optimize(analyzedPlan(query, DataType.DATETIME));
    }

    private static LogicalPlan analyzedPlan(String query, DataType timestampType) {
        EsField timestamp = timestampType == DataType.DATETIME
            ? DateEsField.dateEsField("@timestamp", Map.of(), true, NONE)
            : new EsField("@timestamp", timestampType, Map.of(), true, NONE);
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            timestamp,
            "key",
            new EsField("key", DataType.KEYWORD, Map.of(), true, NONE),
            "payload",
            new EsField("payload", DataType.KEYWORD, Map.of(), true, NONE)
        );
        var analyzer = EsqlTestUtils.analyzer().minimumTransportVersion(TransportVersion.current());
        for (String index : List.of("a", "b", "c", "d", "e", "f")) {
            analyzer.addIndex(new EsIndex(index, mapping, Map.of(index, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        return analyzer.statement(query);
    }

    private static final class Harness implements Releasable {
        private final List<String> observedWarnings = new ArrayList<>();
        private final TestThreadPool realThreadPool;
        private final ThreadPool threadPool;
        private final PendingClient client;
        private final CircuitBreaker breaker;
        private final MockBlockFactory blocks;
        private final DriverContext context;
        private final SequenceSourceOperator source;
        private final SequenceSourceOperator.Factory operatorFactory;
        private final Sequence sequence;
        private final PlainActionFuture<Void> done = new PlainActionFuture<>();
        private final List<DriverContext> stageContexts = new ArrayList<>();
        private int stagePlans;
        private int driverCreations;
        private int stageExecutions;
        private int failPlan = -1;
        private int failDriverCreation = -1;
        private int failStage = -1;
        private int deferStage = -1;
        private int deferStageStart = -1;
        private Runnable deferredStart;
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
            this(realStages, queue, executionInfo, null);
        }

        private Harness(boolean realStages, DeterministicTaskQueue queue, EsqlExecutionInfo executionInfo, Sequence suppliedSequence) {
            this(realStages, queue, executionInfo, suppliedSequence, null);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            Integer retainedHandleCapacity
        ) {
            this(realStages, queue, executionInfo, suppliedSequence, retainedHandleCapacity, null);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            Integer retainedHandleCapacity,
            QueryBuilder requestFilter
        ) {
            this(realStages, queue, executionInfo, suppliedSequence, retainedHandleCapacity, requestFilter, null);
        }

        private Harness(Sequence sequence, Integer retainedHandleCapacity, SequenceLatestValidConfig latestValid) {
            this(true, null, null, sequence, retainedHandleCapacity, null, latestValid);
        }

        private Harness(Sequence sequence, Integer retainedHandleCapacity) {
            this(true, null, null, sequence, retainedHandleCapacity, null, null, true);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            Integer retainedHandleCapacity,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid
        ) {
            this(realStages, queue, executionInfo, suppliedSequence, retainedHandleCapacity, requestFilter, latestValid, false);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            Integer retainedHandleCapacity,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid,
            boolean compileConditions
        ) {
            this(
                realStages,
                queue,
                executionInfo,
                suppliedSequence,
                retainedHandleCapacity,
                requestFilter,
                latestValid,
                compileConditions,
                DriverContext.WarningsMode.COLLECT
            );
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            Integer retainedHandleCapacity,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid,
            boolean compileConditions,
            DriverContext.WarningsMode warningsMode
        ) {
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
            context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blocks, null) {
                @Override
                public WarningsMode warningsMode() {
                    return warningsMode;
                }

                @Override
                public boolean addWarning(String warning) {
                    boolean added = super.addWarning(warning);
                    if (added) {
                        observedWarnings.add(warning);
                    }
                    return added;
                }
            };
            sequence = suppliedSequence == null ? replayPlan() : suppliedSequence;
            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader("sequence-test-caller", "owner");
                operatorFactory = new SequenceSourceOperator.Factory(
                    new SequenceExec(sequence.source(), sequence, requestFilter),
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
                );
                if (compileConditions) {
                    if (retainedHandleCapacity == null) {
                        source = (SequenceSourceOperator) operatorFactory.getWithConditions(context, 16, null);
                    } else {
                        var conditions = SequenceConditionCompiler.compile(sequence, 16, FoldContext.small(), context, null);
                        boolean success = false;
                        try {
                            source = new SequenceSourceOperator(
                                operatorFactory,
                                context,
                                TimeValue.timeValueMinutes(2),
                                retainedHandleCapacity,
                                conditions.config(),
                                conditions
                            );
                            success = true;
                        } finally {
                            if (success == false) {
                                conditions.close();
                            }
                        }
                    }
                } else {
                    source = retainedHandleCapacity == null
                        ? (SequenceSourceOperator) (latestValid == null
                            ? operatorFactory.get(context)
                            : operatorFactory.get(context, latestValid))
                        : new SequenceSourceOperator(
                            operatorFactory,
                            context,
                            TimeValue.timeValueMinutes(2),
                            retainedHandleCapacity,
                            latestValid
                        );
                }
            } catch (RuntimeException | Error e) {
                context.finish();
                assertEquals("failed construction must release matcher and projection ownership", 0, breaker.getUsed());
                blocks.ensureAllBlocksAreReleased();
                if (realThreadPool != null) {
                    realThreadPool.close();
                }
                throw e;
            }
        }

        private void prepareLatestOpen() {
            for (LogicalPlan child : sequence.children()) {
                for (var relation : SequenceStageProjection.eventSources((SequenceStage) child)) {
                    assertNull(source.getOutput());
                    client.take(EsqlSearchShardsAction.TYPE).listener.onResponse(
                        shards(relation.concreteQualifiedIndices().toArray(String[]::new))
                    );
                }
            }
            assertNull(source.getOutput());
            client.take(TransportOpenPointInTimeAction.TYPE).listener.onResponse(openResponse());
        }

        private Supplier<List<Driver>> planStage(PhysicalPlan physical, Consumer<Page> output) {
            stagePlans++;
            if (stagePlans == failPlan) {
                throw new IllegalStateException("controlled stage planning failure");
            }
            // The production local planner and driver own all source/binding/block references. Start and terminal
            // delivery can be delayed below to test computation/publication windows without a production hook.
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
                false,
                null,
                0,
                MatcherWatchdog.noop(),
                TransportVersion.current()
            );
            var planned = planner.plan(
                "sequence stage",
                FoldContext.small(),
                PlannerSettings.DEFAULTS,
                new OutputExec(physical, output),
                EmptyIndexedByShardId.instance(),
                true
            );
            return () -> {
                driverCreations++;
                if (driverCreations == failDriverCreation) {
                    throw new IllegalStateException("controlled stage driver creation failure");
                }
                List<Driver> drivers = planned.createDrivers("sequence-callbacks");
                for (Driver driver : drivers) {
                    assertFalse("every execution requires a fresh DriverContext", stageContexts.contains(driver.driverContext()));
                    stageContexts.add(driver.driverContext());
                }
                return drivers;
            };
        }

        private void executeStage(List<Driver> drivers, ActionListener<Void> listener) {
            assertEquals(1, drivers.size());
            stageExecutions++;
            if (stageExecutions == failStage) {
                drivers.getFirst().abort(new IllegalStateException("controlled synchronous stage failure"), listener);
                return;
            }
            boolean deferTerminal = stageExecutions == deferStage;
            if (stageExecutions == deferStageStart) {
                assertNull(deferredStart);
                deferredStart = () -> startStage(drivers, listener, deferTerminal);
            } else {
                startStage(drivers, listener, deferTerminal);
            }
        }

        private void startStage(List<Driver> drivers, ActionListener<Void> listener, boolean deferTerminal) {
            var completed = new PlainActionFuture<Void>();
            Driver.start(threadPool.getThreadContext(), Runnable::run, drivers.getFirst(), Driver.DEFAULT_MAX_ITERATIONS, completed);
            assertTrue("these bounded local identity stages must finish synchronously", completed.isDone());
            completed.actionGet();
            if (deferTerminal) {
                assertNull(deferredStage);
                deferredStage = listener;
            } else {
                listener.onResponse(null);
            }
        }

        private void respond(Pending<SearchResponse> pending, int stage) {
            respond(pending, stage, eventResponse(stage));
        }

        private void respond(Pending<SearchResponse> pending, int stage, SearchResponse response) {
            var request = (SearchRequest) pending.request;
            assertArrayEquals(new String[] { "a", "b" }, request.getPointInTimeAuthorizationIndices().indices());
            assertArrayEquals(new String[] { stage == 0 ? "a" : "b" }, request.getPointInTimeAuthorizationScope().indices());
            try (Releasable ignored = response::decRef) {
                if (request.source().sorts().size() == 1) {
                    for (SearchHit hit : response.getHits().getHits()) {
                        Object[] original = hit.getSortValues();
                        Object[] physical = { original[original.length - 1] };
                        hit.sortValues(SearchSortValues.fromFormattedAndRaw(physical, physical));
                    }
                }
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

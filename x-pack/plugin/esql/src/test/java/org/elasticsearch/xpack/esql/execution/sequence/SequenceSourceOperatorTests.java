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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
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
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlSearchShardsAction;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.ConfigurationBuilder;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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

    /** Exercise Factory.get with no hidden pragma; the selected limit is private execution state, not a status API. */
    public void testProductionFactorySelectsStageAwareDefaults() throws ReflectiveOperationException {
        var boundField = SequenceSourceOperator.class.getDeclaredField("candidateBound");
        boundField.setAccessible(true);
        for (int stages : List.of(2, 3, 4)) {
            String members = String.join(",", java.util.Collections.nCopies(stages, "(FROM a | KEEP key)"));
            Sequence sequence = plan("SEQUENCE BY key WITHIN 30s PRIMARY 1 " + members + " | KEEP key | LIMIT 1");
            try (var harness = new Harness(false, null, null, sequence)) {
                assertEquals("factory default for [" + stages + "] stages", 8, boundField.getInt(harness.source));
                assertTrue("constructing the production factory must not start a search", harness.client.pending.isEmpty());
                harness.finishDriver();
                harness.assertFinished();
            }
        }
        Sequence accepted = plan("SEQUENCE BY key WITHIN 30s PRIMARY 1 (FROM a | KEEP key),(FROM a | KEEP key) | KEEP key | LIMIT 1");
        // The factory must refuse an oversized plan even if it arrives without running through the analyzer.
        Sequence overlong = accepted.replaceChildren(java.util.Collections.nCopies(5, accepted.children().getFirst()));
        assertThat(expectThrows(IllegalArgumentException.class, () -> {
            try (var harness = new Harness(false, null, null, overlong)) {
                assertTrue(harness.client.pending.isEmpty());
            }
        }).getMessage(), containsString("SEQUENCE supports at most [4] stages"));
    }

    /** A failed stage releases raw/bound cells and the prepared compiler on the same held-memory timeline. */
    public void testBudgetReleasesAfterStageFailure() {
        try (var harness = new Harness(true, true)) {
            SequenceMemoryBudget memory = sourceBudget(harness.source);
            assertNotNull(memory);
            assertTrue(memory.currentBytes() > 0);
            assertEquals(memory.peakBytes(), harness.source.status().runtime().peakBytesHeld());
            harness.failStage = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertThat(
                expectThrows(IllegalStateException.class, harness.source::getOutput).getMessage(),
                containsString("controlled synchronous stage failure")
            );
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertEquals(0, memory.currentBytes());
            assertEquals(memory.peakBytes(), harness.source.status().runtime().peakBytesHeld());
        }
    }

    /** Repeated production close must not refund a damaged budget twice or borrow an unrelated reservation. */
    public void testBudgetOverReleaseAndRepeatedSourceClosePreserveUnrelatedReservation() throws Exception {
        try (var harness = new Harness(true, true)) {
            SequenceMemoryBudget memory = sourceBudget(harness.source);
            assertTrue(memory.currentBytes() > 0);
            harness.breaker.addEstimateBytesAndMaybeBreak(777, "unrelated");
            try {
                var breakerField = SequenceSourceOperator.class.getDeclaredField("breaker");
                breakerField.setAccessible(true);
                CircuitBreaker scoped = (CircuitBreaker) breakerField.get(harness.source);
                var scopeField = scoped.getClass().getDeclaredField("scope");
                scopeField.setAccessible(true);
                Object scope = scopeField.get(scoped);
                var scopeBytes = scope.getClass().getDeclaredField("bytes");
                scopeBytes.setAccessible(true);
                var totalBytes = SequenceMemoryBudget.class.getDeclaredField("currentBytes");
                totalBytes.setAccessible(true);
                assertTrue(scopeBytes.getLong(scope) > 0);
                // Damage only admission counts. The real source still owns its full request-breaker reservation.
                scopeBytes.setLong(scope, scopeBytes.getLong(scope) - 1);
                totalBytes.setLong(memory, totalBytes.getLong(memory) - 1);
                IllegalStateException failure = expectThrows(IllegalStateException.class, harness.source::close);
                assertThat(failure.getMessage(), containsString("SEQUENCE release exceeds live admissions"));
                assertEquals(0, memory.currentBytes());
                assertEquals(777, harness.breaker.getUsed());
                long beforeSecondClose = harness.breaker.getUsed();
                // Call the production owner's close method directly; no release-once wrapper intercepts this call.
                harness.source.close();
                assertEquals(0, beforeSecondClose - harness.breaker.getUsed());
                assertEquals(777, harness.breaker.getUsed());
                assertEquals(0, memory.currentBytes());
            } finally {
                harness.source.close();
                harness.breaker.addWithoutBreaking(-777);
            }
            harness.finishDriver();
            harness.assertFinished();
        }
    }

    /** A secondary condition close failure cannot strand scoped admissions or replace a real admission failure. */
    public void testBudgetPreservesPrimaryFailureWhenCleanupThrows() {
        try (var harness = new Harness(true, true)) {
            harness.source.close();
            var cleanup = new IllegalStateException("secondary profiled condition cleanup");
            try (
                var source = new SequenceSourceOperator(
                    harness.operatorFactory,
                    harness.context,
                    TimeValue.timeValueMinutes(2),
                    latestConfig(harness.sequence),
                    () -> {
                        throw cleanup;
                    }
                )
            ) {
                SequenceMemoryBudget memory = sourceBudget(source);
                assertNull(source.getOutput());
                harness.client.take(EsqlSearchShardsAction.TYPE).listener.onResponse(shards("a"));
                long limit = harness.breaker.getLimit();
                double overhead = harness.breaker.getOverhead();
                harness.breaker.setLimitAndOverhead(harness.breaker.getUsed(), 1);
                try {
                    CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, source::getOutput);
                    assertEquals(1, failure.getSuppressed().length);
                    assertSame(cleanup, failure.getSuppressed()[0]);
                    assertEquals(0, memory.currentBytes());
                    assertEquals(0, harness.breaker.getUsed());
                } finally {
                    harness.breaker.setLimitAndOverhead(limit, overhead);
                }
            }
        }
    }

    /** Collected output and bound input remain Source-held after close until the delayed terminal callback cleans them. */
    public void testBudgetOwnsPagesUntilDeferredStageCleanup() {
        try (var harness = new Harness(true, true)) {
            SequenceMemoryBudget memory = sourceBudget(harness.source);
            harness.deferStage = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertNull(harness.source.getOutput());
            assertNotNull(harness.deferredStage);
            assertTrue(memory.currentBytes() > 0);
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            assertTrue("the late callback still owns its bound/collected data", memory.currentBytes() > 0);
            harness.deferredStage.onResponse(null);
            assertEquals(0, memory.currentBytes());
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertEquals(memory.peakBytes(), harness.source.status().runtime().peakBytesHeld());
        }
    }

    /** Returning a Page ends Source attribution while preserving its original factory and downstream breaker ownership. */
    public void testBudgetResultHandoffPreservesRequestOwnershipWithProfilingOnAndOff() {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key), (FROM b | KEEP key) | KEEP key | LIMIT 1
            """);
        for (boolean profile : List.of(false, true)) {
            try (var harness = new Harness(true, null, null, sequence, null, null, false, DriverContext.WarningsMode.COLLECT, profile)) {
                SequenceMemoryBudget memory = sourceBudget(harness.source);
                assertNotNull(memory);
                assertEquals(profile, harness.source.status().runtime() != null);
                harness.prepareOpen().listener.onResponse(openResponse());
                Page result = null;
                try {
                    for (int attempt = 0; attempt < 30 && result == null; attempt++) {
                        result = harness.source.getOutput();
                        if (result == null && harness.client.pending.isEmpty() == false) {
                            var pending = harness.client.take(TransportSearchAction.TYPE);
                            var request = (SearchRequest) pending.request;
                            int stage = request.getPointInTimeAuthorizationScope().indices()[0].equals("a") ? 0 : 1;
                            harness.respond(pending, stage);
                        }
                    }
                    assertNotNull(result);
                    assertEquals(1, result.getPositionCount());
                    for (int column = 0; column < result.getBlockCount(); column++) {
                        BlockFactory output = result.getBlock(column).blockFactory();
                        assertEquals(harness.blocks.maxPrimitiveArrayBytes(), output.maxPrimitiveArrayBytes());
                        assertEquals(harness.blocks.bytesRefRamOverestimateThreshold(), output.bytesRefRamOverestimateThreshold());
                    }
                    harness.finishDriver();
                    assertEquals(0, memory.currentBytes());
                    assertTrue(memory.peakBytes() > 0);
                    if (profile) {
                        assertEquals(memory.peakBytes(), harness.source.status().runtime().peakBytesHeld());
                    }
                    assertTrue("the unclosed outgoing Page keeps its real breaker reservation", harness.breaker.getUsed() > 0);
                    harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                } finally {
                    Releasable owned = result;
                    if (owned != null) {
                        owned.close();
                    }
                }
                harness.assertFinished();
            }
        }
    }

    /** An append failure releases the rejected Page and preserves all earlier admitted ownership. */
    public void testStagePageAppendFailureRefundsAdmissionAtomically() throws Exception {
        for (boolean appendBeforeThrow : List.of(false, true)) {
            try (var harness = new Harness(true, true)) {
                SequenceMemoryBudget budget = sourceBudget(harness.source);
                long before = budget.currentBytes();
                long breakerBefore = harness.breaker.getUsed();
                Error primary = new AssertionError("controlled collector append failure");
                Class<?> type = Class.forName(SequenceSourceOperator.class.getName() + "$StagePages");
                var constructor = type.getDeclaredConstructor(SequenceSourceOperator.class);
                constructor.setAccessible(true);
                Object collector = constructor.newInstance(harness.source);
                var pagesField = type.getDeclaredField("pages");
                pagesField.setAccessible(true);
                List<Page> pages = rejectingAppend(primary, appendBeforeThrow);
                pagesField.set(collector, pages);
                var add = type.getDeclaredMethod("add", Page.class);
                add.setAccessible(true);
                try (Releasable ignored = (Releasable) collector) {
                    Page accepted = new Page(harness.blocks.newConstantBytesRefBlockWith(new BytesRef("accepted"), 1));
                    add.invoke(collector, accepted);
                    long acceptedBudget = budget.currentBytes();
                    long acceptedBreaker = harness.breaker.getUsed();
                    Page rejected = new Page(harness.blocks.newConstantBytesRefBlockWith(new BytesRef("rejected"), 1));
                    InvocationTargetException failure = expectThrows(
                        InvocationTargetException.class,
                        () -> add.invoke(collector, rejected)
                    );
                    assertSame(primary, failure.getCause());
                    assertEquals(List.of(accepted), pages);
                    assertEquals(acceptedBudget, budget.currentBytes());
                    assertEquals(acceptedBreaker, harness.breaker.getUsed());
                }
                assertEquals(before, budget.currentBytes());
                assertEquals(breakerBefore, harness.breaker.getUsed());
                harness.finishDriver();
                harness.assertFinished();
                assertEquals(0, budget.currentBytes());
            }
        }
    }

    /** Profiling cannot change source admission or permit output before this first-page budget rejection. */
    public void testSameMemoryLimitWithProfilingOnAndOffFailsBeforeAnyOutput() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM a | KEEP key,payload), (FROM b | KEEP key) | KEEP key,payload | LIMIT 1
            """);
        Long wanted = null;
        for (boolean profile : List.of(false, true)) {
            try (
                var harness = new Harness(
                    true,
                    null,
                    null,
                    sequence,
                    null,
                    null,
                    false,
                    DriverContext.WarningsMode.COLLECT,
                    profile,
                    65_536
                )
            ) {
                harness.prepareLatestOpen();
                assertNull(harness.source.getOutput());
                var pending = harness.client.take(TransportSearchAction.TYPE);
                deliverLatest(pending, latestPage("a", 0, List.of(new LatestRow(0, 0, "shared", List.of("x".repeat(80_000))))));
                CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, harness.source::getOutput);
                assertThat(failure.getMessage(), containsString("esql.sequence.max_memory"));
                assertEquals(65_536, failure.getByteLimit());
                assertEquals(0, harness.source.status().rowsEmitted());
                if (wanted == null) {
                    wanted = failure.getBytesWanted();
                } else {
                    assertEquals(wanted.longValue(), failure.getBytesWanted());
                }
                finishLatest(harness);
                assertEquals(0, sourceBudget(harness.source).currentBytes());
            }
        }
    }

    /** A rejected child output is released before its ownership can enter the source collector. */
    public void testForeignStagePageOverBudgetIsReleasedAtAdmission() throws Exception {
        try (var harness = new Harness(true, null, null, null, null, null, false, DriverContext.WarningsMode.COLLECT, false, 16_384)) {
            SequenceMemoryBudget budget = sourceBudget(harness.source);
            long before = budget.currentBytes();
            long requestBefore = harness.breaker.getUsed();
            Class<?> type = Class.forName(SequenceSourceOperator.class.getName() + "$StagePages");
            var constructor = type.getDeclaredConstructor(SequenceSourceOperator.class);
            constructor.setAccessible(true);
            Object collector = constructor.newInstance(harness.source);
            var add = type.getDeclaredMethod("add", Page.class);
            add.setAccessible(true);
            try (Releasable ignored = (Releasable) collector) {
                Page page = new Page(harness.blocks.newConstantBytesRefBlockWith(new BytesRef("x".repeat(32_768)), 1));
                InvocationTargetException failure = expectThrows(InvocationTargetException.class, () -> add.invoke(collector, page));
                assertTrue(failure.getCause() instanceof CircuitBreakingException);
                assertThat(failure.getCause().getMessage(), containsString("esql.sequence.max_memory"));
                assertEquals(before, budget.currentBytes());
                assertEquals(requestBefore, harness.breaker.getUsed());
            }
            harness.finishDriver();
            harness.assertFinished();
            assertEquals(0, budget.currentBytes());
        }
    }

    /** Only container append is synthetic; the Page, blocks, reservations, ledger and Source are real. */
    private static <E> List<E> rejectingAppend(Error failure, boolean appendBeforeThrow) {
        return new ArrayList<>() {
            private int calls;

            @Override
            public boolean add(E value) {
                if (++calls == 1) {
                    return super.add(value);
                }
                if (appendBeforeThrow) {
                    super.add(value);
                }
                throw failure;
            }
        };
    }

    /** Read every MV entry so provenance checks never silently discard later selected members. */
    private static void assertProvenance(Page page, int row, int channel, List<SequenceProvenance.Member> expected) {
        assertEquals(expected.size(), page.<BytesRefBlock>getBlock(channel).getValueCount(row));
        assertEquals(expected, SequenceProvenance.decode(strings(page.getBlock(channel), row)));
    }

    /** Inspect cleanup of unpublished current state without adding a test-only accessor to the production source. */
    private static SequenceMemoryBudget sourceBudget(SequenceSourceOperator source) {
        try {
            var field = SequenceSourceOperator.class.getDeclaredField("memoryBudget");
            field.setAccessible(true);
            return (SequenceMemoryBudget) field.get(source);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
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
            try (var harness = new Harness(false, null, null, null, filter)) {
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

    /** A search extends the fixed lease without introducing another background owner. */
    public void testEachSearchExtendsFiveMinuteLeaseWithoutHeartbeat() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(false, queue)) {
            var open = harness.prepareOpen();
            assertEquals(TimeValue.timeValueMinutes(5), ((OpenPointInTimeRequest) open.request).keepAlive());
            open.listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            assertEquals(TimeValue.timeValueMinutes(5), ((SearchRequest) search.request).source().pointInTimeBuilder().getKeepAlive());
            queue.runTasksUpToTimeInOrder(TimeValue.timeValueMinutes(40).millis());
            assertTrue("an idle search must not schedule heartbeat requests", harness.client.pending.isEmpty());
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            search.listener.onFailure(new IllegalStateException("search cancelled"));
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** Removing renewal must preserve the stage's ownership and blocked listener during a long pause. */
    public void testStageWaitKeepsOwnershipWithoutBackgroundHeartbeat() {
        var queue = new DeterministicTaskQueue();
        try (var harness = new Harness(true, queue)) {
            harness.deferStage = 1;
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            harness.respond(harness.client.take(TransportSearchAction.TYPE), 0);
            assertNull(harness.source.getOutput());
            var blocked = harness.source.isBlocked().listener();
            queue.runTasksUpToTimeInOrder(TimeValue.timeValueMinutes(40).millis());
            assertFalse(blocked.isDone());
            assertTrue(harness.client.pending.isEmpty());
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            harness.deferredStage.onResponse(null);
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testLatePitOpenClosesSnapshotAfterDriverContextFinish() {
        try (var harness = new Harness()) {
            var open = harness.prepareOpen();
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            open.listener.onResponse(openResponse());
            assertTrue("close dispatch releases the driver without awaiting the response", harness.done.isDone());
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
                assertTrue(harness.done.isDone());
            }
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    public void testLateSearchFailureReleasesOwnershipAfterCloseDispatch() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            assertNull(harness.source.getOutput());
            var search = harness.client.take(TransportSearchAction.TYPE);
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            search.listener.onFailure(new IllegalStateException("search failed after cancellation"));
            var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
            assertEquals(new BytesArray("snapshot"), ((ClosePointInTimeRequest) close.request).getId());
            harness.assertFinished();
            close.listener.onResponse(new ClosePointInTimeResponse(true, 2));
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    /** A hung cleanup response must own neither the query nor another close attempt. */
    public void testPitCloseIsSentOnceWithoutWaitingForCallback() {
        try (var harness = new Harness()) {
            harness.prepareOpen().listener.onResponse(openResponse());
            harness.finishDriver();
            var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
            try {
                harness.assertFinished();
                for (var field : close.listener.getClass().getDeclaredFields()) {
                    assertTrue("cleanup listener must have no query state", java.lang.reflect.Modifier.isStatic(field.getModifiers()));
                }
                harness.source.finish();
                harness.source.close();
                assertTrue(harness.client.pending.isEmpty());
            } finally {
                close.listener.onResponse(new ClosePointInTimeResponse(true, 2));
            }
        }
    }

    /** Failure and incomplete-response diagnostics must not expose the token, exception text or stack. */
    public void testPitCloseDiagnosticsNeverIncludeTokenOrExceptionText() {
        Exception failure = new IllegalStateException("raw private details for snapshot");
        for (int depth = 0; depth < 12; depth++) {
            failure = new RemoteTransportException("wrapper containing snapshot", failure);
        }
        for (boolean incomplete : List.of(false, true)) {
            try (var harness = new Harness(); var log = MockLog.capture(SequenceSourceOperator.class, ExceptionsHelper.class)) {
                log.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "safe cleanup diagnostic",
                        SequenceSourceOperator.class.getCanonicalName(),
                        Level.WARN,
                        "Could not confirm complete PIT cleanup for SEQUENCE query; reason ["
                            + (incomplete
                                ? "incomplete_response], exception type [none]"
                                : "close_failure], exception type [RemoteTransportException]")
                    )
                );
                for (Class<?> loggerClass : List.of(SequenceSourceOperator.class, ExceptionsHelper.class)) {
                    log.addExpectation(
                        new MockLog.UnseenEventExpectation("private token", loggerClass.getCanonicalName(), Level.WARN, "*snapshot*")
                    );
                    log.addExpectation(
                        new MockLog.UnseenEventExpectation("raw cleanup throwable", loggerClass.getCanonicalName(), Level.WARN, "*") {
                            @Override
                            public boolean innerMatch(LogEvent event) {
                                return event.getThrown() != null;
                            }
                        }
                    );
                }
                harness.prepareOpen().listener.onResponse(openResponse());
                harness.finishDriver();
                var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
                if (incomplete) {
                    close.listener.onResponse(new ClosePointInTimeResponse(false, 0));
                } else {
                    close.listener.onFailure(failure);
                }
                harness.assertFinished();
                assertTrue(harness.client.pending.isEmpty());
                log.assertAllExpectationsMatched();
            }
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

    public void testDefaultKeyFilterProfileDoesNotProfileShardSearch() {
        var executionInfo = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        try (var harness = Harness.withKeyFilter(executionInfo, false, false)) {
            var search = harness.prepareFilteredSearch();
            assertFalse(((SearchRequest) search.request).source().profile());
            assertEquals(
                Map.of("built_searches", 1L, "coordinator_fallbacks", Map.of()),
                executionInfo.queryProfile().sequenceKeyFilterSnapshot()
            );
            harness.respond(search, 1);
            assertFilteredMatchAndFinish(harness);
        }
    }

    public void testIneligibleKeyAtBreakerBoundaryFallsBackWithoutReservation() {
        var executionInfo = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        Sequence computed = plan("""
            sequence by key WITHIN 30 seconds PRIMARY 1
            (from a | eval key=TO_LOWER(key), captured_payload=CONCAT(key,"/captured") | keep key,captured_payload),
            (from b | eval key=TO_LOWER(key) | keep key)
            | keep key,captured_payload | limit 1
            """);
        try (
            var harness = new Harness(
                true,
                null,
                executionInfo,
                computed,
                null,
                null,
                false,
                DriverContext.WarningsMode.COLLECT,
                true,
                PlannerSettings.SEQUENCE_MAX_MEMORY.getDefault(Settings.EMPTY).getBytes(),
                true,
                false,
                false
            )
        ) {
            harness.prepareFirstSearch();
            long used = harness.breaker.getUsed();
            long limit = harness.breaker.getLimit();
            double overhead = harness.breaker.getOverhead();
            org.mockito.Mockito.clearInvocations(harness.breaker);
            harness.breaker.setLimitAndOverhead(used + 2048, 1);
            try {
                assertNull(harness.source.getOutput());
            } finally {
                harness.breaker.setLimitAndOverhead(limit, overhead);
            }
            var search = harness.client.take(TransportSearchAction.TYPE);
            assertFalse(((SearchRequest) search.request).source().query().toString().contains("__esql_sequence_waiting_keys_v1"));
            var profile = executionInfo.queryProfile().sequenceKeyFilterSnapshot();
            harness.respond(search, 1);
            assertFilteredMatchAndFinish(harness);
            assertEquals(Map.of("built_searches", 0L, "coordinator_fallbacks", Map.of("non_pushable_key", 1L)), profile);
            org.mockito.Mockito.verify(harness.breaker, org.mockito.Mockito.never())
                .addEstimateBytesAndMaybeBreak(org.mockito.ArgumentMatchers.anyLong(), eq("esql_sequence_key_filter"));
        }
    }

    public void testKeyFilterProfileHeaderFailureStillDispatchesFilteredSearch() {
        var executionInfo = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        try (var harness = Harness.withKeyFilter(executionInfo, true)) {
            var search = harness.prepareFilteredSearch();
            assertFalse(((SearchRequest) search.request).source().profile());
            assertNull(executionInfo.queryProfile().sequenceKeyFilterSnapshot());
            assertNull(harness.threadPool.getThreadContext().getHeader(SequenceKeyFilterShardProfile.REQUEST_HEADER));
            harness.respond(search, 1);
            assertFilteredMatchAndFinish(harness);
        }
    }

    public void testKeyFilterProfileRecordingFailureKeepsRowsAndReleasesResponse() {
        var executionInfo = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        try (var harness = Harness.withKeyFilter(executionInfo, false)) {
            var search = harness.prepareFilteredSearch();
            assertTrue(((SearchRequest) search.request).source().profile());
            assertEquals(1L, executionInfo.queryProfile().sequenceKeyFilterSnapshot().get("built_searches"));
            SearchResponse response = withBrokenShardProfile(eventResponse(1));
            harness.respond(search, 1, response);
            verify(response).getSearchProfileShardResults();
            assertNull(executionInfo.queryProfile().sequenceKeyFilterSnapshot());
            assertFilteredMatchAndFinish(harness);
            assertFalse(response.hasReferences());
        }
    }

    public void testKeyFilterProfileRecordingFailureStillClosesRenewedSnapshotAfterCancellation() {
        var executionInfo = new EsqlExecutionInfo(Predicates.never(), EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        try (var harness = Harness.withKeyFilter(executionInfo, false)) {
            var search = harness.prepareFilteredSearch();
            assertTrue(((SearchRequest) search.request).source().profile());
            AtomicLong usedAtWake = observeSearchWake(harness);
            harness.finishDriver();
            assertFalse(harness.done.isDone());
            assertTrue(harness.client.pending.isEmpty());
            SearchResponse response = withBrokenShardProfile(renewedSearchResponse());
            harness.respond(search, 1, response);
            verify(response).getSearchProfileShardResults();
            assertNull(executionInfo.queryProfile().sequenceKeyFilterSnapshot());
            assertFalse(response.hasReferences());
            assertEquals(0L, usedAtWake.get());
            long filterBytes = harness.keyFilterBytes();
            verify(harness.breaker).addWithoutBreaking(-filterBytes);
            assertSearchFinished(harness, "renewed");
        }
    }

    private static SearchResponse withBrokenShardProfile(SearchResponse response) {
        SearchResponse broken = spy(response);
        doThrow(new IllegalStateException("controlled shard profile recording failure")).when(broken).getSearchProfileShardResults();
        return broken;
    }

    private static void assertFilteredMatchAndFinish(Harness harness) {
        assertNull(harness.source.getOutput());
        try (Page page = harness.source.getOutput()) {
            assertNotNull(page);
            assertEquals(1, page.getPositionCount());
            BytesRefBlock key = page.getBlock(0), value = page.getBlock(1);
            assertEquals("shared", key.getBytesRef(key.getFirstValueIndex(0), new BytesRef()).utf8ToString());
            assertEquals("shared/captured", value.getBytesRef(value.getFirstValueIndex(0), new BytesRef()).utf8ToString());
            assertProvenance(
                page,
                0,
                2,
                List.of(new SequenceProvenance.Member(1, "a", "event-a"), new SequenceProvenance.Member(2, "b", "event-b"))
            );
        }
        assertEquals(2, harness.stageExecutions);
        finishLatest(harness);
    }

    public void testKeyFilterRefundsBeforeResponseAdmissionAndWake() {
        try (var harness = Harness.withKeyFilter()) {
            var search = harness.prepareFilteredSearch();
            long noFilter = harness.breaker.getUsed() - harness.keyFilterBytes();
            AtomicLong usedAtWake = observeSearchWake(harness);
            var response = SearchResponse.emptyResponseBuilder().build();
            try (Releasable ignored = response::decRef) {
                // The response fits exactly once the in-flight snapshot's charge is gone.
                harness.breaker.setLimitAndOverhead(noFilter + 256, 1);
                search.listener.onResponse(response);
                assertEquals(noFilter + 256, usedAtWake.get());
            }
            assertTrue("the published completion owns a response reference", response.hasReferences());
            harness.finishDriver();
            assertFalse(response.hasReferences());
            assertSearchFinished(harness, "snapshot");
        }
    }

    public void testKeyFilterRefundDoesNotAdmitAnOversizedResponse() {
        try (var harness = Harness.withKeyFilter()) {
            var search = harness.prepareFilteredSearch();
            long noFilter = harness.breaker.getUsed() - harness.keyFilterBytes();
            AtomicLong usedAtWake = observeSearchWake(harness);
            var response = renewedSearchResponse();
            try (Releasable ignored = response::decRef) {
                harness.breaker.setLimitAndOverhead(noFilter + 255, 1);
                search.listener.onResponse(response);
                expectThrows(CircuitBreakingException.class, harness.source::getOutput);
                assertTrue(response.hasReferences());
            }
            assertFalse("admission failure must not retain a response reference", response.hasReferences());
            assertEquals(noFilter, usedAtWake.get());
            harness.finishDriver();
            assertSearchFinished(harness, "renewed");
        }
    }

    public void testKeyFilterRefundsOnSynchronousDispatchFailure() {
        try (var harness = Harness.withKeyFilter()) {
            harness.prepareFirstSearch();
            var rejected = new IllegalStateException("controlled search dispatch failure");
            harness.client.searchDispatchFailure = rejected;
            assertNull(harness.source.getOutput());
            assertSame(rejected, expectThrows(IllegalStateException.class, harness.source::getOutput));
            long filterBytes = harness.keyFilterBytes();
            verify(harness.breaker).addWithoutBreaking(-filterBytes);
            harness.finishDriver();
            assertSearchFinished(harness, "snapshot");
        }
    }

    public void testKeyFilterRefundsBeforeCancelledDriverCompletes() {
        for (boolean deliverResponse : List.of(false, true)) {
            try (var harness = Harness.withKeyFilter()) {
                long unrelated = 777;
                harness.breaker.addWithoutBreaking(unrelated);
                try {
                    var search = harness.prepareFilteredSearch();
                    long filterBytes = harness.keyFilterBytes();
                    AtomicLong usedAtWake = observeSearchWake(harness);
                    AtomicLong usedAtDriverCompletion = new AtomicLong(-1);
                    harness.finishDriver();
                    harness.context.waitForAsyncActions(
                        ActionListener.running(() -> usedAtDriverCompletion.set(harness.breaker.getUsed()))
                    );
                    assertFalse(harness.done.isDone());
                    assertEquals(unrelated + filterBytes, harness.breaker.getUsed());
                    assertTrue(harness.client.pending.isEmpty());
                    if (deliverResponse) {
                        var response = renewedSearchResponse();
                        try (Releasable ignored = response::decRef) {
                            search.listener.onResponse(response);
                            assertTrue(response.hasReferences());
                        }
                        assertFalse(response.hasReferences());
                    } else {
                        search.listener.onFailure(new IllegalStateException("late search failure"));
                    }
                    assertEquals(unrelated, usedAtWake.get());
                    assertEquals(unrelated, usedAtDriverCompletion.get());
                    assertEquals(unrelated, harness.breaker.getUsed());
                    verify(harness.breaker).addWithoutBreaking(-filterBytes);
                } finally {
                    harness.breaker.addWithoutBreaking(-unrelated);
                }
                assertSearchFinished(harness, deliverResponse ? "renewed" : "snapshot");
            }
        }
    }

    public void testKeyFilterRefundExceptionPreservesSearchFailureAndRenewedSnapshot() {
        for (boolean deliverResponse : List.of(false, true)) {
            try (var harness = Harness.withKeyFilter()) {
                var search = harness.prepareFilteredSearch();
                long filterBytes = harness.keyFilterBytes();
                long noFilter = harness.breaker.getUsed() - filterBytes;
                var refundFailure = new IllegalStateException("controlled key filter refund failure");
                doAnswer(invocation -> {
                    invocation.callRealMethod();
                    throw refundFailure;
                }).when(harness.breaker).addWithoutBreaking(-filterBytes);
                AtomicLong usedAtWake = observeSearchWake(harness);
                if (deliverResponse) {
                    var response = renewedSearchResponse();
                    try (Releasable ignored = response::decRef) {
                        search.listener.onResponse(response);
                        assertTrue(response.hasReferences());
                    }
                    assertFalse("refund failure must not retain a response reference", response.hasReferences());
                    assertSame(refundFailure, expectThrows(IllegalStateException.class, harness.source::getOutput));
                } else {
                    var primary = new IllegalStateException("controlled search failure");
                    search.listener.onFailure(primary);
                    assertSame(primary, expectThrows(IllegalStateException.class, harness.source::getOutput));
                    assertArrayEquals(new Throwable[] { refundFailure }, primary.getSuppressed());
                }
                assertEquals(noFilter, usedAtWake.get());
                verify(harness.breaker).addWithoutBreaking(-filterBytes);
                harness.finishDriver();
                assertSearchFinished(harness, deliverResponse ? "renewed" : "snapshot");
            }
        }
    }

    private static AtomicLong observeSearchWake(Harness harness) {
        var blocked = harness.source.isBlocked().listener();
        assertFalse(blocked.isDone());
        AtomicLong used = new AtomicLong(-1);
        blocked.addListener(ActionListener.running(() -> used.set(harness.breaker.getUsed())));
        return used;
    }

    private static SearchResponse renewedSearchResponse() {
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
            new BytesArray("renewed"),
            null,
            null
        );
    }

    private static void assertSearchFinished(Harness harness, String snapshot) {
        var close = harness.client.take(TransportClosePointInTimeAction.TYPE);
        assertEquals(new BytesArray(snapshot), ((ClosePointInTimeRequest) close.request).getId());
        close.listener.onResponse(new ClosePointInTimeResponse(true, 2));
        assertEquals(2, harness.source.status().candidateSearchRequests());
        assertEquals(1, harness.stageExecutions);
        assertTrue(harness.client.pending.isEmpty());
        harness.assertFinished();
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

    public void testSynchronousStageCompletionReturnsCapturedOutputWithoutAnotherSearch() {
        try (var harness = new Harness(true)) {
            harness.prepareCompletedStages();
            try (Page page = harness.source.getOutput()) {
                assertNotNull(page);
                assertEquals(1, page.getPositionCount());
                BytesRefBlock key = page.getBlock(0), value = page.getBlock(1);
                assertEquals("shared", key.getBytesRef(key.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                assertEquals("shared/captured", value.getBytesRef(value.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                assertProvenance(
                    page,
                    0,
                    2,
                    List.of(new SequenceProvenance.Member(1, "a", "event-a"), new SequenceProvenance.Member(2, "b", "event-b"))
                );
            }
            assertTrue(harness.client.pending.isEmpty());
            assertEquals(2, harness.stageExecutions);
            finishLatest(harness);
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
            assertEquals(2, firstStageSearches);
            assertEquals(3, harness.stageExecutions);
            assertEquals("one prepared template per used source branch", 2, harness.stagePlans);
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** A failing later member identifies the explicit BY name and still releases its PIT, blocks and breaker reservations. */
    public void testMultivaluedKeyNamesMemberAndKey() {
        Sequence sequence = plan("""
            SEQUENCE BY account WITHIN 1 minutes PRIMARY 1
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
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
            assertTrue(harness.client.pending.isEmpty());
        }
    }

    /** A hit at the selected physical coordinate must retain the selected index/ID identity. */
    public void testRetainedSelectionAndOriginalIdsNeedNoPayloadSearch() {
        Sequence sequence = plan("""
            sequence by key WITHIN 30 seconds PRIMARY 1
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
                        for (int column = 0; column < 5; column++) {
                            BytesRefBlock block = page.getBlock(column);
                            actual.add(block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                        }
                        assertEquals(List.of("shared", "event-a", "a", "event-b", "b"), actual);
                        assertEquals(6, page.getBlockCount());
                        assertProvenance(
                            page,
                            0,
                            5,
                            List.of(new SequenceProvenance.Member(1, "a", "event-a"), new SequenceProvenance.Member(2, "b", "event-b"))
                        );
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
            harness.finishDriver();
            harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
            harness.assertFinished();
        }
    }

    /** Optimize the complete statement so MV_EXPAND's copy alias and the outer MEMBER demand both participate. */
    public void testOptimizedExpandedAliasPreservesOriginalMemberPayload() {
        assertOptimizedStagePayload(
            "EVAL expanded = payload | MV_EXPAND expanded | KEEP key, payload",
            "payload",
            List.of("left", "right"),
            List.of("left", "right")
        );

    }

    /** Projection and extraction preserve demanded values through the real optimizer and first-read capture. */
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
            assertOptimizedStagePayload(shape[0], shape[1], List.of("LEFT"), List.of(shape[2]));

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
            assertOptimizedStagePayload(commands, "payload", List.of("LEFT", "right"), List.of("LEFT", "right"));

        }
    }

    /** Demand order can differ from key order without changing the selected values. */
    public void testDemandedOutputKeepsItsOwnChannelLayout() {
        assertOptimizedStagePayload("EVAL payload = TO_LOWER(payload) | KEEP payload, key", "payload", List.of("LEFT"), List.of("left"));

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
            () -> SequenceStageInput.selection(stage, List.of(wrongType), unused -> {}, 2).retained(0)
        ).getMessage();
        assertThat(message, containsString("SEQUENCE stage [2] output changed type of column [lower#"));
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

    private void assertOptimizedStagePayload(String commands, String member, List<String> inputValues, List<String> expectedValues) {
        Sequence sequence = optimizedPlan(Strings.format("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY origin
            (FROM a | KEEP key) AS origin,
            (FROM b | %s) AS execution
            | EVAL original = MEMBER(execution, %s)
            | KEEP key, original | LIMIT 1
            """, commands, member));
        try (var harness = new Harness(true, null, null, sequence)) {
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
            sequence by key WITHIN 30 seconds PRIMARY 1
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
                            for (int column = 0; column < 5; column++) {
                                BytesRefBlock block = page.getBlock(column);
                                actual.add(block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString());
                            }
                            assertEquals(List.of("shared", "event-a", "a", "event-b", "b"), actual);
                            assertEquals(6, page.getBlockCount());
                            assertProvenance(
                                page,
                                0,
                                5,
                                List.of(new SequenceProvenance.Member(1, "a", "event-a"), new SequenceProvenance.Member(2, "b", "event-b"))
                            );
                        }
                    }
                    if (searches < 2 && harness.client.pending.isEmpty() == false) {
                        harness.respond(harness.client.take(TransportSearchAction.TYPE), searches++);
                    }
                }
                assertTrue("the bounded fixture must finish", harness.source.isFinished());
                assertEquals(limit == 999_999_999 ? 0 : 1, rows);
                assertEquals(2, searches);
                harness.finishDriver();
                harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                harness.assertFinished();
                assertTrue(harness.client.pending.isEmpty());
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

    public void testCandidateRequestUsesLargerContinuationPage() {
        CandidatePaging paging = readCandidatePages(257);
        assertEquals(List.of(256, 1024), paging.requestSizes());
    }

    public void testFullCandidateContinuationKeepsCursorOpen() {
        CandidatePaging paging = readCandidatePages(1281);
        assertEquals(List.of(256, 1024, 1), paging.hitCounts());
    }

    public void testShortCandidateContinuationReturnsCompletingEventAndClosesCursor() {
        CandidatePaging paging = readCandidatePages(513);
        assertEquals(List.of(256, 257), paging.hitCounts());
        assertEquals(1, paging.rows().size());
        List<List<String>> row = paging.rows().getFirst();
        assertEquals(List.of("selected"), row.getFirst());
        assertEquals(
            List.of(new SequenceProvenance.Member(1, "a", "event-a-0"), new SequenceProvenance.Member(2, "b", "event-b-512")),
            SequenceProvenance.decode(row.get(1))
        );
    }

    private record CandidatePaging(List<Integer> requestSizes, List<Integer> hitCounts, List<List<List<String>>> rows) {}

    private CandidatePaging readCandidatePages(int laterHitCount) {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 10 seconds PRIMARY 1
            (FROM a | KEEP key), (FROM b | KEEP key) | KEEP key | LIMIT 10
            """);
        try (var harness = new Harness(sequence)) {
            // A full continuation is larger than this harness's usual 1 MiB request breaker.
            harness.breaker.setLimitAndOverhead(4L << 20, harness.breaker.getOverhead());
            harness.prepareLatestOpen();
            List<Integer> requestSizes = new ArrayList<>();
            List<Integer> hitCounts = new ArrayList<>();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                if (index.equals("a")) {
                    // The second start stays unmatched, so completion does not by itself end the reader.
                    return latestPage(
                        "a",
                        0,
                        List.of(new LatestRow(0, 0, "selected", List.of()), new LatestRow(1, 1, "waiting", List.of()))
                    );
                }
                Object[] after = request.source().searchAfter();
                int first = after == null ? 0 : ShardDocSortField.decodeDoc(((Number) after[1]).longValue()) + 1;
                assertEquals(hitCounts.stream().mapToInt(Integer::intValue).sum(), first);
                int size = request.source().size();
                int last = Math.min(first + size, laterHitCount);
                List<LatestRow> hits = new ArrayList<>();
                for (int doc = first; doc < last; doc++) {
                    String key = doc == laterHitCount - 1 ? "selected" : "no-start";
                    hits.add(new LatestRow(doc + 2, doc, key, List.of()));
                }
                requestSizes.add(size);
                hitCounts.add(hits.size());
                return latestPage("b", 1, hits);
            });
            finishLatest(harness);
            return new CandidatePaging(requestSizes, hitCounts, rows);
        }
    }

    private void assertRawTimestampPages(long startTimestamp, long endTimestamp, DataType timestampType) {
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 2 milliseconds PRIMARY 1
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
                        assertTrue(query, query.contains(SequenceSourceQueryBuilder.NAME));
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
        Sequence sequence = plan("""
            SEQUENCE BY key WITHIN 10 milliseconds PRIMARY 1
            (FROM a | MV_EXPAND key | EVAL accepted=LENGTH(key)+LENGTH(payload) | WHERE accepted==7 | KEEP key),
            (FROM b | KEEP key) | KEEP key | LIMIT 10
            """);
        try (var harness = new Harness(sequence, latestConfig(sequence))) {
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
    public void testMandatoryAndOutputValuesAreProjectedTogetherAtFirstRead() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30s PRIMARY 1
            (FROM a | EVAL selected=TO_UPPER(payload),demanded=TO_LOWER(payload),late=CONCAT(key,"/late")
              | WHERE selected!="SKIP" | KEEP selected,key,demanded,late),
            (FROM b | KEEP key) | KEEP selected,key,demanded,late | LIMIT 10
            """);
        var stage = (SequenceStage) sequence.children().getFirst();
        Attribute demanded = stageAttribute(sequence, 0, "demanded");
        Attribute optional = stageAttribute(sequence, 0, "late");
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
        assertEquals(List.of(optional), selection.retained(0));
        assertEquals(1, admissions.size());
        assertEquals(List.of("key", "demanded", "late"), selection.output(0).subList(0, 3).stream().map(Attribute::name).toList());
        selection.output(0);
        selection.source(0);
        assertEquals("one lazy projection per source branch", 1, admissions.size());
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
        String changed = expectThrows(IllegalStateException.class, () -> new Harness(sequence, badType)).getMessage();
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
        String lost = expectThrows(IllegalStateException.class, () -> new Harness(removed, missing)).getMessage();
        assertThat(lost, containsString("SEQUENCE stage [1] output lost column [payload#"));
    }

    /** Long middle-stage streams do not move U beyond the last unused stage-1 timestamp. */
    public void testLatestValidLongMiddleTailKeepsStartBound() {
        Sequence sequence = latestThreeStagePlan();
        for (boolean pendingFinal : List.of(false, true)) {
            try (var harness = new Harness(sequence, latestConfig(sequence))) {
                harness.prepareLatestOpen();
                AtomicInteger middleReads = new AtomicInteger();
                List<List<List<String>>> rows = drainLatest(harness, request -> {
                    String index = request.getPointInTimeAuthorizationScope().indices()[0];
                    if (index.equals("a")) {
                        var start = new LatestRow(0, 0, "shared", List.of());
                        return latestPage("a", 0, pendingFinal ? List.of(start, new LatestRow(0, 1, "other", List.of())) : List.of(start));
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
                    // This key has a start but no middle, so neither arm can complete a match.
                    return latestPage("c", 2, pendingFinal ? List.of(new LatestRow(9_000, 0, "other", List.of())) : List.of());
                });
                assertTrue(rows.isEmpty());
                assertEquals(pendingFinal ? 2 : 1, middleReads.get());
                assertEquals(pendingFinal ? 4L : 3L, harness.source.status().candidateSearchRequests());
                finishLatest(harness);
            }
        }
    }

    /** Used rows retain ring slots/high-water state but cannot keep fetching a middle-stage tail. */
    public void testLatestValidUsedStartMarkersStopReads() {
        Sequence sequence = latestThreeStagePlan();
        try (var harness = new Harness(sequence, latestConfig(sequence))) {
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
        try (var harness = new Harness(sequence, latestConfig(sequence))) {
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
            SEQUENCE BY key WITHIN 10 milliseconds PRIMARY 1
            (FROM a | KEEP key),
            (FROM b | EVAL accepted=TO_LOWER(payload) | WHERE accepted=="keep" | KEEP key),
            (FROM c | KEEP key) | KEEP key | LIMIT 10
            """);
        try (var harness = new Harness(sequence, latestConfig(sequence))) {
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

    /** Mandatory-only inputs join the first-read projection without loading unused KEEP values. */
    public void testMandatoryOnlyInputLoadsItsDemandWithoutUnusedSource() {
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
        var optionalOutput = new SequenceStageInput(stage, sequence.sourceAttributes());
        assertEquals(List.of("key"), optionalOutput.source(0).output().stream().map(Attribute::name).toList());
        var config = SequenceLatestValidConfig.defaults(
            List.of(List.of(new SequenceLatestValidConfig.ValueDemand(stageAttribute(sequence, 0, "visible"))), List.of()),
            List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), selected -> {
                assertEquals(List.of("left"), strings(selected.block(0, 0), 0));
                return true;
            }))
        );
        try (var harness = new Harness(sequence, config)) {
            harness.prepareLatestOpen();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                assertEquals("this key-only fixture must not perform payload hydration", 2, request.source().sorts().size());
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                Map<String, Object> query = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    Strings.toString(request.source().query()),
                    false
                );
                assertTrue(query.get(SequenceSourceQueryBuilder.NAME) instanceof Map<?, ?>);
                Map<?, ?> guard = (Map<?, ?>) query.get(SequenceSourceQueryBuilder.NAME);
                List<String> expected = index.equals("a") ? List.of("@timestamp", "key", "payload") : List.of("@timestamp", "key");
                assertEquals(List.of(index), guard.get("indices"));
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
            finishLatest(harness);
        }
    }

    /** Mandatory cells and output cells remain independent through EVAL, KEEP and RENAME. */
    public void testMandatoryAndOutputValuesThroughEvalKeepAndRename() {
        for (boolean direct : List.of(false, true)) {
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
            try (var harness = new Harness(sequence, config)) {
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
                finishLatest(harness);
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
        try (var harness = new Harness(sequence, config)) {
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
            finishLatest(harness);
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
            try (var harness = new Harness(sequence, config)) {
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

    /** First-read output capture must release its producing page when copying the retained cell trips the breaker. */
    public void testE5RetainedOutputCopyBreakerFailureReleasesStagePage() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 30 seconds PRIMARY 1
            (FROM a | KEEP key,payload), (FROM b | KEEP key) | KEEP key,payload | LIMIT 1
            """);
        var config = SequenceLatestValidConfig.defaults(List.of(List.of(), List.of()), List.of());
        try (var harness = new Harness(sequence, config)) {
            harness.prepareLatestOpen();
            assertNull(harness.source.getOutput());
            var pending = harness.client.take(TransportSearchAction.TYPE);
            deliverLatest(pending, latestPage("a", 0, List.of(new LatestRow(0, 0, "shared", List.of("x".repeat(65_536))))));
            assertNull(harness.source.getOutput());
            assertEquals(1, harness.stageExecutions);
            long limit = harness.breaker.getLimit();
            harness.breaker.setLimitAndOverhead(harness.breaker.getUsed() + 4096, 1);
            try {
                CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, harness.source::getOutput);
                assertTrue(
                    "failure must reach retained output capture",
                    java.util.Arrays.stream(failure.getStackTrace())
                        .anyMatch(
                            frame -> frame.getClassName().equals(SequenceSourceOperator.class.getName())
                                && frame.getMethodName().equals("handle")
                        )
                );
            } finally {
                harness.breaker.setLimitAndOverhead(limit, 1);
            }
            finishLatest(harness);
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
        try (var harness = new Harness(sequence, config)) {
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

    /** Captured output stays available after a later merged row expires the corresponding matcher record. */
    public void testCapturedOutputSurvivesUsedRecordExpiry() {
        Sequence sequence = optimizedPlan("""
            SEQUENCE BY key WITHIN 10 seconds PRIMARY 1
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
        try (var harness = new Harness(sequence, config)) {
            harness.prepareLatestOpen();
            List<List<List<String>>> rows = drainLatest(harness, request -> {
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                assertEquals(2, request.source().sorts().size());
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
            for (List<List<String>> row : rows) {
                assertEquals(List.of("value"), sourceCell(sequence, row, 0, "visible"));
                assertEquals(List.of("shared/late"), sourceCell(sequence, row, 0, "late"));
            }
            finishLatest(harness);
        }
    }

    /** Factory equality includes the captured per-query budget, while the source description remains stable. */
    public void testLatestValidFactorySelectionPreservesDefaultRecordContract() {
        Sequence sequence = latestThreeStagePlan();
        try (var harness = new Harness(sequence, latestConfig(sequence))) {
            var factory = harness.operatorFactory;
            assertEquals(10, SequenceSourceOperator.Factory.class.getRecordComponents().length);
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
            SEQUENCE BY key WITHIN 10 seconds PRIMARY 1
            (FROM a | KEEP key), (FROM b | KEEP key), (FROM c | KEEP key) | KEEP key | LIMIT 10
            """);
    }

    /** Own WHERE values stay private while MEMBER continues to name the earlier stage's final processed output. */
    public void testCompiledConditionsKeepOwnValuesThroughTrailingProjections() {
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
            try (var harness = new Harness(sequence)) {
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
                finishLatest(harness);
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
        try (var harness = new Harness(sequence)) {
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
                    () -> new SequenceSourceOperator(harness.operatorFactory, harness.context, TimeValue.timeValueMinutes(2), config, null)
                );
                expectThrows(
                    IllegalArgumentException.class,
                    () -> new SequenceSourceOperator(harness.operatorFactory, harness.context, TimeValue.timeValueMinutes(2), config, null)
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
            assertNotNull(harness.source.status().latestValidCounters());
        }

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
                        config,
                        cleanup
                    )
                );
                assertThat(failure.getMessage(), containsString("[esql_sequence_reader]"));
                assertEquals(1, failure.getSuppressed().length);
                assertEquals("secondary condition cleanup failure", failure.getSuppressed()[0].getMessage());
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

    /** A real scope-admission breaker failure remains primary when terminal condition cleanup also fails. */
    public void testGetOutputKeepsPrimaryFailureAndSuppressesConditionCleanup() {
        try (var harness = new Harness()) {
            harness.source.close();
            var cleanup = new IllegalStateException("secondary output cleanup failure");
            AtomicInteger closes = new AtomicInteger();
            try (
                var source = new SequenceSourceOperator(
                    harness.operatorFactory,
                    harness.context,
                    TimeValue.timeValueMinutes(2),
                    latestConfig(harness.sequence),
                    () -> {
                        closes.incrementAndGet();
                        throw cleanup;
                    }
                )
            ) {
                assertNull(source.getOutput());
                harness.client.take(EsqlSearchShardsAction.TYPE).listener.onResponse(shards("a"));
                long limit = harness.breaker.getLimit();
                double overhead = harness.breaker.getOverhead();
                harness.breaker.setLimitAndOverhead(harness.breaker.getUsed(), 1);
                try {
                    CircuitBreakingException failure = expectThrows(CircuitBreakingException.class, source::getOutput);
                    assertEquals(1, failure.getSuppressed().length);
                    assertSame(cleanup, failure.getSuppressed()[0]);
                    assertEquals(1, closes.get());
                    assertEquals(0, harness.breaker.getUsed());
                } finally {
                    harness.breaker.setLimitAndOverhead(limit, overhead);
                }
            }
        }
    }

    /** Empty input reaches finish inside getOutput, so a terminal cleanup failure must survive the finished flag. */
    public void testEmptyInputKeepsTerminalCleanupFailureInsteadOfThrowingNull() {
        try (var harness = new Harness()) {
            harness.source.close();
            var primary = new IllegalStateException("terminal condition cleanup failure");
            try (
                var source = new SequenceSourceOperator(
                    harness.operatorFactory,
                    harness.context,
                    TimeValue.timeValueMinutes(2),
                    latestConfig(harness.sequence),
                    () -> {
                        throw primary;
                    }
                )
            ) {
                for (String index : List.of("a", "b")) {
                    assertNull(source.getOutput());
                    harness.client.take(EsqlSearchShardsAction.TYPE).listener.onResponse(shards(index));
                }
                assertNull(source.getOutput());
                harness.client.take(TransportOpenPointInTimeAction.TYPE).listener.onResponse(openResponse());
                assertNull(source.getOutput());
                harness.respond(harness.client.take(TransportSearchAction.TYPE), 0, SearchResponse.emptyResponseBuilder().build());
                try {
                    assertSame(primary, expectThrows(IllegalStateException.class, source::getOutput));
                } finally {
                    harness.client.take(TransportClosePointInTimeAction.TYPE).listener.onResponse(new ClosePointInTimeResponse(true, 2));
                }
                assertEquals(0, harness.breaker.getUsed());
                assertTrue(harness.client.pending.isEmpty());
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
            try (var harness = new Harness(true, null, null, sequence, null, config, false, mode)) {
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
                        ? List.of("SEQUENCE dropped [127] candidates at the limit of [1] per stage and key; matches may be missing")
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
            (FROM d | WHERE TO_LONG(payload) == TO_LONG(MEMBER(1,payload)) | KEEP key)
            | KEEP key,payload | LIMIT 10
            """);
        try (var harness = new Harness(sequence, true)) {
            harness.prepareLatestOpen();
            List<List<List<String>>> result = drainLatest(harness, request -> {
                assertTrue("summaries and condition warnings wait for terminal closure", harness.observedWarnings.isEmpty());
                String index = request.getPointInTimeAuthorizationScope().indices()[0];
                int stage = index.charAt(0) - 'a';
                int perKey = stage == 0 ? 65 : 64;
                List<LatestRow> rows = new ArrayList<>();
                for (int key = 0; key < 2; key++) {
                    if (stage == 3) {
                        rows.add(new LatestRow(386 + key, key, "key" + key, List.of("1")));
                    } else {
                        for (int row = 0; row < perKey; row++) {
                            long time = stage == 0 ? key * 65L + row : 130 + (stage - 1) * 128L + key * 64L + row;
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
            assertEquals(64, counters.largestRingFill());
            assertEquals(21, harness.observedWarnings.stream().filter(warning -> warning.startsWith("SEQUENCE ") == false).count());
            assertEquals(
                List.of(
                    "SEQUENCE dropped [2] candidates at the limit of [64] per stage and key; matches may be missing",
                    "SEQUENCE abandoned [2] searches at the work limit of [131072] steps; matches may be missing"
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
                if (range.get("lte") instanceof String upper && java.time.Instant.parse(expected).equals(java.time.Instant.parse(upper))) {
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
        hit.setDocumentField(new DocumentField("@timestamp", List.of(timestamp)));
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
            hit.setDocumentField(new DocumentField("@timestamp", List.of(timestamp.toString())));
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
                hit.setDocumentField(new DocumentField("@timestamp", List.of(timestamp.toString())));
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

    /** Lifecycle tests demand a computed output cell so capture and terminal cleanup both own real values. */
    private static Sequence computedOutputPlan() {
        return plan("""
            sequence by key WITHIN 30 seconds PRIMARY 1
            (from a | eval captured_payload=CONCAT(key,"/captured") | keep key,captured_payload), (from b | keep key)
            | keep key,captured_payload | limit 1
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

        private Harness(boolean realStages, boolean profile) {
            this(realStages, null, null, null, null, null, false, DriverContext.WarningsMode.COLLECT, profile);
        }

        private static Harness withKeyFilter() {
            return withKeyFilter(null, false);
        }

        private static Harness withKeyFilter(EsqlExecutionInfo executionInfo, boolean duplicateProfileHeader) {
            return withKeyFilter(executionInfo, duplicateProfileHeader, true);
        }

        private static Harness withKeyFilter(EsqlExecutionInfo executionInfo, boolean duplicateProfileHeader, boolean shardProfile) {
            return new Harness(
                true,
                null,
                executionInfo,
                null,
                null,
                null,
                false,
                DriverContext.WarningsMode.COLLECT,
                true,
                PlannerSettings.SEQUENCE_MAX_MEMORY.getDefault(Settings.EMPTY).getBytes(),
                true,
                duplicateProfileHeader,
                shardProfile
            );
        }

        private Harness(boolean realStages, DeterministicTaskQueue queue) {
            this(realStages, queue, null);
        }

        private Harness(boolean realStages, DeterministicTaskQueue queue, EsqlExecutionInfo executionInfo) {
            this(realStages, queue, executionInfo, null);
        }

        private Harness(boolean realStages, DeterministicTaskQueue queue, EsqlExecutionInfo executionInfo, Sequence sequence) {
            this(realStages, queue, executionInfo, sequence, null);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence sequence,
            QueryBuilder filter
        ) {
            this(realStages, queue, executionInfo, sequence, filter, null, false, DriverContext.WarningsMode.COLLECT, false);
        }

        private Harness(Sequence sequence, SequenceLatestValidConfig config) {
            this(true, null, null, sequence, null, config, false, DriverContext.WarningsMode.COLLECT, false);
        }

        private Harness(Sequence sequence) {
            this(sequence, false);
        }

        private Harness(Sequence sequence, boolean stressWorkLimit) {
            this(true, null, null, sequence, null, null, stressWorkLimit, DriverContext.WarningsMode.COLLECT, false);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid,
            boolean stressWorkLimit,
            DriverContext.WarningsMode warningsMode
        ) {
            this(realStages, queue, executionInfo, suppliedSequence, requestFilter, latestValid, stressWorkLimit, warningsMode, false);
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid,
            boolean stressWorkLimit,
            DriverContext.WarningsMode warningsMode,
            boolean profile
        ) {
            this(
                realStages,
                queue,
                executionInfo,
                suppliedSequence,
                requestFilter,
                latestValid,
                stressWorkLimit,
                warningsMode,
                profile,
                org.elasticsearch.xpack.esql.planner.PlannerSettings.SEQUENCE_MAX_MEMORY.getDefault(Settings.EMPTY).getBytes()
            );
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid,
            boolean stressWorkLimit,
            DriverContext.WarningsMode warningsMode,
            boolean profile,
            long memoryLimit
        ) {
            this(
                realStages,
                queue,
                executionInfo,
                suppliedSequence,
                requestFilter,
                latestValid,
                stressWorkLimit,
                warningsMode,
                profile,
                memoryLimit,
                false,
                false,
                false
            );
        }

        private Harness(
            boolean realStages,
            DeterministicTaskQueue queue,
            EsqlExecutionInfo executionInfo,
            Sequence suppliedSequence,
            QueryBuilder requestFilter,
            SequenceLatestValidConfig latestValid,
            boolean stressWorkLimit,
            DriverContext.WarningsMode warningsMode,
            boolean profile,
            long memoryLimit,
            boolean keyFilter,
            boolean duplicateProfileHeader,
            boolean shardProfile
        ) {
            realThreadPool = queue == null ? new TestThreadPool("sequence-callbacks") : null;
            threadPool = queue == null ? realThreadPool : queue.getThreadPool();
            client = new PendingClient(threadPool);
            Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb")
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build();
            CircuitBreaker requestBreaker = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getBreaker(CircuitBreaker.REQUEST);
            breaker = keyFilter ? spy(requestBreaker) : requestBreaker;
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
            sequence = suppliedSequence == null ? computedOutputPlan() : suppliedSequence;
            try (var ignored = threadPool.getThreadContext().stashContext()) {
                threadPool.getThreadContext().putHeader("sequence-test-caller", "owner");
                if (duplicateProfileHeader) {
                    threadPool.getThreadContext().putHeader(SequenceKeyFilterShardProfile.REQUEST_HEADER, "collision");
                }
                var configuration = new ConfigurationBuilder(EsqlTestUtils.TEST_CFG).profile(profile);
                if (keyFilter) {
                    configuration.pragmas(
                        new QueryPragmas(
                            Settings.builder()
                                .put("sequence_key_filter", true)
                                .put("sequence_key_filter_shard_profile", shardProfile)
                                .build()
                        )
                    );
                }
                operatorFactory = new SequenceSourceOperator.Factory(
                    new SequenceExec(sequence.source(), sequence, requestFilter),
                    client,
                    client,
                    null,
                    configuration.build(),
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
                    executionInfo,
                    memoryLimit
                );
                source = (SequenceSourceOperator) (stressWorkLimit ? operatorFactory.getWithConditions(context, 64, null)
                    : latestValid == null ? operatorFactory.get(context)
                    : operatorFactory.get(context, latestValid));
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

        private void prepareFirstSearch() {
            prepareOpen().listener.onResponse(openResponse());
            assertNull(source.getOutput());
            respond(client.take(TransportSearchAction.TYPE), 0);
            assertNull(source.getOutput());
            assertEquals(1, stageExecutions);
            assertTrue(client.pending.isEmpty());
        }

        private Pending<SearchResponse> prepareFilteredSearch() {
            prepareFirstSearch();
            assertNull(source.getOutput());
            var search = client.take(TransportSearchAction.TYPE);
            assertThat(((SearchRequest) search.request).source().query().toString(), containsString("__esql_sequence_waiting_keys_v1"));
            assertTrue(client.pending.isEmpty());
            return search;
        }

        private long keyFilterBytes() {
            var bytes = ArgumentCaptor.forClass(Long.class);
            verify(breaker, atLeastOnce()).addEstimateBytesAndMaybeBreak(bytes.capture(), eq("esql_sequence_key_filter"));
            return bytes.getAllValues().stream().mapToLong(Long::longValue).sum();
        }

        private void prepareCompletedStages() {
            prepareOpen().listener.onResponse(openResponse());
            for (int stage = 0; stage < 2; stage++) {
                assertNull(source.getOutput());
                respond(client.take(TransportSearchAction.TYPE), stage);
                assertNull(source.getOutput());
                assertTrue("synchronous completion is consumed before dispatch", client.pending.isEmpty());
            }
            assertEquals(2, stageExecutions);
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
        private RuntimeException searchDispatchFailure;

        private PendingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
            if (searchDispatchFailure != null) {
                assertEquals("owner", threadPool().getThreadContext().getHeader("sequence-test-caller"));
                assertThat(request.source().query().toString(), containsString("__esql_sequence_waiting_keys_v1"));
                throw searchDispatchFailure;
            }
            super.search(request, listener);
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

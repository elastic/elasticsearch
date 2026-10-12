/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.SubPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SubPlansExecutor}: success, failure unwind, STOP, cancel, lazy sink registration, session-id isolation, and
 * PROFILE wiring. On every path the query listener must complete exactly once and the root local exchange must be deregistered from the
 * exchange service.
 * <p>
 * {@code ComputeService} cannot be constructed in a unit test (it requires {@code TransportService}, {@code SearchService},
 * {@code ClusterService}, etc.), so it is mocked here with Mockito. This is documented as an AGENTS.md "last resort" use of a mock;
 * everything else (exchange service, task, thread pool) is real.
 * <p>
 * Key architectural notes for the new implementation:
 * <ul>
 *   <li>Only the root {@code LocalExchange} is registered in {@code ExchangeService}; nested exchanges are coordinator-private.</li>
 *   <li>Leaf sinks are {@code LocalExchangeSink}s on their parent's {@code LocalExchange}, not {@code ExchangeService} sink handlers so
 *       {@link ExchangeService#sinkKeys()} is always empty in these tests.</li>
 *   <li>The entry point is the constructor (takes {@code rootPlan} and {@code listener}) followed by
 *       {@link SubPlansExecutor#executePlan()}. There is no separate executor parameter; the permit-gated depth first search runs on
 *       whatever thread calls each completion callback.</li>
 * </ul>
 * Default {@code branch_parallel_degree} is 2 unless a test sets it explicitly.
 */
public class SubPlansExecutorTests extends ESTestCase {

    /**
     * ComputeService is mocked because its constructor requires the full node stack.
     */
    private ComputeService computeService;
    private PlannerSettings.Holder plannerSettingsHolder;
    private TestThreadPool threadPool;
    private ExchangeService exchangeService;
    /**
     * Observes whether {@code cancelQueryOnFailure} ran; reset before each test.
     */
    private AtomicBoolean cancelled;

    @Before
    public void setUpRunner() {
        threadPool = new TestThreadPool(getTestName());
        exchangeService = new ExchangeService(
            Settings.EMPTY,
            threadPool,
            ThreadPool.Names.SEARCH,
            TestBlockFactory.getNonBreakingInstance()
        );
        cancelled = new AtomicBoolean();

        plannerSettingsHolder = mock(PlannerSettings.Holder.class);
        when(plannerSettingsHolder.get()).thenReturn(PlannerSettings.DEFAULTS);

        computeService = mock(ComputeService.class);
        when(computeService.cancelQueryOnFailure(any())).thenAnswer(inv -> new RunOnce(() -> cancelled.set(true)));
        when(computeService.plannerSettings()).thenReturn(plannerSettingsHolder);
        var childSessionCounter = new AtomicInteger();
        when(computeService.newChildSession(any())).thenAnswer(inv -> inv.getArgument(0) + "/" + childSessionCounter.incrementAndGet());
        when(computeService.profileDescription(any(), any())).thenAnswer(inv -> inv.getArgument(0) + "." + inv.getArgument(1));
    }

    @After
    public void tearDownRunner() {
        terminate(threadPool);
    }

    // success

    /**
     * Happy path: one merge, two leaves both succeed. The terminal listener fires with a result, cancellation does not fire, and the root
     * exchange is deregistered.
     */
    public void testOneMergeTwoLeafSuccess() throws Exception {
        stubRunComputeSuccess();
        stubExecutePlanSuccess();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), future);

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("root exchange must be deregistered after success", exchangeFullyEmpty());
    }

    // merge failure

    /**
     * Root merge's {@code runCompute} fails asynchronously via {@code listener.onFailure}. The future fails with the injected exception,
     * cancellation fires, and the root exchange is deregistered.
     * <p>
     * Note: {@code runCompute} is documented as non-throwing; failure must be signalled through the listener, not by throwing.
     */
    public void testOneMergeTwoLeafMergeFailureInRunCompute() throws Exception {
        var injected = new RuntimeException("injected planning failure");
        stubExecutePlanSuccess();
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onFailure(injected);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected planning failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after failure", exchangeFullyEmpty());
    }

    // leaf failure

    /**
     * One randomly chosen leaf among eight fails. The remaining seven succeed.
     */
    public void testOneMergeEightLeafRandomLeafFailureInExecutePlan() throws Exception {
        var injected = new RuntimeException("injected leaf failure");
        int failingLeaf = randomIntBetween(0, 7);
        stubRunComputeSuccess();
        stubExecutePlanWithOneFailure(injected, failingLeaf);

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(8), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after leaf failure", exchangeFullyEmpty());
    }

    // nested failure

    /**
     * One randomly chosen inner merge's {@code runCompute} fails via {@code listener.onFailure}. The future fails with the injected
     * exception, cancellation fires, and the root exchange is deregistered.
     */
    public void testNestedMergesRandomFailureInRunCompute() throws Exception {
        var injected = new RuntimeException("injected inner failure");
        int failingCall = randomIntBetween(1, 2); // 0 = root (always succeeds), 1 = innerA, 2 = innerB
        var callCount = new AtomicInteger();
        doAnswer(inv -> {
            int call = callCount.getAndIncrement();
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            if (call == failingCall) {
                listener.onFailure(injected);
                return null;
            }
            ComputeContext context = inv.getArgument(1);
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        stubExecutePlanSuccess();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), not(instanceOf(TaskCancelledException.class)));
        assertEquals("injected inner failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after nested failure", exchangeFullyEmpty());
    }

    /**
     * One randomly chosen leaf among six fails via {@code listener.onFailure}. The other five succeed.
     */
    public void testNestedMergesRandomLeafFailureInExecutePlan() throws Exception {
        var injected = new RuntimeException("injected leaf failure");
        int failingLeaf = randomIntBetween(0, 5);
        stubRunComputeSuccess();
        stubExecutePlanWithOneFailure(injected, failingLeaf);

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after nested leaf failure", exchangeFullyEmpty());
    }

    /**
     * Regression: the cancel runnable returned by {@code cancelQueryOnFailure} must run exactly once, not once per merge node that
     * records a failure.
     */
    public void testNestedMergeFailureCancelsQueryExactlyOnce() {
        var injected = new RuntimeException("injected inner failure");
        var cancelRuns = new AtomicInteger();
        when(computeService.cancelQueryOnFailure(any())).thenAnswer(inv -> new RunOnce(() -> {
            cancelled.set(true);
            cancelRuns.incrementAndGet();
        }));

        var callCount = new AtomicInteger();
        doAnswer(inv -> {
            int call = callCount.getAndIncrement();
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            if (call == 1) { // innerA fails via listener
                listener.onFailure(injected);
                return null;
            }
            ComputeContext context = inv.getArgument(1);
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        stubExecutePlanSuccess();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);

        expectThrows(ExecutionException.class, future::get);
        assertTrue("cancel runnable must have fired", cancelled.get());
        assertEquals("cancel runnable must fire once, not once per merge", 1, cancelRuns.get());
        verify(computeService, times(1)).cancelQueryOnFailure(any());
    }

    // synchronous executePlan failure

    /**
     * {@code executePlan} throws synchronously during the first-wave dispatch. The throw is caught by {@code startLeaf} and routed to
     * failure: no hang, cancellation fires, root exchange deregistered.
     */
    public void testSynchronousExecutePlanFailureFirstWaveFailsCleanly() {
        var injected = new RuntimeException("injected synchronous executePlan failure");
        stubRunComputeSuccess();
        doThrow(injected).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected synchronous executePlan failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after synchronous failure", exchangeFullyEmpty());
    }

    /**
     * {@code executePlan} throws synchronously on the second leaf. With {@code branch_parallel_degree=1}, the first leaf completes and
     * the second is dispatched via the outer {@code SubPlansExecutor#executeNext()} loop. The throw is caught by {@code startLeaf}'s catch
     * block and routed to failure.
     */
    public void testSynchronousExecutePlanFailureOnRefillWaveFailsCleanly() {
        var injected = new RuntimeException("injected refill-wave failure");
        var leafCallCount = new AtomicInteger();
        stubRunComputeSuccess();
        doAnswer(inv -> {
            if (leafCallCount.getAndIncrement() == 0) {
                Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
                sinkSupplier.get().finish();
                ActionListener<Result> listener = inv.getArgument(8);
                Configuration cfg = inv.getArgument(4);
                listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
                return null;
            }
            throw injected;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected refill-wave failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after refill failure", exchangeFullyEmpty());
    }

    // lazy sink registration

    /**
     * Leaf sinks are created lazily at dispatch time via {@code sinkSupplier.get()} inside {@code executePlan}, not eagerly when the tree
     * is built. With {@code branch_parallel_degree=1} and three leaves, only one leaf is dispatched per scheduling round; the other two
     * are not dispatched until each prior leaf completes.
     */
    public void testUndispatchedLeavesHaveNoSinkHandlers() throws Exception {
        stubRunComputeSuccess();
        List<Runnable> pendingCompletions = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            pendingCompletions.add(() -> {
                sinkSupplier.get().finish();
                listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            });
            return null;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(3), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()), future);

        // Only one leaf dispatched so far; the other two are queued.
        assertThat(pendingCompletions, hasSize(1));

        // Completing each leaf synchronously triggers the next dispatch.
        for (int i = 0; i < 3; i++) {
            assertThat("only leaf " + i + " should have been dispatched so far", pendingCompletions, hasSize(i + 1));
            pendingCompletions.get(i).run();
        }

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("root exchange must be deregistered after success", exchangeFullyEmpty());
    }

    // stop / limit

    /**
     * Async STOP via {@link ExchangeService#finishSessionEarly} closes the root exchange, which cascades through nested merge sinks to
     * leaf sinks so all parked stubs unblock. With {@code branch_parallel_degree=8} all six leaves and three merges are in flight when
     * STOP fires.
     */
    public void testFinishSessionEarlyUnblocksParkedNestedTree() throws Exception {
        CountDownLatch parked = new CountDownLatch(3 + 6); // root + innerA + innerB, and six leaves
        stubParkUntilExchangeCloses(parked);
        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 8).build())
        );
        assertTrue("all merges and leaves must park before STOP", parked.await(10, TimeUnit.SECONDS));
        // Only the root exchange is registered in ExchangeService; nested exchanges are coordinator-private.
        assertTrue("root local exchange must be registered", exchangeService.sourceKeys().contains("test-session"));

        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue("STOP must find and finish the root exchange", stopped.get(10, TimeUnit.SECONDS));

        started.future.get(10, TimeUnit.SECONDS);
        assertFalse("STOP is a graceful finish, not a failure", cancelled.get());
        assertBusy(() -> assertTrue("all exchanges must be deregistered after STOP", exchangeFullyEmpty()));
    }

    /**
     * STOP with {@code branch_parallel_degree=1}: five leaves are queued (no sink, not yet dispatched) when {@code finishSessionEarly}
     * runs. Queued leaves must observe {@code execInfo.isStopped()} and skip {@code executePlan} entirely.
     */
    public void testFinishSessionEarlyUnblocksQueuedNestedLeaves() throws Exception {
        CountDownLatch parked = new CountDownLatch(2 + 1); // root + innerA + one dispatched leaf
        AtomicInteger leafDispatches = new AtomicInteger();
        stubParkUntilExchangeClosesOrCancelled(parked, null, leafDispatches);
        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build())
        );
        assertTrue("root, innerA, and the first leaf must park before STOP", parked.await(10, TimeUnit.SECONDS));
        assertEquals("only the first-wave leaf must be dispatched before STOP", 1, leafDispatches.get());

        started.execInfo.markAsStopped();
        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue(stopped.get(10, TimeUnit.SECONDS));

        started.future.get(10, TimeUnit.SECONDS);
        assertEquals("queued leaves must not call executePlan after STOP", 1, leafDispatches.get());
        assertFalse("STOP is a graceful finish, not a failure", cancelled.get());
        assertBusy(() -> assertTrue("all exchanges must be deregistered after STOP", exchangeFullyEmpty()));
    }

    /**
     * STOP that arrives before {@code executePlan()} is called. {@code execInfo.markAsStopped()} is set first; every merge and leaf skips
     * work via {@code outputFinished} and the query terminates without calling {@code runCompute} or {@code executePlan}.
     */
    public void testStopBeforeDispatchReleasesUnstartedNestedMerges() throws Exception {
        // runCompute and executePlan stubs are intentionally not set up: if either is called the mock
        // returns null (void), the listener is never called, and the query hangs — caught by the timeout below.
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        execInfo.markAsStopped();

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()),
            null,
            new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of()),
            execInfo
        );

        started.future.get(10, TimeUnit.SECONDS);
        assertFalse("STOP is a graceful stop, not a failure", cancelled.get());
        verify(computeService, times(0)).runCompute(any(), any(), any(), any(), any(), any(), any());
        verify(computeService, times(0)).executePlan(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any()
        );
        assertBusy(() -> assertTrue("all exchanges must be deregistered after pre-dispatch STOP", exchangeFullyEmpty()));
    }

    /**
     * An inner merge's {@code runCompute} fails asynchronously (via {@code listener.onFailure}) while sibling leaves are still running.
     * The leaves complete successfully after the merge has already recorded a failure. The query must fail cleanly with the merge's
     * exception without hanging or leaking exchanges.
     * <p>
     * With {@code branch_parallel_degree=2} and topology {@code twoMergesThreeLeaves()}: leafA (direct child of root) completes
     * immediately to free a permit for leafC; leafB and leafC (children of inner) are in flight when inner's runCompute listener fires
     * with failure.
     */
    public void testInnerMergeAsyncFailureWithRunningLeaf() throws Exception {
        var injected = new RuntimeException("injected inner-merge async failure");
        AtomicReference<ActionListener<DriverCompletionInfo>> innerMergeListener = new AtomicReference<>();
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            ActionListener<DriverCompletionInfo> delegate = inv.getArgument(6);
            ActionListener<DriverCompletionInfo> listener = ActionListener.notifyOnce(delegate);
            if (context.exchangeSinkSupplier() == null) {
                // Root: succeed synchronously (no parent sink).
                listener.onResponse(DriverCompletionInfo.EMPTY);
            } else {
                // Inner merge: capture the listener so the test can fail it asynchronously.
                innerMergeListener.set(listener);
            }
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        var leafCallCount = new AtomicInteger();
        List<ActionListener<Result>> parkedLeaves = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            int call = leafCallCount.getAndIncrement();
            ActionListener<Result> delegate = inv.getArgument(8);
            ActionListener<Result> listener = ActionListener.notifyOnce(delegate);
            Configuration cfg = inv.getArgument(4);
            if (call == 0) {
                // leafA: succeed immediately to release a permit so leafC gets dispatched.
                listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            } else {
                // leafB and leafC: park without completing.
                parkedLeaves.add(listener);
            }
            return null;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(twoMergesThreeLeaves(), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 2).build()), future);

        // leafA completed immediately; leafB and leafC are parked under the inner merge.
        assertThat("leafB and leafC must be in flight", parkedLeaves, hasSize(2));
        assertNotNull("inner merge's runCompute listener must be captured", innerMergeListener.get());

        // Fail the inner merge asynchronously while its leaves are still running.
        innerMergeListener.get().onFailure(injected);

        // Complete leafB and leafC successfully after the merge has already recorded a failure.
        Configuration cfg = configuration(new QueryPragmas(Settings.EMPTY));
        for (ActionListener<Result> leaf : parkedLeaves) {
            leaf.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
        }

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected inner-merge async failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after inner-merge async failure", exchangeFullyEmpty());
    }

    /**
     * All six leaves succeed with {@code branch_parallel_degree=8} (all dispatched in a single wave). Exercises the full nested tree
     * ({@code threeMergesSixLeaves}) in a high-parallelism success scenario.
     */
    public void testThreeMergesSixLeavesAllSucceedHighParallelism() throws Exception {
        stubRunComputeSuccess();
        stubExecutePlanSuccess();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 8).build()), future);

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("root exchange must be deregistered after success", exchangeFullyEmpty());
    }

    // cancel

    /**
     * Mid-flight cancel: one leaf is already dispatched, others are queued. Queued leaves observe {@code rootTask.isCancelled()} and
     * skip {@code executePlan}. In-flight stubs complete when the task is cancelled.
     */
    public void testCancelAfterNestedLeafDispatched() throws Exception {
        CountDownLatch parked = new CountDownLatch(2 + 1); // root + innerA + one leaf
        AtomicInteger leafDispatches = new AtomicInteger();
        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        stubParkUntilExchangeClosesOrCancelled(parked, rootTask, leafDispatches);

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()),
            null,
            rootTask
        );
        assertTrue("root, innerA, and the first leaf must park before cancel", parked.await(10, TimeUnit.SECONDS));
        assertEquals("only the first-wave leaf dispatched before cancel", 1, leafDispatches.get());

        TaskCancelHelper.cancel(rootTask, "test cancellation");

        var ex = expectThrows(ExecutionException.class, () -> started.future.get(10, TimeUnit.SECONDS));
        assertThat(ex.getCause(), instanceOf(TaskCancelledException.class));
        assertEquals("only the already-dispatched leaf must call executePlan", 1, leafDispatches.get());
        assertBusy(() -> assertTrue("cancel must drain parked and queued work", exchangeFullyEmpty()));
    }

    /**
     * Regression: pre-cancellation with 1000 leaves. All leaves immediately observe {@code rootTask.isCancelled()} and fail without
     * calling {@code executePlan}. The failure cascade must not stack-overflow.
     */
    public void testCancellationWithManyLeavesDoesNotStackOverflow() throws Exception {
        int leafCount = 1000;
        stubRunComputeSuccess();

        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        TaskCancelHelper.cancel(rootTask, "test cancellation");

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(leafCount), rootTask, future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(TaskCancelledException.class));
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("root exchange must be deregistered after cancellation", exchangeFullyEmpty());
    }

    // stack overflow prevention

    /**
     * 1000 leaves all complete synchronously. The permit-gated depth first search must not recurse deeply enough to overflow the stack.
     */
    public void testCompleteManyLeavesDoNotStackOverflow() throws Exception {
        int leafCount = 1000;
        stubRunComputeSuccess();
        stubExecutePlanSuccess();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(leafCount), future);

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("root exchange must be deregistered after success", exchangeFullyEmpty());
    }

    // session isolation

    /**
     * Two executors created for the same session id (as happens for real queries where {@code execute} runs once per subquery round plus
     * once for the main plan) must not collide in the exchange service. The per-executor {@code sessionPrefix}
     * (from {@code newChildSession}) keeps exchange ids disjoint.
     */
    public void testTwoExecutorsWithSameSessionIdDoNotCollide() throws Exception {
        stubRunComputeSuccess();
        List<ExchangeSink> unfinishedSinks = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            unfinishedSinks.add(sinkSupplier.get()); // acquire but do not finish the sink yet
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future1 = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), future1);
        future1.get();

        // Start the second round with the same "test-session" id while the first round's sinks are still open.
        var future2 = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), future2);
        future2.get();

        // Finishing the sinks lets the deferred parent-exchange decrements fire.
        unfinishedSinks.forEach(ExchangeSink::finish);
        assertBusy(() -> assertTrue("exchange service must be fully empty once all sinks drain", exchangeFullyEmpty()));
    }

    // ── profile ───────────────────────────────────────────────────────────────

    /**
     * The caller's {@link PlanTimeProfile} must be forwarded to the root merge segment's {@code runCompute}; nested merge segments must
     * each get their own independent instance so planning time is not double-counted.
     */
    public void testRootMergeReceivesCallersPlanTimeProfile() throws Exception {
        Map<String, PlanTimeProfile> segmentProfiles = stubSuccessRecordingSegmentProfiles();

        var queryProfile = new PlanTimeProfile();
        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), new QueryPragmas(Settings.EMPTY), queryProfile, future);
        future.get();

        assertThat(segmentProfiles.keySet(), equalTo(Set.of("main.final", "subplan-0.merge", "subplan-1.merge")));
        assertSame("root merge must use the caller's planTimeProfile", queryProfile, segmentProfiles.get("main.final"));
        PlanTimeProfile innerA = segmentProfiles.get("subplan-0.merge");
        PlanTimeProfile innerB = segmentProfiles.get("subplan-1.merge");
        assertNotNull(innerA);
        assertNotNull(innerB);
        assertNotSame("inner merges must not accumulate into the query-level profile", queryProfile, innerA);
        assertNotSame("inner merges must not accumulate into the query-level profile", queryProfile, innerB);
        assertNotSame("each inner merge must have its own profile", innerA, innerB);
    }

    /**
     * When profiling is not requested ({@code null} planTimeProfile), every segment must receive {@code null}.
     */
    public void testNullPlanTimeProfilePropagatesToAllSegments() throws Exception {
        Map<String, PlanTimeProfile> segmentProfiles = stubSuccessRecordingSegmentProfiles();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);
        future.get();

        assertThat(segmentProfiles.keySet(), equalTo(Set.of("main.final", "subplan-0.merge", "subplan-1.merge")));
        segmentProfiles.forEach((desc, profile) -> assertNull("segment [" + desc + "] must have null profile", profile));
    }

    // helpers

    /**
     * Stubs {@code runCompute} to succeed synchronously. For non-root merges (which have a parent sink supplier), the sink is obtained
     * and finished so the parent exchange sees all producers done.
     */
    private void stubRunComputeSuccess() {
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
    }

    /**
     * Stubs {@code executePlan} to create and finish the leaf sink then call {@code listener.onResponse}.
     */
    private void stubExecutePlanSuccess() {
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    /**
     * Stubs {@code executePlan} so that call index {@code failingLeaf} fires {@code listener.onFailure}; all other calls finish normally.
     */
    private void stubExecutePlanWithOneFailure(RuntimeException injected, int failingLeaf) {
        var leafCallCount = new AtomicInteger();
        doAnswer(inv -> {
            int call = leafCallCount.getAndIncrement();
            ActionListener<Result> listener = inv.getArgument(8);
            if (call == failingLeaf) {
                listener.onFailure(injected);
            } else {
                Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
                sinkSupplier.get().finish();
                Configuration cfg = inv.getArgument(4);
                listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            }
            return null;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    /**
     * Stubs a fully successful run and collects, per segment description, the {@link PlanTimeProfile} instance that each
     * {@code runCompute} call received.
     */
    private Map<String, PlanTimeProfile> stubSuccessRecordingSegmentProfiles() {
        Map<String, PlanTimeProfile> segmentProfiles = Collections.synchronizedMap(new HashMap<>());
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            segmentProfiles.put(context.description(), inv.getArgument(5));
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        stubExecutePlanSuccess();
        return segmentProfiles;
    }

    /**
     * Parks {@code runCompute} and {@code executePlan} on the real exchange objects.
     * <ul>
     *   <li>A merge with a parent sink completes when that sink is finished (STOP closed the parent source).</li>
     *   <li>The root merge has no parent sink; it waits until its source is {@code finishEarly}'d.</li>
     *   <li>Leaves complete when their parent-exchange sink is finished.</li>
     * </ul>
     */
    private void stubParkUntilExchangeCloses(CountDownLatch parked) {
        stubParkUntilExchangeClosesOrCancelled(parked, null, null);
    }

    private void stubParkUntilExchangeClosesOrCancelled(CountDownLatch parked, CancellableTask rootTask, AtomicInteger leafDispatches) {
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            ActionListener<DriverCompletionInfo> delegate = inv.getArgument(6);
            ActionListener<DriverCompletionInfo> listener = ActionListener.notifyOnce(delegate);
            ExchangeSource source = context.exchangeSourceSupplier().get();
            if (context.exchangeSinkSupplier() != null) {
                // Nested merge: finish the nested merge's own exchange when the parent exchange finishes so
                // children can unblock too. A shared RunOnce prevents double-finish if both success and
                // cancel paths fire.
                RunOnce finishSource = new RunOnce(source::finish);
                Runnable succeed = new RunOnce(() -> {
                    finishSource.run();
                    listener.onResponse(DriverCompletionInfo.EMPTY);
                });
                context.exchangeSinkSupplier().get().addCompletionListener(ActionListener.running(succeed));
                if (rootTask != null) {
                    Runnable failCancelled = new RunOnce(() -> {
                        finishSource.run();
                        listener.onFailure(rootTask.getTaskCancelledException());
                    });
                    rootTask.addListener(failCancelled::run);
                }
            } else {
                // Root merge: poll the source until it's finished by STOP. Don't call source.finish()
                // in the callback because the source is already finished by the time polling ends.
                Runnable succeed = new RunOnce(() -> listener.onResponse(DriverCompletionInfo.EMPTY));
                if (rootTask != null) {
                    Runnable failCancelled = new RunOnce(() -> listener.onFailure(rootTask.getTaskCancelledException()));
                    rootTask.addListener(failCancelled::run);
                }
                completeWhenSourceFinished(source, succeed);
            }
            parked.countDown();
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        doAnswer(inv -> {
            if (leafDispatches != null) {
                leafDispatches.incrementAndGet();
            }
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            ActionListener<Result> delegate = inv.getArgument(8);
            ActionListener<Result> listener = ActionListener.notifyOnce(delegate);
            Configuration cfg = inv.getArgument(4);
            Runnable succeed = new RunOnce(
                () -> listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null))
            );
            sinkSupplier.get().addCompletionListener(ActionListener.running(succeed));
            if (rootTask != null) {
                Runnable failCancelled = new RunOnce(() -> listener.onFailure(rootTask.getTaskCancelledException()));
                rootTask.addListener(failCancelled::run);
            }
            parked.countDown();
            return null;
        }).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    private void completeWhenSourceFinished(ExchangeSource source, Runnable onDone) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
            while (source.isFinished() == false) {
                Page page = source.pollPage();
                if (page != null) {
                    page.releaseBlocks();
                    continue;
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            onDone.run();
        });
    }

    private boolean exchangeFullyEmpty() {
        return exchangeService.sinkKeys().isEmpty() && exchangeService.sourceKeys().isEmpty();
    }

    private record StartedQuery(PlainActionFuture<Result> future, String sessionId, CancellableTask rootTask, EsqlExecutionInfo execInfo) {}

    private StartedQuery startQuery(SubPlan.Merge topology, QueryPragmas pragmas) {
        return startQuery(topology, pragmas, null, new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of()));
    }

    private StartedQuery startQuery(
        SubPlan.Merge topology,
        QueryPragmas pragmas,
        PlanTimeProfile planTimeProfile,
        CancellableTask rootTask
    ) {
        return startQuery(
            topology,
            pragmas,
            planTimeProfile,
            rootTask,
            new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER)
        );
    }

    private StartedQuery startQuery(
        SubPlan.Merge topology,
        QueryPragmas pragmas,
        PlanTimeProfile planTimeProfile,
        CancellableTask rootTask,
        EsqlExecutionInfo execInfo
    ) {
        String sessionId = "test-session";
        Configuration config = configuration(pragmas);
        FoldContext foldCtx = new FoldContext(Long.MAX_VALUE);
        var future = new PlainActionFuture<Result>();
        new SubPlansExecutor(
            computeService,
            exchangeService,
            sessionId,
            rootTask,
            new EsqlFlags(false),
            config,
            foldCtx,
            execInfo,
            Map.of(),
            () -> {},
            planTimeProfile,
            topology,
            future
        ).executePlan();
        return new StartedQuery(future, sessionId, rootTask, execInfo);
    }

    private void buildAndExecute(SubPlan.Merge topology, ActionListener<Result> listener) {
        buildAndExecute(topology, new QueryPragmas(Settings.EMPTY), listener);
    }

    private void buildAndExecute(SubPlan.Merge topology, QueryPragmas pragmas, ActionListener<Result> listener) {
        buildAndExecute(topology, pragmas, null, listener);
    }

    private void buildAndExecute(
        SubPlan.Merge topology,
        QueryPragmas pragmas,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        buildAndExecute(
            topology,
            new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of()),
            pragmas,
            planTimeProfile,
            listener
        );
    }

    /** Overload that accepts a pre-built task (e.g. a pre-cancelled one). */
    private void buildAndExecute(SubPlan.Merge topology, CancellableTask rootTask, ActionListener<Result> listener) {
        buildAndExecute(topology, rootTask, new QueryPragmas(Settings.EMPTY), null, listener);
    }

    private void buildAndExecute(
        SubPlan.Merge topology,
        CancellableTask rootTask,
        QueryPragmas pragmas,
        PlanTimeProfile planTimeProfile,
        ActionListener<Result> listener
    ) {
        String sessionId = "test-session";
        Configuration config = configuration(pragmas);
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        FoldContext foldCtx = new FoldContext(Long.MAX_VALUE);
        new SubPlansExecutor(
            computeService,
            exchangeService,
            sessionId,
            rootTask,
            new EsqlFlags(false),
            config,
            foldCtx,
            execInfo,
            Map.of(),
            () -> {},
            planTimeProfile,
            topology,
            listener
        ).executePlan();
    }

    // topology builders

    /**
     * One merge with {@code n} direct leaf children.
     */
    private static SubPlan.Merge oneMergeNLeaves(int n) {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        List<SubPlan> leaves = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            leaves.add(new SubPlan.Leaf(stub));
        }
        return new SubPlan.Merge(stub, leaves, MergeExec.Kind.UNION);
    }

    /**
     * <pre>
     * Merge (root)
     * ├─ Leaf (leafA, direct child of root)
     * └─ Merge (inner)
     *    ├─ Leaf (leafB)
     *    └─ Leaf (leafC)
     * </pre>
     */
    private static SubPlan.Merge twoMergesThreeLeaves() {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        SubPlan.Merge inner = new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)), MergeExec.Kind.UNION);
        return new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), inner), MergeExec.Kind.UNION);
    }

    /**
     * <pre>
     * Merge (root)              ← runCompute call 0
     * ├─ Merge (innerA)         ← runCompute call 1
     * │  ├─ Leaf (leafA)
     * │  └─ Leaf (leafB)
     * └─ Merge (innerB)         ← runCompute call 2
     *    ├─ Leaf (leafC)
     *    ├─ Leaf (leafD)
     *    ├─ Leaf (leafE)
     *    └─ Leaf (leafF)
     * </pre>
     */
    private static SubPlan.Merge threeMergesSixLeaves() {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        SubPlan.Merge innerA = new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)), MergeExec.Kind.UNION);
        SubPlan.Merge innerB = new SubPlan.Merge(
            stub,
            List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub), new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)),
            MergeExec.Kind.UNION
        );
        return new SubPlan.Merge(stub, List.of(innerA, innerB), MergeExec.Kind.UNION);
    }
}

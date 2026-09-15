/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.IsBlockedResult;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
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
 * Unit tests for {@code SubPlansExecutor}: success, failure unwind, STOP, cancel, lazy sink registration,
 * session-id isolation, and PROFILE wiring. On every path the query listener must complete exactly once and
 * both exchange sinks and sources must be deregistered.
 * <p>
 * {@code ComputeService} cannot be constructed in a unit test (it requires {@code TransportService}, {@code SearchService},
 * {@code ClusterService}, etc.), so it is mocked here with Mockito. This is documented as an AGENTS.md "last resort" use of a mock;
 * everything else (exchange service, task, thread pool) is real. Default {@code branch_parallel_degree} is 2 unless a test
 * sets it explicitly.
 */
public class SubPlansExecutorTests extends ESTestCase {

    private ComputeService computeService; // ComputeService is mocked because its constructor requires the full node stack.
    private PlannerSettings.Holder plannerSettingsHolder; // mocked and used by ComputeService
    private TestThreadPool threadPool; // a real thread pool used by ExchangeService
    private ExchangeService exchangeService; // a real ExchangeService, used by SubPlansExecutor to register/deregister source/sink
    // A flag the tests use to observe whether cancelQueryOnFailure fired. It's reset to false before each test so tests don't bleed into
    // each other.
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

        // executeMerge calls computeService.plannerSettings().get() to pass PlannerSettings into runCompute.
        // The mock returns defaults so that call doesn't NPE.
        plannerSettingsHolder = mock(PlannerSettings.Holder.class);
        when(plannerSettingsHolder.get()).thenReturn(PlannerSettings.DEFAULTS);

        computeService = mock(ComputeService.class);
        when(computeService.cancelQueryOnFailure(any())).thenAnswer(inv -> new RunOnce(() -> cancelled.set(true)));
        when(computeService.plannerSettings()).thenReturn(plannerSettingsHolder);
        // Counter-based like the real implementation: SubPlansExecutor derives its per-instance sessionPrefix from
        // newChildSession, and uniqueness across executors is what prevents exchange-id collisions between rounds.
        var childSessionCounter = new AtomicInteger();
        when(computeService.newChildSession(any())).thenAnswer(inv -> inv.getArgument(0) + "/" + childSessionCounter.incrementAndGet());
        when(computeService.profileDescription(any(), any())).thenAnswer(inv -> inv.getArgument(0) + "." + inv.getArgument(1));
    }

    @After
    public void tearDownRunner() {
        terminate(threadPool);
    }

    /**
     * Verifies three invariants on the success path:
     * <ol>
     *   <li><b>The terminal listener fires with a result, not a failure.</b> {@code future::get} returns without throwing.</li>
     *   <li><b>Query cancellation does not fire.</b> {@code cancelQueryOnFailure} must not be invoked when every ref
     *       completes successfully — cancelling a successful query would abort in-flight work unnecessarily.</li>
     *   <li><b>No residual exchange registrations.</b> All sink handlers and the root source handler must be deregistered
     *       after the query ends.</li>
     * </ol>
     *
     * <p><b>How the success path completes — step-by-step walk-through</b></p>
     *
     * <b>Phase 1a ({@code buildSubPlanContext}):</b> one {@code ExchangeSourceHandler} is registered for root; leafA and leafB only
     * reserve keep-alive refs on it — their {@code ExchangeSinkHandler}s are created lazily when each leaf is dispatched in phase 2b.
     *
     * <b>Phase 1b ({@code allocateComputeRefs}):</b> a {@code ComputeListener} is opened with count 5 (initial + guard + segmentListener
     * + childListeners[0] + childListeners[1]). LeafA and leafB are added to {@code scheduledLeaves}. {@code guard.onResponse(null)}
     * — count 4. Try-with-resources closes {@code ComputeListener} — count 3. {@code segmentListener} and the two child listeners
     * are still outstanding.
     *
     * <b>Phase 2a:</b> {@code executeMerge(root)} calls {@code runCompute}; the mock immediately calls
     * {@code segmentListener.onResponse(DriverCompletionInfo.EMPTY)} — count 2. {@code leafDispatchStarted} is still false, so
     * the success wrapper does not drain unstarted merges.
     *
     * <b>Phase 2b (leaf dispatch):</b> {@code tryExecuteNextLeaf} dispatches each leaf in turn; {@code ParentSink.attach} registers
     * the leaf's {@code ExchangeSinkHandler} and wires it into root's source. For each leaf, the {@code executePlan} mock:
     * <ol>
     *   <li>Calls {@code sinkSupplier.get()} to create the {@code ExchangeSink} from the just-registered handler.</li>
     *   <li>Calls {@code sink.finish()} — marks the sink as done. The handler fires its completion listener synchronously
     *       because the sink is already finished when {@code ParentSink.finish} later calls
     *       {@code addCompletionListener}.</li>
     *   <li>Calls {@code listener.onResponse(result)} → {@code finishLeaf} → {@code ParentSink.finish(null)}
     *       → {@code addCompletionListener} fires immediately → {@code exchangeService.finishSinkHandler} deregisters the
     *       sink → {@code childListeners[X].onResponse(completionInfo)} → count decrements.</li>
     * </ol>
     * After both leaves complete, count reaches 0 with no recorded failure. {@code ComputeListener} fires:
     * <ul>
     *   <li>{@code cancelQueryOnFailure} is <em>not</em> invoked — the failure collector is empty.</li>
     *   <li>{@code terminalListener.onResponse(profiles)} → {@code removeExchangeSource(root)} deregisters root's source
     *       handler; the root has no {@code ParentSink}, so that finish is skipped;
     *       {@code completionListener.onResponse(profiles)} → {@code future.onResponse(result)}.</li>
     * </ul>
     */
    public void testOneMergeTwoLeafSuccess() throws Exception {
        // runCompute completes its listener synchronously with an empty result.
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        // executePlan simulates a leaf producer: creates the exchange sink, finishes it (no data),
        // then calls the result listener. ParentSink.finish adds a completion listener to the sink
        // handler that fires only after all sinks are finished, so we must finish the sink before
        // calling the result listener to ensure the handler is deregistered synchronously.
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            ExchangeSink sink = sinkSupplier.get();
            sink.finish();
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaves(), future);

        // Get with a short timeout — the test should complete synchronously.
        future.get();

        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("exchange service must be fully empty after success", exchangeFullyEmpty());
    }

    // Failure in runCompute(merge) or executePlan(leaf)
    /**
     * Verifies three invariants that must hold when {@code runCompute} throws synchronously for the root merge segment:
     * <ol>
     *   <li><b>The terminal listener fires exactly once with the failure.</b> {@code future::get} must return immediately with the
     *       injected exception — no hang (which would mean some listener ref was never completed) and no spurious success.</li>
     *   <li><b>Query cancellation fires.</b> {@code cancelQueryOnFailure} must be invoked so the root {@code CancellableTask} is
     *       cancelled and any already-dispatched data-node searches are aborted.</li>
     *   <li><b>No residual exchange registrations.</b> The lazy leaf sinks were never registered, and the root source
     *       keyed {@code "test-session"} must also be gone — {@code sinkKeys()} alone would miss a leaked root source.</li>
     * </ol>
     *
     * <p><b>How the failure propagates — step-by-step walk-through</b></p>
     *
     * <b>Phase 1a ({@code buildSubPlanContext}):</b> one {@code ExchangeSourceHandler} is registered for root, keyed
     * {@code "test-session"}. LeafA and leafB get lazy {@code ParentSink}s that only hold keep-alive refs on root's source;
     * no sink handlers are registered for them.
     *
     * <b>Phase 1b ({@code allocateComputeRefs}):</b> a {@code ComputeListener} is opened and five refs are acquired:
     * <pre>
     *   initial ref (1)       owned by ComputeListener itself
     *   guard         (1)     held for the duration of allocateComputeRefs
     *   segmentListener (1)   for runCompute
     *   childListeners[0] (1) for leafA
     *   childListeners[1] (1) for leafB
     *                    ─────
     *   count = 5
     * </pre>
     * Leaves are enrolled in {@code scheduledLeaves}. {@code guard.onResponse} and close drop the count to 3
     * ({@code segmentListener} + two child listeners).
     *
     * <b>Phase 2a:</b> {@code executeMerge(root)} calls {@code runCompute}, which throws immediately.
     * {@code handleAncestorStartFailure} runs:
     * <ol>
     *   <li>{@code segmentListener.onFailure(e)} — count 2; failure recorded in {@code FailureCollector};
     *       {@code cancelOnFailure} drains unstarted merges (root is already started, so abort is a no-op).</li>
     *   <li>{@code childListeners[0].onFailure(e)} — count 1.</li>
     *   <li>{@code childListeners[1].onFailure(e)} — count 0.</li>
     *   <li>{@code child.abort} — {@code ParentSink.finish} for both leafA and leafB;
     *       their sinks were never registered (lazy), so this just releases their keep-alive refs on root's source.</li>
     * </ol>
     * Reaching zero fires:
     * <ul>
     *   <li>{@code cancelQueryOnFailure} → {@code cancelled.set(true)}</li>
     *   <li>{@code terminalListener.onFailure(e)} → {@code removeExchangeSource(root)} deregisters root's source handler;
     *       {@code finishEarly} drains the source; {@code completionListener.onFailure(e)} → {@code future.onFailure(e)}</li>
     * </ul>
     *
     * {@code execute()} returns without Phase 2b, so no leaf is dispatched.
     */
    public void testOneMergeTwoLeafMergeFailureInRunCompute() {
        var injected = new RuntimeException("injected planning failure");
        // runCompute throws, simulating a synchronous planning-time error.
        doThrow(injected).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaves(), future);

        // Terminal listener receives the failure exactly once (no hang, no spurious success).
        var ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(RuntimeException.class));
        assertEquals("injected planning failure", ex.getCause().getMessage());

        // cancelQueryOnFailure was invoked — the root task would be cancelled in production.
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());

        // Lazy leaf sinks were never registered; the root source must still be removed with them.
        assertTrue("exchange service must be fully empty after failure", exchangeFullyEmpty());
    }

    /**
     * Verifies that an asynchronous failure from a randomly chosen leaf's {@code executePlan} call propagates cleanly to the
     * root while the other leaf completes successfully.
     *
     * <p><b>Why listener.onFailure</b></p>
     * This test covers the asynchronous failure path: {@code executePlan} accepts the dispatch and later reports the failure
     * through its {@code ActionListener}. The synchronous-throw path ({@code executePlan} throwing before returning, which
     * {@code executeLeaf} catches and routes to {@code finishLeaf}) is covered separately by
     * {@link #testSynchronousExecutePlanFailureFirstWaveFailsCleanly} and
     * {@link #testSynchronousExecutePlanFailureOnRefillWaveFailsCleanly}.
     *
     * <p><b>Setup</b></p>
     * One root merge and two direct leaf children. Default {@code branch_parallel_degree} is 2, so Phase 2b's first wave
     * dispatches both leaves on the {@code execute()} thread — not one leaf and then the other via {@code onDone}.
     * {@code runCompute} is called once (for the root) and completes immediately. A randomly chosen {@code executePlan}
     * call ({@code failingLeaf ∈ [0, 1]}) calls {@code listener.onFailure(injected)} without creating a sink; the other
     * call finishes its sink and calls {@code listener.onResponse}.
     *
     * <p><b>Walk-through</b></p>
     * <ol>
     *   <li><b>Phase 2a:</b> {@code runCompute(root)} succeeds. Leaves were already on {@code scheduledLeaves} from Phase 1b.</li>
     *   <li><b>Phase 2b:</b> both leaves are in the first wave. The failing stub calls {@code listener.onFailure(injected)}.
     *       {@code finishLeaf} runs: {@code ParentSink.finish} deregisters that leaf's sink handler immediately
     *       (failure path skips the async drain), and that {@code childListener.onFailure(injected)} propagates to root's
     *       {@code ComputeListener}, which records the failure, fires {@code cancelQueryOnFailure}, and decrements the count.
     *       The succeeding stub finishes its sink and calls {@code listener.onResponse}, decrementing root's count again.</li>
     *   <li><b>Terminal firing:</b> after both child listeners complete, root's {@code ComputeListener} reaches zero and fires
     *       the terminal listener with the recorded failure, completing the future with {@code injected}.</li>
     * </ol>
     *
     * <p><b>Assertions</b></p>
     * <ul>
     *   <li>The future fails with the injected exception message.</li>
     *   <li>{@code cancelQueryOnFailure} fired.</li>
     *   <li>The exchange service is fully empty: both leaf sinks and the root source are deregistered.</li>
     * </ul>
     */
    public void testOneMergeTwoLeafRandomLeafFailureInExecutePlan() throws Exception {
        var injected = new RuntimeException("injected leaf failure");
        int failingLeaf = randomIntBetween(0, 1);
        var leafCallCount = new AtomicInteger();
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaves(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after leaf failure", exchangeFullyEmpty());
    }

    /**
     * Verifies that a failure reported asynchronously by a randomly chosen leaf's {@code executePlan} call propagates cleanly to the
     * root even when the other seven leaves complete successfully.
     *
     * <p><b>Setup</b></p>
     * The {@code oneMergeEightLeaf} topology has one root merge with eight direct leaf children — no inner merges.
     * {@code runCompute} is called once (for the root) and completes its listener immediately. One randomly chosen
     * {@code executePlan} call ({@code failingLeaf ∈ [0, 7]}) calls {@code listener.onFailure(injected)} instead of completing
     * normally; the remaining seven calls finish their exchange sink and call {@code listener.onResponse}.
     * Note: the failure is delivered through the listener to exercise the asynchronous failure path; the synchronous-throw
     * path is covered by the dedicated {@code testSynchronousExecutePlanFailure*} tests.
     *
     * <p><b>Walk-through</b></p>
     * <ol>
     *   <li><b>Phase 2a:</b> {@code runCompute(root)} succeeds. All eight leaves were already on {@code scheduledLeaves}
     *       from Phase 1b, as root's children.</li>
     *   <li><b>Phase 2b — dispatch leaves:</b> default {@code branch_parallel_degree} is 2, so the first wave runs two
     *       leaves on the {@code execute()} thread and the rest refill via {@code onDone}.
     *       <ul>
     *         <li>For the failing leaf: {@code listener.onFailure(injected)} is called. {@code finishLeaf} deregisters the
     *             leaf's sink handler immediately (failure path skips the async drain) and calls
     *             {@code childListeners[failingLeaf].onFailure(injected)} directly on root's {@code ComputeListener},
     *             which records the failure and fires {@code cancelQueryOnFailure}.</li>
     *         <li>For each succeeding leaf: the sink supplier is invoked, the sink is finished, and {@code listener.onResponse}
     *             is called, decrementing root's count via {@code childListeners[i].onResponse}.</li>
     *       </ul>
     *   </li>
     *   <li><b>Terminal firing:</b> after all eight child listeners complete (seven successes + one failure), root's
     *       {@code ComputeListener} reaches zero and fires the terminal listener with the recorded failure, completing the future
     *       with {@code injected}.</li>
     * </ol>
     *
     * <p><b>Assertions</b></p>
     * <ul>
     *   <li>The future fails with the injected exception message.</li>
     *   <li>{@code cancelQueryOnFailure} fired.</li>
     *   <li>The exchange service is fully empty: all eight leaf sinks and the root source are deregistered.</li>
     * </ul>
     */
    public void testOneMergeEightLeafRandomLeafFailureInExecutePlan() throws Exception {
        var injected = new RuntimeException("injected leaf failure");
        int failingLeaf = randomIntBetween(0, 7);
        var leafCallCount = new AtomicInteger();
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(8), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after leaf failure", exchangeFullyEmpty());
    }

    /**
     * Verifies that a synchronous failure in a randomly chosen inner merge propagates upward and cleans up <em>all</em>
     * exchange registrations, even though the sibling inner merge and its leaves complete successfully.
     *
     * <p><b>Setup</b></p>
     * {@code runCompute} is called three times: call 0 for root (always succeeds), then inner merges lazily via
     * {@code ensureAncestorsStarted} — call 1 for innerA (first-wave leaves), call 2 for innerB (first innerB leaf).
     * A randomly chosen inner merge ({@code failingCall ∈ {1, 2}}) throws synchronously; the other succeeds and its
     * leaves are dispatched via {@code executePlan} (2 leaves if innerA succeeds, 4 if innerB succeeds).
     *
     * <p><b>Walk-through — failing inner merge is innerA (call 1)</b></p>
     * <ol>
     *   <li><b>Phase 2a:</b> root's {@code runCompute} succeeds inline. {@code leafDispatchStarted} is still false, so
     *       the root wrapper does not drain unstarted merges — innerA and innerB stay unstarted.</li>
     *   <li><b>Phase 2b:</b> first-wave leaves are under innerA. {@code ensureAncestorsStarted} starts innerA;
     *       {@code executeMerge} throws on call 1. {@code handleAncestorStartFailure} aborts leafA and leafB and
     *       {@code cancelOnFailure} aborts unstarted innerB.</li>
     *   <li><b>Terminal firing:</b> root fires with the recorded failure from innerA.</li>
     * </ol>
     * The walk-through is symmetric when innerB fails (call 2): innerA's 2 leaves complete, then the first innerB leaf
     * starts innerB and that {@code runCompute} throws.
     *
     * <p><b>Assertions</b></p>
     * <ul>
     *   <li>The future fails with the injected exception message.</li>
     *   <li>{@code cancelQueryOnFailure} fired.</li>
     *   <li>The exchange service is fully empty: all leaf sinks, both inner-merge sinks and sources, and the root
     *       source are deregistered.</li>
     * </ul>
     */
    public void testNestedMergesRandomFailureInRunCompute() {
        var injected = new RuntimeException("injected inner failure");
        int failingCall = randomIntBetween(1, 2); // 1 = innerA, 2 = innerB
        var callCount = new AtomicInteger();
        doAnswer(inv -> {
            int call = callCount.getAndIncrement();
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            if (call == failingCall) {
                throw injected;
            }
            ComputeContext context = inv.getArgument(1);
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), not(instanceOf(TaskCancelledException.class)));
        assertEquals("injected inner failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after nested failure", exchangeFullyEmpty());
    }

    /**
     * Verifies that an asynchronous failure from a randomly chosen leaf's {@code executePlan} call propagates cleanly
     * through its parent inner merge up to root, while all other leaves complete successfully.
     *
     * <p><b>Setup</b></p>
     * All three {@code runCompute} calls (root, innerA, innerB) succeed. The six leaves are dispatched in order:
     * leafA (call 0) and leafB (call 1) from innerA, then leafC–leafF (calls 2–5) from innerB.
     * A randomly chosen leaf ({@code failingLeaf ∈ [0, 5]}) calls {@code listener.onFailure(injected)}; the other
     * five finish their sink and call {@code listener.onResponse}.
     *
     * <p><b>Walk-through</b></p>
     * <ol>
     *   <li><b>Phase 2a / 2b:</b> all three {@code runCompute} calls succeed (root in 2a, inners lazily in 2b).
     *       LeafA–leafF were already on {@code scheduledLeaves} from Phase 1b (leafA/B for innerA, leafC–F for innerB).</li>
     *   <li><b>Phase 2b:</b> default {@code branch_parallel_degree} is 2, so two leaves run in the first wave
     *       and the rest refill via {@code onDone}.
     *       <ul>
     *         <li>For the failing leaf: {@code listener.onFailure(injected)} deregisters its sink immediately and
     *             propagates to its parent inner merge's child listener. When that inner merge's count reaches zero
     *             (after its other leaf or leaves complete), its terminal listener deregisters its source and sink and
     *             propagates {@code onFailure} to root's child listener. Root's {@code ComputeListener} records the
     *             failure and fires {@code cancelQueryOnFailure}.</li>
     *         <li>For each succeeding leaf: sink finished, {@code listener.onResponse} propagates through its parent
     *             inner merge's child listener when that merge's count reaches zero.</li>
     *       </ul>
     *   </li>
     *   <li><b>Terminal firing:</b> after both inner merges' child listeners complete (one failure, one success),
     *       root's count reaches zero and fires the terminal listener with the recorded failure.</li>
     * </ol>
     *
     * <p><b>Assertions</b></p>
     * <ul>
     *   <li>The future fails with the injected exception message.</li>
     *   <li>{@code cancelQueryOnFailure} fired.</li>
     *   <li>The exchange service is fully empty: all six leaf sinks, both inner-merge sinks and sources, and the root
     *       source are deregistered.</li>
     * </ul>
     */
    public void testNestedMergesRandomLeafFailureInExecutePlan() throws Exception {
        var injected = new RuntimeException("injected leaf failure");
        int failingLeaf = randomIntBetween(0, 5); // 0–1 = leafA/B (innerA), 2–5 = leafC–F (innerB)
        var leafCallCount = new AtomicInteger();
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after nested leaf failure", exchangeFullyEmpty());
    }

    /**
     * After a failure in innerA's {@code runCompute}, the cancel <em>runnable</em> returned by
     * {@code ComputeService.cancelQueryOnFailure} must run exactly once — not once per merge node.
     * {@code execute()} always calls the factory once at setup, so {@code verify(times(1)).cancelQueryOnFailure}
     * alone does not prove cancel ran.
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
            if (call == 1) { // innerA fails
                throw injected;
            }
            ComputeContext context = inv.getArgument(1);
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);

        expectThrows(ExecutionException.class, future::get);
        assertTrue("cancel runnable must have fired", cancelled.get());
        assertEquals("cancel runnable must fire once, not once per merge", 1, cancelRuns.get());
        // Factory is invoked once at execute() setup, not once per merge node.
        verify(computeService, times(1)).cancelQueryOnFailure(any());
    }

    /**
     * A synchronous throw from {@code executePlan} during the initial (first-wave) dispatch — which runs inline on the calling thread must
     * be caught by {@code executeLeaf} and routed to {@code finishLeaf}, failing the query cleanly: no hang, cancellation fired, and the
     * sink handler that {@code ParentSink.attach} registered just before the throw
     * deregistered again.
     */
    public void testSynchronousExecutePlanFailureFirstWaveFailsCleanly() {
        var injected = new RuntimeException("injected synchronous executePlan failure");
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        doThrow(injected).when(computeService)
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaves(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected synchronous executePlan failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after synchronous failure", exchangeFullyEmpty());
    }

    /**
     * A synchronous throw from {@code executePlan} on a <em>refill</em> dispatch — which runs inside the refill runnable on the search
     * pool, where an escaping exception would be swallowed by the executor — must also be caught by {@code executeLeaf}.
     *
     * <p>With {@code branch_parallel_degree=1} and two leaves, the first {@code executePlan} call completes inline (so the refill claims
     * leaf 1 on a search-pool thread) and the second call throws.
     */
    public void testSynchronousExecutePlanFailureOnRefillWaveFailsCleanly() throws Exception {
        var injected = new RuntimeException("injected refill-wave failure");
        var leafCallCount = new AtomicInteger();
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()), future);

        // A bounded get: pre-fix the future never completes and the test would otherwise hang.
        var ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertEquals("injected refill-wave failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertBusy(() -> assertTrue("exchange service must be fully empty after refill failure", exchangeFullyEmpty()));
    }

    // lazy start

    /**
     * A synchronous root {@code runCompute} completion during Phase 2a is not LIMIT/STOP. The success wrapper must not
     * {@code drainUnstartedMerges}: that would {@code executeMerge} every nested segment before any leaf is claimed.
     * With {@code branch_parallel_degree=1}, only root and innerA have run when the first leaf parks; innerB has no sink.
     * Failing that leaf then aborts unstarted innerB (the already-completed root wrapper will not drain it).
     */
    public void testSyncRootCompletionDoesNotStartUnstartedNestedMerges() throws Exception {
        var injected = new RuntimeException("injected first-leaf failure");
        CountDownLatch parked = new CountDownLatch(1);
        List<String> runComputeDescriptions = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<ActionListener<Result>> firstLeafListener = new AtomicReference<>();
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            runComputeDescriptions.add(context.description());
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().finish();
            }
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            ActionListener<Result> listener = ActionListener.notifyOnce(inv.getArgument(8));
            if (firstLeafListener.compareAndSet(null, listener)) {
                parked.countDown();
                return null;
            }
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build())
        );
        assertTrue("the first-wave leaf must park so execute() returns before innerB is needed", parked.await(10, TimeUnit.SECONDS));
        assertThat(
            "sync root completion must not drain-start innerB; only root and the leaf's ancestor innerA run",
            runComputeDescriptions,
            equalTo(List.of("main.final", "subplan-0.merge"))
        );
        assertThat(
            "an unstarted nested merge must not have a sink handler",
            exchangeService.sinkKeys(),
            not(hasItem("test-session/1/subplan-1"))
        );

        firstLeafListener.get().onFailure(injected);

        var ex = expectThrows(ExecutionException.class, () -> started.future.get(10, TimeUnit.SECONDS));
        assertEquals("injected first-leaf failure", ex.getCause().getMessage());
        assertThat(runComputeDescriptions, equalTo(List.of("main.final", "subplan-0.merge")));
        assertBusy(() -> assertTrue(exchangeFullyEmpty()));
    }

    /**
     * Regression test for the inactive-sink-reaper data-loss bug: leaf sink handlers must be registered lazily at dispatch
     * time, not eagerly in Phase 1a. An eagerly registered handler for a leaf queued behind {@code branchParallelDegree} has
     * no attached {@code ExchangeSink} and an empty buffer ({@code hasData() == false}), so the {@code InactiveSinksReaper}
     * would reap it after the inactive interval, silently dropping or failing that branch.
     *
     * <p>With {@code branch_parallel_degree=1} and three leaves, {@code executePlan} is stubbed to capture each dispatch
     * without completing it. Right after {@code execute()} returns, exactly one sink handler (the in-flight leaf's) may be
     * registered — the two queued leaves must be invisible to the exchange service. As each leaf completes, the next leaf's
     * sink appears, and the not-yet-dispatched leaves remain unregistered throughout.
     */
    public void testUndispatchedLeavesHaveNoSinkHandlers() throws Exception {
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        // Capture each dispatched leaf as a completion Runnable without completing it, so dispatch stalls at
        // branchParallelDegree and the remaining leaves stay queued.
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(3), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()), future);

        // Phase 2b dispatched exactly one leaf synchronously; the two queued leaves must have no sink handler
        // (pre-fix: all three were registered in Phase 1a and hasSize(1) fails with 3).
        assertThat(pendingCompletions, hasSize(1));
        assertThat(exchangeService.sinkKeys(), hasSize(1));

        // The setUp stub makes this executor's sessionPrefix "test-session/1", so leaf sink ids are deterministic.
        for (int i = 0; i < 3; i++) {
            final int dispatchedSoFar = i + 1;
            assertBusy(() -> assertThat(pendingCompletions, hasSize(dispatchedSoFar)));
            for (int queued = dispatchedSoFar; queued < 3; queued++) {
                String queuedLeafSinkId = "test-session/1/subplan-" + queued;
                assertThat("queued leaf must have no sink handler", exchangeService.sinkKeys(), not(hasItem(queuedLeafSinkId)));
            }
            pendingCompletions.get(i).run();
        }

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertBusy(() -> assertTrue("exchange service must be fully empty after success", exchangeFullyEmpty()));
    }

    // stop

    /**
     * Async STOP only calls {@link ExchangeService#finishSessionEarly} with the bare session id, which is the root merge source. Nested
     * merge sources and leaf sinks are not looked up. They must finish because closing the root source closes the remotes that feed it,
     * each nested merge stub then {@code finish()}es its own source (the real driver does the same when its parent sink is done), and that
     * closes the nested leaves.
     * <p>
     * {@code runCompute}/{@code executePlan} stay parked on those exchange objects instead of completing themselves. If the cascade is
     * broken the future never completes. All six leaves are in flight so every leaf sink is a remote of a nested source at STOP time.
     */
    public void testFinishSessionEarlyUnblocksParkedNestedTree() throws Exception {
        CountDownLatch parked = new CountDownLatch(3 + 6); // root + innerA + innerB, and six leaves
        stubParkUntilExchangeCloses(parked);

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 8).build())
        );
        assertTrue("nested merges and leaves must park on the exchange before STOP", parked.await(10, TimeUnit.SECONDS));
        assertThat(
            exchangeService.sourceKeys(),
            hasItems("test-session", "test-session/1/subplan-0/merge", "test-session/1/subplan-1/merge")
        );

        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue("STOP must find the root source under the bare session id", stopped.get(10, TimeUnit.SECONDS));

        started.future.get(10, TimeUnit.SECONDS);
        assertFalse("STOP is a graceful finishEarly, not a failure", cancelled.get());
        assertBusy(() -> assertTrue("nested sources and leaf sinks must go with the root", exchangeFullyEmpty()));
    }

    /**
     * Same STOP cascade as {@link #testFinishSessionEarlyUnblocksParkedNestedTree}, but with {@code branch_parallel_degree=1}
     * so five leaves are still queued (no sink handler, not a remote of any source) when {@code finishSessionEarly} runs.
     * {@code executeLeaf} must release those queued leaves without calling {@code executePlan}: production STOP marks
     * {@link EsqlExecutionInfo} stopped before {@code finishSessionEarly}, and starting a new plan after STOP can fail
     * the query instead of returning partial results.
     */
    public void testFinishSessionEarlyUnblocksQueuedNestedLeaves() throws Exception {
        CountDownLatch parked = new CountDownLatch(2 + 1);
        AtomicInteger leafDispatches = new AtomicInteger();
        stubParkUntilExchangeClosesOrCancelled(parked, null, leafDispatches);

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build())
        );
        assertTrue(
            "root, innerA, and the one dispatched leaf must park; innerB starts lazily after STOP fires",
            parked.await(10, TimeUnit.SECONDS)
        );
        assertEquals("only the first-wave leaf is dispatched before STOP", 1, leafDispatches.get());
        assertThat(exchangeService.sinkKeys(), hasSize(2)); // innerA sink and one leaf; innerB is unstarted, so has no sink
        assertThat(
            "queued leaves must not have sink handlers yet",
            exchangeService.sinkKeys(),
            not(hasItem("test-session/1/subplan-0.subplan-1"))
        );
        // An unstarted nested merge must register nothing: an idle handler with no attached sink reports hasData()==false and
        // would be reaped by InactiveSinksReaper once it outlived esql.exchange.sink_inactive_interval.
        assertThat(
            "an unstarted nested merge must not have a sink handler",
            exchangeService.sinkKeys(),
            not(hasItem("test-session/1/subplan-1"))
        );

        started.execInfo.markAsStopped();
        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue(stopped.get(10, TimeUnit.SECONDS));

        started.future.get(10, TimeUnit.SECONDS);
        assertEquals("queued leaves must not call executePlan after STOP", 1, leafDispatches.get());
        assertFalse("STOP is a graceful finishEarly, not a failure", cancelled.get());
        assertBusy(() -> assertTrue("queued leaves must not leak handlers after STOP", exchangeFullyEmpty()));
    }

    /**
     * The reviewer's failure mode: a queued {@code executePlan} after STOP throws. Pre-fix that failure was recorded
     * on the merge listener and the query failed. After the skip, the second dispatch never happens and STOP succeeds.
     */
    public void testQueuedExecutePlanFailureAfterStopDoesNotFailQuery() throws Exception {
        CountDownLatch parked = new CountDownLatch(2 + 1);
        AtomicInteger leafDispatches = new AtomicInteger();
        stubParkUntilExchangeCloses(parked);
        doAnswer(inv -> {
            if (leafDispatches.incrementAndGet() > 1) {
                throw new RuntimeException("queued executePlan after STOP");
            }
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            ActionListener<Result> listener = ActionListener.notifyOnce(inv.getArgument(8));
            Configuration cfg = inv.getArgument(4);
            sinkSupplier.get()
                .addCompletionListener(
                    ActionListener.running(
                        () -> listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null))
                    )
                );
            parked.countDown();
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build())
        );
        assertTrue(
            "root, innerA, and the one dispatched leaf must park; innerB starts lazily after STOP fires",
            parked.await(10, TimeUnit.SECONDS)
        );
        assertEquals(1, leafDispatches.get());

        started.execInfo.markAsStopped();
        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue(stopped.get(10, TimeUnit.SECONDS));

        started.future.get(10, TimeUnit.SECONDS);
        assertEquals("queued leaves must not call executePlan after STOP", 1, leafDispatches.get());
        assertFalse("STOP must not become a query failure", cancelled.get());
        assertBusy(() -> assertTrue(exchangeFullyEmpty()));
    }

    /**
     * STOP that lands before {@code execute} runs must still let the query terminate.
     * <p>
     * {@code TransportEsqlAsyncStopAction} marks {@link EsqlExecutionInfo} stopped <em>before</em> it calls
     * {@code finishSessionEarly}. When STOP arrives while the query is still planning, that second call finds no source
     * registered under the bare session id and is a no-op, but {@code isStopped} stays true for the rest of the query. Every
     * leaf therefore takes the stopped skip in {@code executeLeaf} and no leaf ever reaches {@code ensureAncestorsStarted}.
     * <p>
     * Nothing else starts a nested merge on this path. {@code drainUnstartedMerges} runs from the root merge's success
     * wrapper, and the root cannot succeed: each unstarted nested merge still holds the keep-alive ref its {@link SubPlan}
     * child took on the root's exchange source during setup, so the root coordinator parks on that source forever. Unless the
     * stopped skip releases those merges itself, the query hangs and every pre-allocated {@code ComputeListener} ref leaks.
     * <p>
     * Uses {@link #stubCoordinatorDrivers} rather than the parking stub: the point of the test is whether an unstarted
     * merge is ever released, which only a stub that completes a nested merge off its own source can show.
     */
    public void testStopBeforeDispatchReleasesUnstartedNestedMerges() throws Exception {
        stubCoordinatorDrivers();
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        // STOP landed during planning, before phase 1a registered the root source, so finishSessionEarly was a no-op.
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
            any()
        );
        assertBusy(() -> assertTrue("unstarted nested merges must not leak sources or sinks", exchangeFullyEmpty()));
    }

    // abort vs a live sibling

    /**
     * A nested merge that fails to start while a sibling leaf beneath it is already running.
     * <p>
     * {@code ensureAncestorsStarted} discards {@code startMerge}'s return value: a caller that loses the {@code started} CAS treats
     * that as "another thread is starting it, carry on", attaches its sink and calls {@code executePlan}. If the caller that <em>won</em>
     * the CAS then throws out of {@code executeMerge}, {@code handleAncestorStartFailure} settles every one of that merge's
     * {@code childListeners} - including the running sibling's - and aborts its children, which finishes that sibling's sink handler
     * underneath a live producer.
     * <p>
     * The test wedges the two halves apart: the merge's {@code runCompute} blocks until the sibling leaf has attached and is inside
     * {@code executePlan}, and only then throws. The sibling is left parked, as a real driver would be, and released afterwards to
     * confirm a late completion arriving after the teardown is absorbed.
     */
    public void testMergeStartFailureWhileSiblingLeafIsRunning() throws Exception {
        CountDownLatch siblingRunning = new CountDownLatch(1);
        AtomicInteger leafDispatches = new AtomicInteger();
        List<ActionListener<Result>> parkedLeaves = Collections.synchronizedList(new ArrayList<>());
        List<ExchangeSink> parkedSinks = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Exception> lateFailure = new AtomicReference<>();

        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            ActionListener<DriverCompletionInfo> listener = ActionListener.notifyOnce(inv.getArgument(6));
            if (context.exchangeSinkSupplier() == null) {
                // Root: park on its own source the way a real coordinator does.
                ExchangeSource source = context.exchangeSourceSupplier().get();
                threadPool.executor(ThreadPool.Names.SEARCH)
                    .execute(() -> completeWhenSourceFinished(source, () -> listener.onResponse(DriverCompletionInfo.EMPTY)));
                return null;
            }
            // The nested merge. executeMerge has already attached its sink; hold here until the sibling leaf is live, then fail the
            // start the way a synchronous throw out of runCompute would.
            assertTrue("sibling leaf must attach before the merge start fails", siblingRunning.await(30, TimeUnit.SECONDS));
            throw new RuntimeException("merge failed to start");
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        doAnswer(inv -> {
            Configuration cfg = inv.getArgument(4);
            ActionListener<Result> listener = ActionListener.notifyOnce(inv.getArgument(8));
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            ExchangeSink sink = sinkSupplier.get();
            if (leafDispatches.incrementAndGet() == 1) {
                // leafA, under the root: succeed at once so its refill dispatches the third leaf on another thread.
                sink.finish();
                listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
                return null;
            }
            // The sibling under the nested merge: attached and running. Park it and let the merge start fail now.
            parkedSinks.add(sink);
            parkedLeaves.add(listener);
            siblingRunning.countDown();
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(twoMergesThreeLeafs(), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 2).build()), future);

        Exception failure = expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("merge failed to start"));

        // The running sibling was torn down under a live producer. Complete it late, as its driver eventually would, and check that
        // the executor absorbs it rather than throwing or leaking.
        assertThat(parkedLeaves, hasSize(1));
        // Pin down that the teardown really did run ahead of the late write below, so this is a write into a sink whose handler has
        // already been finished and deregistered - not a still-live one.
        assertBusy(() -> assertTrue("the torn-down sibling's handler must be gone before the late write", exchangeFullyEmpty()));
        try {
            // A real driver would still be writing when the teardown lands. Push a page through the torn-down sink: the test
            // framework's leak detector is what decides whether that page survives the handler having already been finished.
            parkedSinks.get(0).addPage(new Page(TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(1)));
            parkedSinks.get(0).finish();
            parkedLeaves.get(0)
                .onResponse(
                    new Result(
                        List.of(),
                        List.of(),
                        null,
                        configuration(new QueryPragmas(Settings.EMPTY)),
                        DriverCompletionInfo.EMPTY,
                        null,
                        null
                    )
                );
        } catch (Exception e) {
            lateFailure.set(e);
        }
        assertNull("a late leaf completion after teardown must not throw", lateFailure.get());
        assertBusy(() -> assertTrue("torn-down sibling must not leak its sink handler", exchangeFullyEmpty()));
    }

    /**
     * Failure-path drain must abort unstarted nested merges, not only settle their ComputeListener refs. With
     * {@code branch_parallel_degree=1}, innerB is still unstarted when the first innerA leaf fails. The test {@code cancelQueryOnFailure}
     * mock does not cancel {@code rootTask}, so {@code notifyIfCancelled} cannot skip the refill: without {@code ParentSink.finished}, an
     * innerB leaf would {@code attach()} after drain {@code finishEarly}'d innerB's source.
     * <p>
     * LeafB under already-started innerA may still dispatch; that is not this bug. {@code finishSessionEarly} runs only after refill has
     * had a chance to reach innerB, so it unblocks the parked root/innerA stubs without hiding the attach via
     * {@code sessionAlreadyStopped}.
     */
    public void testFailureAbortsUnstartedMergeBeforeQueuedLeafAttach() throws Exception {
        var injected = new RuntimeException("injected first-leaf failure");
        CountDownLatch parked = new CountDownLatch(2 + 1);
        AtomicInteger innerADispatches = new AtomicInteger();
        AtomicInteger innerBDispatches = new AtomicInteger();
        AtomicReference<ActionListener<Result>> firstLeafListener = new AtomicReference<>();
        stubParkUntilExchangeCloses(parked);
        doAnswer(inv -> {
            String path = inv.getArgument(7);
            ActionListener<Result> listener = ActionListener.notifyOnce(inv.getArgument(8));
            if (path.startsWith("subplan-1")) {
                innerBDispatches.incrementAndGet();
                throw new AssertionError("innerB leaf [" + path + "] must not call executePlan after sibling failure");
            }
            innerADispatches.incrementAndGet();
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            if (firstLeafListener.compareAndSet(null, listener)) {
                sinkSupplier.get().addCompletionListener(ActionListener.running(() -> {}));
                parked.countDown();
                return null;
            }
            // leafB under already-started innerA may still run; complete it so refill reaches innerB.
            sinkSupplier.get().finish();
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build())
        );
        assertTrue("root, innerA, and the one dispatched leaf must park; innerB is still unstarted", parked.await(10, TimeUnit.SECONDS));
        assertEquals(1, innerADispatches.get());
        assertEquals(0, innerBDispatches.get());
        assertThat(
            "an unstarted nested merge must not have a sink handler",
            exchangeService.sinkKeys(),
            not(hasItem("test-session/1/subplan-1"))
        );

        firstLeafListener.get().onFailure(injected);

        assertBusy(() -> {
            assertTrue("refill must have reached innerA's second leaf before we stop the session", innerADispatches.get() >= 2);
            assertEquals("innerB leaves must skip via parentSink.finished, not executePlan", 0, innerBDispatches.get());
            assertThat(exchangeService.sinkKeys(), not(hasItem("test-session/1/subplan-1")));
        });

        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue(stopped.get(10, TimeUnit.SECONDS));

        var ex = expectThrows(ExecutionException.class, () -> started.future.get(10, TimeUnit.SECONDS));
        assertThat(ex.getCause(), not(instanceOf(TaskCancelledException.class)));
        assertEquals("injected first-leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertBusy(() -> assertTrue("aborted innerB must not leak handlers", exchangeFullyEmpty()));
    }

    // cancel
    /**
     * Mid-flight cancel of a nested tree: one leaf is already in {@code executePlan}, the rest are queued.
     * {@code executeLeaf} skips the queue via {@code notifyIfCancelled}. In-flight merge/leaf stubs complete when
     * the task is cancelled, the same way a real driver checks {@code CancellableTask}. Pre-cancel tests never
     * reach {@code executePlan}; this one does, then cancels.
     */
    public void testCancelAfterNestedLeafDispatched() throws Exception {
        CountDownLatch parked = new CountDownLatch(2 + 1);
        AtomicInteger leafDispatches = new AtomicInteger();
        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        stubParkUntilExchangeClosesOrCancelled(parked, rootTask, leafDispatches);

        StartedQuery started = startQuery(
            threeMergesSixLeaves(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()),
            null,
            rootTask
        );
        assertTrue(
            "root, innerA, and the one dispatched leaf must park; innerB is still unstarted and cancel aborts it",
            parked.await(10, TimeUnit.SECONDS)
        );
        assertEquals("only the first-wave leaf is dispatched before cancel", 1, leafDispatches.get());

        TaskCancelHelper.cancel(rootTask, "test cancellation");

        var ex = expectThrows(ExecutionException.class, () -> started.future.get(10, TimeUnit.SECONDS));
        assertThat(ex.getCause(), instanceOf(TaskCancelledException.class));
        assertEquals("only the already-dispatched leaf called executePlan", 1, leafDispatches.get());
        assertBusy(() -> assertTrue("cancel must drain parked and queued work", exchangeFullyEmpty()));
    }

    /**
     * Regression test for when the root task was canceled while many leaves were queued.
     */
    public void testCancellationWithManyLeavesDoesNotStackOverflow() throws Exception {
        int leafCount = 1000; // well above the ~500 frames that would overflow before the fix

        // runCompute must succeed so Phase 2b begins with all leaves already on scheduledLeaves.
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        // executePlan should never be called: the task is cancelled before any leaf is dispatched.

        String sessionId = "test-session";
        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        Configuration config = configuration(new QueryPragmas(Settings.EMPTY));
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        FoldContext foldCtx = new FoldContext(Long.MAX_VALUE);
        Executor executor = threadPool.executor(ThreadPool.Names.SEARCH);

        // Pre-cancel so every leaf hits the cancel path in executeLeaf instead of calling executePlan.
        TaskCancelHelper.cancel(rootTask, "test cancellation");

        var future = new PlainActionFuture<Result>();
        new SubPlansExecutor(
            computeService,
            exchangeService,
            executor,
            sessionId,
            rootTask,
            new EsqlFlags(false),
            config,
            foldCtx,
            execInfo,
            Map.of()
        ).execute(oneMergeNLeaves(leafCount), null, future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(TaskCancelledException.class));
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after cancellation", exchangeFullyEmpty());
    }

    /**
     * If the search executor throws when {@code submitOnDone} tries to schedule the next leaf (a plain executor throwing
     * synchronously; an {@code EsThreadPoolExecutor} would instead deliver shutdown-rejection via the refill runnable's
     * {@code onRejection} hook — force execution makes queue-pressure rejection impossible), the remaining undispatched
     * leaves must be failed via {@code failRemainingLeaves} and the query must complete with an error rather than hanging
     * indefinitely.
     *
     * <p><b>Setup</b></p>
     * A topology of 5 leaves under one root merge is used. The root task is pre-cancelled so that no
     * leaf actually calls {@code executePlan} — each leaf immediately hits the cancellation path in
     * {@code executeLeaf} ({@code notifyIfCancelled} fires synchronously). The executor wrapper starts
     * forwarding to the real pool normally and begins rejecting after {@code runCompute} completes at
     * the end of Phase 2a. This timing ensures that the first {@code submitOnDone} call from the
     * cancellation path is rejected.
     *
     * <p><b>Walk-through</b></p>
     * <ol>
     *   <li><b>Phase 1a:</b> 1 source handler registered; the 5 lazy leaves only reserve keep-alive refs
     *       (shouldReject=false).</li>
     *   <li><b>Phase 1b / 2a:</b> 5 leaves added to {@code scheduledLeaves}; {@code runCompute} mock fires
     *       {@code segmentListener.onResponse}, then sets {@code shouldReject=true}.</li>
     *   <li><b>Phase 2b:</b> leaf 0 dispatched; {@code notifyIfCancelled} fires synchronously with
     *       {@link org.elasticsearch.tasks.TaskCancelledException}; {@code finishLeaf(leaf0, null,
     *       cancellation)} releases leaf 0's keep-alive ref and notifies {@code childListeners[0]}; then
     *       {@code submitOnDone}'s {@code searchExecutor.execute} throws {@code EsRejectedExecutionException};
     *       {@code failRemainingLeaves} claims leaves 1–4 and calls {@code finishLeaf} for each, releasing
     *       their refs and notifying {@code childListeners[1–4]}; the root {@code ComputeListener} reaches
     *       zero and fires the terminal listener with the first recorded failure
     *       ({@link org.elasticsearch.tasks.TaskCancelledException}).</li>
     * </ol>
     *
     * <p><b>Assertions</b></p>
     * <ul>
     *   <li>The future completes (not hangs) with a {@link org.elasticsearch.tasks.TaskCancelledException}.</li>
     *   <li>{@code cancelQueryOnFailure} fired.</li>
     *   <li>The exchange service is fully empty: no sink handler was ever registered and the root source is
     *       deregistered synchronously before {@code future.get()} returns.</li>
     * </ul>
     */
    public void testExecutorRejectionDrainsRemainingLeaves() throws Exception {
        int leafCount = 5;
        AtomicBoolean shouldReject = new AtomicBoolean(false);
        Executor wrappedExecutor = r -> {
            if (shouldReject.get()) {
                throw new EsRejectedExecutionException("simulated executor shutdown", false);
            }
            threadPool.executor(ThreadPool.Names.SEARCH).execute(r);
        };

        // Phase 1a runs before shouldReject is set (addRemoteSink calls go to the real pool).
        // After runCompute wires the merge, set shouldReject so the first submitOnDone rejects.
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            shouldReject.set(true);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        // executePlan is never called: the task is pre-cancelled, so notifyIfCancelled returns true
        // for every leaf and the cancellation listener fires synchronously without dispatching.

        String sessionId = "test-session";
        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        Configuration config = configuration(new QueryPragmas(Settings.EMPTY));
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        FoldContext foldCtx = new FoldContext(Long.MAX_VALUE);

        TaskCancelHelper.cancel(rootTask, "test cancellation");

        var future = new PlainActionFuture<Result>();
        new SubPlansExecutor(
            computeService,
            exchangeService,
            wrappedExecutor,
            sessionId,
            rootTask,
            new EsqlFlags(false),
            config,
            foldCtx,
            execInfo,
            Map.of()
        ).execute(oneMergeNLeaves(leafCount), null, future);

        // Query must complete (not hang) even when submitOnDone is rejected.
        // The reported cause is whichever failure FailureCollector records first: TaskCancelledException
        // from the cancellation path or EsRejectedExecutionException from failRemainingLeaves.
        var ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), anyOf(instanceOf(TaskCancelledException.class), instanceOf(EsRejectedExecutionException.class)));
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after executor rejection", exchangeFullyEmpty());
    }

    /**
     * Two executors created for the <em>same</em> session id (as happens for real queries: {@code ComputeService.execute}
     * runs once per subquery round plus once for the main plan, all with the request's session id) must not collide in the
     * {@code ExchangeService}, even while the first round's sink handlers are still registered — success-path deregistration
     * is deferred until each handler drains, so it can outlive the first round's completion.
     *
     * <p>The first round's {@code executePlan} stub creates each leaf's {@code ExchangeSink} but never finishes it, parking
     * the deferred deregistration; the future still completes. Pre-fix, exchange ids derived from the bare session id, so
     * the second round's {@code createSinkHandler} threw {@code IllegalStateException "sink exchanger ... already exists"}.
     * The per-executor {@code sessionPrefix} (from {@code newChildSession}) keeps the ids disjoint.
     */
    public void testTwoExecutorsWithSameSessionIdDoNotCollide() throws Exception {
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        List<ExchangeSink> unfinishedSinks = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            unfinishedSinks.add(sinkSupplier.get());
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future1 = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaves(), future1);
        future1.get();
        assertThat(
            "first round's sink handlers must still be registered (deferred deregistration)",
            exchangeService.sinkKeys(),
            hasSize(2)
        );

        // Second round with the same "test-session" id while the first round's handlers are still registered.
        var future2 = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaves(), future2);
        future2.get();

        // Finishing the sinks lets the parked deferred deregistrations fire.
        unfinishedSinks.forEach(ExchangeSink::finish);
        assertBusy(() -> assertTrue("exchange service must be fully empty once all sinks drain", exchangeFullyEmpty()));
    }

    // submitOnDone, no stack overflow
    public void testCompleteManyLeavesDoNotStackOverflow() throws Exception {
        int leafCount = 1000;
        doAnswer(inv -> {
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            listener.onResponse(DriverCompletionInfo.EMPTY);
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(leafCount), future);

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("exchange service must be fully empty after success", exchangeFullyEmpty());
    }

    // profile

    /**
     * The caller's {@link PlanTimeProfile} carries the query-level logical/physical optimization time that
     * {@code EsqlSession} measured, and it reaches the PROFILE response only by being handed to a {@code runCompute}
     * call, which attaches it to that segment's {@code PlanProfile} by reference. The root merge segment is this
     * query's coordinator segment — the union/subquery counterpart of the single-plan {@code SubPlan.Leaf} path — so it
     * must receive the caller's instance, otherwise the query-level planning time is absent from PROFILE output for
     * every union query. Nested merge segments must still get their own instances so each one's {@code PlanProfile}
     * reports only its own local-optimization time instead of an over-counted total.
     */
    public void testRootMergeReceivesCallersPlanTimeProfile() throws Exception {
        Map<String, PlanTimeProfile> segmentProfiles = stubSuccessRecordingSegmentProfiles();

        var queryProfile = new PlanTimeProfile();
        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), new QueryPragmas(Settings.EMPTY), queryProfile, future);
        future.get();

        assertThat(segmentProfiles.keySet(), equalTo(Set.of("main.final", "subplan-0.merge", "subplan-1.merge")));
        assertSame("root merge segment must report the query-level planning time", queryProfile, segmentProfiles.get("main.final"));
        PlanTimeProfile innerA = segmentProfiles.get("subplan-0.merge");
        PlanTimeProfile innerB = segmentProfiles.get("subplan-1.merge");
        assertNotNull(innerA);
        assertNotNull(innerB);
        assertNotSame("nested merge segments must not accumulate into the query-level profile", queryProfile, innerA);
        assertNotSame("nested merge segments must not accumulate into the query-level profile", queryProfile, innerB);
        assertNotSame("each nested merge segment needs its own profile", innerA, innerB);
    }

    /**
     * When the request does not ask for a profile the caller passes {@code null}, and every segment must stay
     * profile-free rather than allocating and reporting empty profiles.
     */
    public void testNullPlanTimeProfilePropagatesToAllSegments() throws Exception {
        Map<String, PlanTimeProfile> segmentProfiles = stubSuccessRecordingSegmentProfiles();

        var future = new PlainActionFuture<Result>();
        buildAndExecute(threeMergesSixLeaves(), future);
        future.get();

        assertThat(segmentProfiles.keySet(), equalTo(Set.of("main.final", "subplan-0.merge", "subplan-1.merge")));
        segmentProfiles.forEach((description, profile) -> assertNull("segment [" + description + "]", profile));
    }

    // Helpers

    /**
     * Stubs {@code runCompute} the way a real coordinator driver behaves: drain the segment's exchange source until it
     * reports EOF, then finish the segment's sink (if it has one) and complete the segment listener. Leaves are stubbed to
     * finish their sink and succeed immediately.
     * <p>
     * This differs from {@link #stubParkUntilExchangeCloses} in the signal a <em>nested</em> merge completes on. That stub
     * parks a segment that owns a sink until the sink itself completes, which only happens once the STOP cascade reaches
     * down from the root. A real nested merge instead completes when its own source EOFs - which happens as soon as all of
     * its leaf children have released their sinks, with no help from the root. Tests that need to observe whether an
     * unstarted merge is ever released have to model that direction, or the stub deadlocks on every code path and hides
     * the distinction being tested.
     */
    private void stubCoordinatorDrivers() {
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            ActionListener<DriverCompletionInfo> listener = ActionListener.notifyOnce(inv.getArgument(6));
            ExchangeSource source = context.exchangeSourceSupplier().get();
            ExchangeSink sink = context.exchangeSinkSupplier() == null ? null : context.exchangeSinkSupplier().get();
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> completeWhenSourceFinished(source, () -> {
                if (sink != null) {
                    sink.finish();
                }
                listener.onResponse(DriverCompletionInfo.EMPTY);
            }));
            return null;
        }).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            Configuration cfg = inv.getArgument(4);
            ActionListener<Result> listener = inv.getArgument(8);
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    private boolean exchangeFullyEmpty() {
        return exchangeService.sinkKeys().isEmpty() && exchangeService.sourceKeys().isEmpty();
    }

    /**
     * Stubs a fully successful run and returns a map that collects, per segment description, the {@link PlanTimeProfile}
     * that segment's {@code runCompute} was given.
     * <p>
     * Unlike the failure-path tests, the merge stub here has to attach and finish the segment's exchange sink the way the
     * real {@code runCompute} does. A nested merge writes into its parent's sink handler, and that handler only completes
     * — releasing the fetcher reference its parent's exchange source holds — once a sink has been attached and finished.
     * Skipping that in an all-success run leaks the reference.
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
        doAnswer(inv -> {
            Supplier<ExchangeSink> sinkSupplier = inv.getArgument(9);
            sinkSupplier.get().finish();
            ActionListener<Result> listener = inv.getArgument(8);
            Configuration cfg = inv.getArgument(4);
            listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null));
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
        return segmentProfiles;
    }

    /**
     * Parks {@code runCompute} and {@code executePlan} on the real exchange objects instead of completing the listeners inline. A merge
     * with a parent sink completes when that sink is finished (STOP closed the parent source); it then {@code finish()}es its own source
     * so nested remotes close. The root merge has no parent sink and waits until its source is {@code finishEarly}'d. Leaves complete when
     * their sink is finished.
     */
    private void stubParkUntilExchangeCloses(CountDownLatch parked) {
        stubParkUntilExchangeClosesOrCancelled(parked, null, null);
    }

    private void stubParkUntilExchangeClosesOrCancelled(CountDownLatch parked, CancellableTask rootTask, AtomicInteger leafDispatches) {
        doAnswer(inv -> {
            ComputeContext context = inv.getArgument(1);
            ActionListener<DriverCompletionInfo> listener = ActionListener.notifyOnce(inv.getArgument(6));
            ExchangeSource source = context.exchangeSourceSupplier().get();
            Runnable succeed = new RunOnce(() -> {
                source.finish();
                listener.onResponse(DriverCompletionInfo.EMPTY);
            });
            Runnable failCancelled = new RunOnce(() -> {
                source.finish();
                listener.onFailure(rootTask.getTaskCancelledException());
            });
            if (rootTask != null) {
                rootTask.addListener(failCancelled::run);
            }
            if (context.exchangeSinkSupplier() != null) {
                context.exchangeSinkSupplier().get().addCompletionListener(ActionListener.running(succeed));
            } else {
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
            ActionListener<Result> listener = ActionListener.notifyOnce(inv.getArgument(8));
            Configuration cfg = inv.getArgument(4);
            Runnable succeed = new RunOnce(
                () -> listener.onResponse(new Result(List.of(), List.of(), null, cfg, DriverCompletionInfo.EMPTY, null, null))
            );
            sinkSupplier.get().addCompletionListener(ActionListener.running(succeed));
            if (rootTask != null) {
                rootTask.addListener(() -> listener.onFailure(rootTask.getTaskCancelledException()));
            }
            parked.countDown();
            return null;
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    private void completeWhenSourceFinished(ExchangeSource source, Runnable onDone) {
        try {
            while (source.isFinished() == false) {
                var page = source.pollPage();
                if (page != null) {
                    page.releaseBlocks();
                    continue;
                }
                if (source.isFinished()) {
                    break;
                }
                IsBlockedResult blocked = source.waitForReading();
                if (blocked.listener().isDone() == false) {
                    blocked.listener()
                        .addListener(
                            ActionListener.running(
                                () -> threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> completeWhenSourceFinished(source, onDone))
                            )
                        );
                    return;
                }
                // Unblocked with no page and not finished: do not recurse on this thread (NOT_BLOCKED would overflow).
                threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> completeWhenSourceFinished(source, onDone));
                return;
            }
        } catch (Exception e) {
            onDone.run();
            return;
        }
        onDone.run();
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

    /**
     * Overload taking a caller-supplied {@link EsqlExecutionInfo}, so a test can put the query into a state - such as
     * already-stopped - before {@code execute} is ever called.
     */
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
            threadPool.executor(ThreadPool.Names.SEARCH),
            sessionId,
            rootTask,
            new EsqlFlags(false),
            config,
            foldCtx,
            execInfo,
            Map.of()
        ).execute(topology, planTimeProfile, future);
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
        String sessionId = "test-session";
        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        Configuration config = configuration(pragmas);
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        FoldContext foldCtx = new FoldContext(Long.MAX_VALUE);
        new SubPlansExecutor(
            computeService,
            exchangeService,
            threadPool.executor(ThreadPool.Names.SEARCH),
            sessionId,
            rootTask,
            new EsqlFlags(false),
            config,
            foldCtx,
            execInfo,
            Map.of()
        ).execute(topology, planTimeProfile, listener);
    }

    /**
     * A minimal two-leaf topology: one Merge with two Leaf children. The Merge plan and leaf plans
     * are stubs — their content does not matter since runCompute and executePlan are mocked.
     * <pre>
     * SubPlan.Merge (root)
     * ├─ SubPlan.Leaf (leafA)
     * └─ SubPlan.Leaf (leafB)
     * </pre>
     */
    private static SubPlan.Merge oneMergeTwoLeaves() {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        return new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)));
    }

    /**
     * A flat topology with {@code n} leaf children under a single root merge.
     * {@code runCompute} is called once (for the root); {@code executePlan} is called n times, one per leaf.
     * <pre>
     * SubPlan.Merge (root)
     * ├─ SubPlan.Leaf (leaf0)
     * ├─ ...
     * └─ SubPlan.Leaf (leafn)
     * </pre>
     */
    private static SubPlan.Merge oneMergeNLeaves(int n) {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        List<SubPlan> leaves = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            leaves.add(new SubPlan.Leaf(stub));
        }
        return new SubPlan.Merge(stub, leaves);
    }

    /**
     * A root merge with one direct leaf and one nested merge holding two leaves:
     * <pre>
     * SubPlan.Merge (root)
     * ├─ SubPlan.Leaf (leafA)
     * └─ SubPlan.Merge (inner)
     *    ├─ SubPlan.Leaf (leafB)
     *    └─ SubPlan.Leaf (leafC)
     * </pre>
     * {@code scheduledLeaves} comes out as [leafA, leafB, leafC], so with {@code branch_parallel_degree=2} the first wave claims leafA
     * and leafB, and leafC is claimed later by the refill leafA's completion submits - on a different thread. leafB and leafC are both
     * under {@code inner}, so whichever of them reaches {@code ensureAncestorsStarted} first starts it and the other one races past the
     * lost CAS. That is the overlap {@link #testMergeStartFailureWhileSiblingLeafIsRunning} needs.
     */
    private static SubPlan.Merge twoMergesThreeLeafs() {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        SubPlan.Merge inner = new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)));
        return new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), inner));
    }

    /**
     * A three-level topology: root merge with two inner merges — innerA has two leaves, innerB has four leaves.
     * {@code runCompute} is called three times: call 0 for root, call 1 for innerA, call 2 for innerB.
     * <pre>
     * SubPlan.Merge (root)              ← runCompute call 0
     * ├─ SubPlan.Merge (innerA)         ← runCompute call 1
     * │  ├─ SubPlan.Leaf (leafA)
     * │  └─ SubPlan.Leaf (leafB)
     * └─ SubPlan.Merge (innerB)         ← runCompute call 2
     *    ├─ SubPlan.Leaf (leafC)
     *    ├─ SubPlan.Leaf (leafD)
     *    ├─ SubPlan.Leaf (leafE)
     *    └─ SubPlan.Leaf (leafF)
     * </pre>
     */
    private static SubPlan.Merge threeMergesSixLeaves() {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        SubPlan.Merge innerA = new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)));
        SubPlan.Merge innerB = new SubPlan.Merge(
            stub,
            List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub), new SubPlan.Leaf(stub), new SubPlan.Leaf(stub))
        );
        return new SubPlan.Merge(stub, List.of(innerA, innerB));
    }
}

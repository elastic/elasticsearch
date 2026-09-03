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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
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
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.anyOf;
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
 * Unit tests for {@code SubPlansExecutor} focused on the exception-unwind path in {@code startMerge}: when a synchronous failure occurs,
 * query cancellation must fire, all listener refs must complete exactly once, and both exchange sinks and sources must be deregistered.
 * <p>
 * {@code ComputeService} cannot be constructed in a unit test (it requires {@code TransportService}, {@code SearchService},
 * {@code ClusterService}, etc.), so it is mocked here with Mockito. This is documented as an AGENTS.md "last resort" use of a mock;
 * everything else (exchange service, task, thread pool) is real.
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

        // startMerge calls computeService.plannerSettings().get() to pass PlannerSettings into runCompute.
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
     * <b>Phase 1 ({@code buildSubPlanContext}):</b> one {@code ExchangeSourceHandler} is registered for root; leafA and leafB only
     * reserve keep-alive refs on it — their {@code ExchangeSinkHandler}s are created lazily when each leaf is dispatched in phase 3.
     *
     * <b>Phase 2 ({@code startMerge}):</b> a {@code ComputeListener} is opened with count 5 (initial + guard + segmentListener
     * + childListeners[0] + childListeners[1]). {@code runCompute} mock immediately calls
     * {@code segmentListener.onResponse(DriverCompletionInfo.EMPTY)} — count 4. Then {@code startChildContext} adds leafA and leafB
     * to {@code scheduledLeaves}. {@code guard.onResponse(null)} — count 3. Try-with-resources closes
     * {@code ComputeListener} — count 2. Only {@code childListeners[0]} and {@code childListeners[1]} are still outstanding.
     *
     * <b>Phase 3 (leaf dispatch):</b> {@code tryExecuteNextLeaf} dispatches each leaf in turn; {@code ParentSink.attach} registers
     * the leaf's {@code ExchangeSinkHandler} and wires it into root's source. For each leaf, the {@code executePlan} mock:
     * <ol>
     *   <li>Calls {@code sinkSupplier.get()} to create the {@code ExchangeSink} from the just-registered handler.</li>
     *   <li>Calls {@code sink.finish()} — marks the sink as done. The handler fires its completion listener synchronously
     *       because the sink is already finished when {@code finishParentSink} later calls
     *       {@code addCompletionListener}.</li>
     *   <li>Calls {@code listener.onResponse(result)} → {@code finishLeaf} → {@code finishParentSink(leafX.parentSink, null)}
     *       → {@code addCompletionListener} fires immediately → {@code exchangeService.finishSinkHandler} deregisters the
     *       sink → {@code childListeners[X].onResponse(completionInfo)} → count decrements.</li>
     * </ol>
     * After both leaves complete, count reaches 0 with no recorded failure. {@code ComputeListener} fires:
     * <ul>
     *   <li>{@code cancelQueryOnFailure} is <em>not</em> invoked — the failure collector is empty.</li>
     *   <li>{@code terminalListener.onResponse(profiles)} → {@code removeExchangeSource(root)} deregisters root's source
     *       handler; {@code finishParentSink(null, null)} is a no-op; {@code completionListener.onResponse(profiles)}
     *       → {@code future.onResponse(result)}.</li>
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
        // then calls the result listener. finishParentSink adds a completion listener to the sink
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaf(), future);

        // Get with a short timeout — the test should complete synchronously.
        future.get();

        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("exchange service must be fully empty after success", exchangeFullyEmpty());
    }

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
     * <b>Phase 1 ({@code buildSubPlanContext}):</b> one {@code ExchangeSourceHandler} is registered for root, keyed
     * {@code "test-session"}. LeafA and leafB get lazy {@code ParentSink}s that only hold keep-alive refs on root's source;
     * no sink handlers are registered for them.
     *
     * <b>Phase 2 ({@code startMerge}):</b> a {@code ComputeListener} is opened and five refs are acquired before the inner try block:
     * <pre>
     *   initial ref (1)       owned by ComputeListener itself
     *   guard         (1)     held by startMerge for the duration of its body
     *   segmentListener (1)   for runCompute
     *   childListeners[0] (1) for leafA
     *   childListeners[1] (1) for leafB
     *                    ─────
     *   count = 5
     * </pre>
     * {@code runCompute} throws immediately — the loop that would call {@code startChildContext} (and add leaves to
     * {@code scheduledLeaves}) is never reached. The catch block runs:
     * <ol>
     *   <li>{@code segmentListener.onFailure(e)} — count 4; failure recorded in {@code FailureCollector}.</li>
     *   <li>{@code childListeners[0].onFailure(e)} — count 3.</li>
     *   <li>{@code childListeners[1].onFailure(e)} — count 2.</li>
     *   <li>{@code abortChildren(root, e)} — calls {@code finishParentSink} with the failure for both leafA and leafB;
     *       their sinks were never registered (lazy), so this just releases their keep-alive refs on root's source.</li>
     *   <li>{@code guard.onFailure(e)} (in {@code finally}) — count 1.</li>
     * </ol>
     * The try-with-resources closes {@code ComputeListener}: initial ref released, count 0. Reaching zero fires:
     * <ul>
     *   <li>{@code cancelQueryOnFailure.run()} → {@code cancelled.set(true)}</li>
     *   <li>{@code terminalListener.onFailure(e)} → {@code removeExchangeSource(root)} deregisters root's source handler;
     *       {@code finishEarly} drains the source; {@code completionListener.onFailure(e)} → {@code future.onFailure(e)}</li>
     * </ul>
     *
     * <b>Phase 3 (leaf dispatch):</b> {@code scheduledLeaves} is empty — {@code startChildContext} was never reached before
     * the throw — so no leaves are dispatched.
     */
    public void testOneMergeTwoLeafMergeFailureInRunCompute() {
        var injected = new RuntimeException("injected planning failure");
        // runCompute throws, simulating a synchronous planning-time error.
        doThrow(injected).when(computeService).runCompute(any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaf(), future);

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
     * Verifies that an asynchronous failure from the first leaf's {@code executePlan} call propagates cleanly to the root while the
     * second leaf completes successfully.
     *
     * <p><b>Why listener.onFailure</b></p>
     * This test covers the asynchronous failure path: {@code executePlan} accepts the dispatch and later reports the failure
     * through its {@code ActionListener}. The synchronous-throw path ({@code executePlan} throwing before returning, which
     * {@code executeLeaf} catches and routes to {@code finishLeaf}) is covered separately by
     * {@link #testSynchronousExecutePlanFailureFirstWaveFailsCleanly} and
     * {@link #testSynchronousExecutePlanFailureOnRefillWaveFailsCleanly}.
     *
     * <p><b>Setup</b></p>
     * The {@code twoLeafTopology} has one root merge and two direct leaf children. {@code runCompute} is called once (for the root
     * merge) and completes its listener immediately. The first {@code executePlan} call (leaf index 0) calls
     * {@code listener.onFailure(injected)} without creating a sink; the second call (leaf index 1) finishes the sink and calls
     * {@code listener.onResponse}.
     *
     * <p><b>Walk-through</b></p>
     * <ol>
     *   <li><b>Phase 2:</b> {@code runCompute(root)} succeeds; both leaves are added to {@code scheduledLeaves}.</li>
     *   <li><b>Phase 3:</b> leaf 0 is dispatched first. The stub calls {@code listener.onFailure(injected)}.
     *       {@code finishLeaf(leaf0, null, injected)} runs: {@code finishParentSink} deregisters leaf 0's sink handler immediately
     *       (failure path skips the async drain), and {@code childListeners[0].onFailure(injected)} propagates to root's
     *       {@code ComputeListener}, which records the failure, fires {@code cancelQueryOnFailure}, and decrements the count.
     *       The {@code onDone} callback then claims leaf 1. Leaf 1's stub finishes its sink and calls {@code listener.onResponse};
     *       success propagates to {@code childListeners[1].onResponse}, decrementing root's count again.</li>
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaf(), future);

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
     *   <li><b>Phase 2:</b> {@code runCompute(root)} succeeds; all eight leaves are added directly to {@code scheduledLeaves}
     *       as root's children.</li>
     *   <li><b>Phase 3 — dispatch leaves:</b> leaves are dispatched one at a time via {@code executePlan}.
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

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
     * {@code runCompute} is called three times: call 0 for root (always succeeds), call 1 for innerA, call 2 for innerB.
     * A randomly chosen inner merge ({@code failingCall ∈ {1, 2}}) throws synchronously; the other succeeds and its
     * leaves are dispatched via {@code executePlan} (2 leaves if innerA succeeds, 4 if innerB succeeds).
     *
     * <p><b>Walk-through — failing inner merge is innerA (call 1)</b></p>
     * <ol>
     *   <li><b>Phase 2:</b> root's {@code runCompute} succeeds. {@code startMerge(innerA)} throws on call 1 — innerA's
     *       catch block aborts leafA and leafB (sinks immediately deregistered), fires innerA's terminal listener
     *       ({@code cancelQueryOnFailure}, deregisters innerA's source and sink,
     *       {@code childListeners[0].onFailure} → root count decrements). {@code startMerge(innerB)} succeeds on call 2 —
     *       leafC–leafF added to {@code scheduledLeaves}.</li>
     *   <li><b>Phase 3:</b> leafC–leafF dispatched; each completes normally → innerB's child listeners drain →
     *       innerB count → 0 → innerB's terminal listener deregisters innerB's source and sink,
     *       {@code childListeners[1].onResponse} → root count → 0.</li>
     *   <li><b>Terminal firing:</b> root fires with the recorded failure from innerA.</li>
     * </ol>
     * The walk-through is symmetric when innerB fails (call 2): innerA's 2 leaves are dispatched instead.
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(nestedMerges(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
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
     *   <li><b>Phase 2:</b> all three {@code runCompute} calls succeed; leafA–leafF are added to
     *       {@code scheduledLeaves} (leafA/B for innerA, leafC–F for innerB).</li>
     *   <li><b>Phase 3:</b> leaves dispatched one at a time.
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(nestedMerges(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected leaf failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after nested leaf failure", exchangeFullyEmpty());
    }

    /**
     * {@code cancelQueryOnFailure} must be called exactly once per query regardless of nesting depth.
     *
     * <p>The {@code nestedMerges()} topology has three merge nodes (root, innerA, innerB). After a failure in innerA's {@code runCompute},
     * this test verifies that {@code cancelQueryOnFailure} was invoked exactly once on the {@code ComputeService} mock.
     */
    public void testNestedMergeFailureCancelsQueryExactlyOnce() {
        var injected = new RuntimeException("injected inner failure");
        var callCount = new AtomicInteger();
        doAnswer(inv -> {
            int call = callCount.getAndIncrement();
            ActionListener<DriverCompletionInfo> listener = inv.getArgument(6);
            if (call == 1) { // innerA fails
                throw injected;
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(nestedMerges(), future);

        expectThrows(ExecutionException.class, future::get);
        // cancelQueryOnFailure must be called exactly once — not once per merge node (which would be 3).
        verify(computeService, times(1)).cancelQueryOnFailure(any());
    }

    /**
     * Async STOP only calls {@link ExchangeService#finishSessionEarly} with the bare session id, which is the root merge
     * source. Nested merge sources and leaf sinks are not looked up. They must finish because closing the root source
     * closes the remotes that feed it, each nested merge stub then {@code finish()}es its own source (the real driver
     * does the same when its parent sink is done), and that closes the nested leaves.
     * <p>
     * {@code runCompute}/{@code executePlan} stay parked on those exchange objects instead of completing themselves.
     * If the cascade is broken the future never completes. All six leaves are in flight so every leaf sink is a remote
     * of a nested source at STOP time.
     */
    public void testFinishSessionEarlyUnblocksParkedNestedTree() throws Exception {
        CountDownLatch parked = new CountDownLatch(3 + 6); // root + innerA + innerB, and six leaves
        stubParkUntilExchangeCloses(parked);

        StartedQuery started = startQuery(nestedMerges(), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 8).build()));
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
     * Those leaves are invisible to the first close. They must still complete when later {@code attach}es see
     * {@code buffer.noMoreInputs()} on the already-finished parent source and finish the new sink immediately.
     * Pre-fix / a broken attach would leave the query waiting on a leaf that never observes STOP.
     */
    public void testFinishSessionEarlyUnblocksQueuedNestedLeaves() throws Exception {
        CountDownLatch parked = new CountDownLatch(3 + 1);
        stubParkUntilExchangeCloses(parked);

        StartedQuery started = startQuery(nestedMerges(), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()));
        assertTrue("root, both inner merges, and the one dispatched leaf must park", parked.await(10, TimeUnit.SECONDS));
        assertThat(exchangeService.sinkKeys(), hasSize(3)); // innerA sink, innerB sink, one leaf
        assertThat(
            "queued leaves must not have sink handlers yet",
            exchangeService.sinkKeys(),
            not(hasItem("test-session/1/subplan-0.subplan-1"))
        );

        PlainActionFuture<Boolean> stopped = new PlainActionFuture<>();
        exchangeService.finishSessionEarly(started.sessionId, stopped);
        assertTrue(stopped.get(10, TimeUnit.SECONDS));

        started.future.get(10, TimeUnit.SECONDS);
        assertFalse("STOP is a graceful finishEarly, not a failure", cancelled.get());
        assertBusy(() -> assertTrue("queued leaves must not leak handlers after STOP", exchangeFullyEmpty()));
    }

    /**
     * Mid-flight cancel of a nested tree: one leaf is already in {@code executePlan}, the rest are queued.
     * {@code executeLeaf} skips the queue via {@code notifyIfCancelled}. In-flight merge/leaf stubs complete when
     * the task is cancelled, the same way a real driver checks {@code CancellableTask}. Pre-cancel tests never
     * reach {@code executePlan}; this one does, then cancels.
     */
    public void testCancelAfterNestedLeafDispatched() throws Exception {
        CountDownLatch parked = new CountDownLatch(3 + 1);
        AtomicInteger leafDispatches = new AtomicInteger();
        CancellableTask rootTask = new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of());
        stubParkUntilExchangeClosesOrCancelled(parked, rootTask, leafDispatches);

        StartedQuery started = startQuery(
            nestedMerges(),
            new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()),
            null,
            rootTask
        );
        assertTrue("root, both inner merges, and the one dispatched leaf must park", parked.await(10, TimeUnit.SECONDS));
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

        // runCompute must succeed so all leaves are added to scheduledLeaves before Phase 3 begins.
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
     * <p>This test dispatches 1000 leaves and verifies that the query completes.
     */
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(leafCount), future);

        future.get();
        assertFalse("cancelQueryOnFailure must not fire on success", cancelled.get());
        assertTrue("exchange service must be fully empty after success", exchangeFullyEmpty());
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
     * the end of Phase 2. This timing ensures that the first {@code submitOnDone} call from the
     * cancellation path is rejected.
     *
     * <p><b>Walk-through</b></p>
     * <ol>
     *   <li><b>Phase 1:</b> 1 source handler registered; the 5 lazy leaves only reserve keep-alive refs
     *       (shouldReject=false).</li>
     *   <li><b>Phase 2:</b> {@code runCompute} mock fires {@code segmentListener.onResponse}, then
     *       sets {@code shouldReject=true}; 5 leaves added to {@code scheduledLeaves}.</li>
     *   <li><b>Phase 3:</b> leaf 0 dispatched; {@code notifyIfCancelled} fires synchronously with
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

        // Phase 1 runs before shouldReject is set (addRemoteSink calls go to the real pool).
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
     * Regression test for the inactive-sink-reaper data-loss bug: leaf sink handlers must be registered lazily at dispatch
     * time, not eagerly in Phase 1. An eagerly registered handler for a leaf queued behind {@code branchParallelDegree} has
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(3), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()), future);

        // Phase 3 dispatched exactly one leaf synchronously; the two queued leaves must have no sink handler
        // (pre-fix: all three were registered in Phase 1 and hasSize(1) fails with 3).
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

    /**
     * A synchronous throw from {@code executePlan} during the initial (first-wave) dispatch — which runs inline on the
     * calling thread — must be caught by {@code executeLeaf} and routed to {@code finishLeaf}, failing the query cleanly:
     * no hang, cancellation fired, and the sink handler that {@code ParentSink.attach} registered just before the throw
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
            .executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaf(), future);

        var ex = expectThrows(ExecutionException.class, future::get);
        assertEquals("injected synchronous executePlan failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertTrue("exchange service must be fully empty after synchronous failure", exchangeFullyEmpty());
    }

    /**
     * A synchronous throw from {@code executePlan} on a <em>refill</em> dispatch — which runs inside the refill runnable on
     * the search pool, where an escaping exception would be swallowed by the executor — must also be caught by
     * {@code executeLeaf}. Pre-fix, this scenario left the leaf's {@code ComputeListener} ref permanently unreleased and the
     * query hung forever.
     *
     * <p>With {@code branch_parallel_degree=1} and two leaves, the first {@code executePlan} call completes inline (so the
     * refill claims leaf 1 on a search-pool thread) and the second call throws.
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeNLeaves(2), new QueryPragmas(Settings.builder().put("branch_parallel_degree", 1).build()), future);

        // A bounded get: pre-fix the future never completes and the test would otherwise hang.
        var ex = expectThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        assertEquals("injected refill-wave failure", ex.getCause().getMessage());
        assertTrue("cancelQueryOnFailure must have fired", cancelled.get());
        assertBusy(() -> assertTrue("exchange service must be fully empty after refill failure", exchangeFullyEmpty()));
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());

        var future1 = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaf(), future1);
        future1.get();
        assertThat(
            "first round's sink handlers must still be registered (deferred deregistration)",
            exchangeService.sinkKeys(),
            hasSize(2)
        );

        // Second round with the same "test-session" id while the first round's handlers are still registered.
        var future2 = new PlainActionFuture<Result>();
        buildAndExecute(oneMergeTwoLeaf(), future2);
        future2.get();

        // Finishing the sinks lets the parked deferred deregistrations fire.
        unfinishedSinks.forEach(ExchangeSink::finish);
        assertBusy(() -> assertTrue("exchange service must be fully empty once all sinks drain", exchangeFullyEmpty()));
    }

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
        buildAndExecute(nestedMerges(), new QueryPragmas(Settings.EMPTY), queryProfile, future);
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
        buildAndExecute(nestedMerges(), future);
        future.get();

        assertThat(segmentProfiles.keySet(), equalTo(Set.of("main.final", "subplan-0.merge", "subplan-1.merge")));
        segmentProfiles.forEach((description, profile) -> assertNull("segment [" + description + "]", profile));
    }

    // Helpers

    /**
     * True once both halves of the exchange are clean. {@link ExchangeService#isEmpty()} only inspects sinks, so asserting on it
     * alone would pass while a source handler stayed registered - including the root's, which async stop looks up by session id.
     */
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
        return segmentProfiles;
    }

    /**
     * Parks {@code runCompute} and {@code executePlan} on the real exchange objects instead of completing the listeners
     * inline. A merge with a parent sink completes when that sink is finished (STOP closed the parent source); it then
     * {@code finish()}es its own source so nested remotes close. The root merge has no parent sink and waits until its
     * source is {@code finishEarly}'d. Leaves complete when their sink is finished.
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
        }).when(computeService).executePlan(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
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

    private record StartedQuery(PlainActionFuture<Result> future, String sessionId, CancellableTask rootTask) {}

    private StartedQuery startQuery(SubPlan.Merge topology, QueryPragmas pragmas) {
        return startQuery(topology, pragmas, null, new CancellableTask(1, "esql", "esql", "test", TaskId.EMPTY_TASK_ID, Map.of()));
    }

    private StartedQuery startQuery(
        SubPlan.Merge topology,
        QueryPragmas pragmas,
        PlanTimeProfile planTimeProfile,
        CancellableTask rootTask
    ) {
        String sessionId = "test-session";
        Configuration config = configuration(pragmas);
        EsqlExecutionInfo execInfo = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
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
        return new StartedQuery(future, sessionId, rootTask);
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
    private static SubPlan.Merge oneMergeTwoLeaf() {
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
    private static SubPlan.Merge nestedMerges() {
        PhysicalPlan stub = new LocalSourceExec(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
        SubPlan.Merge innerA = new SubPlan.Merge(stub, List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub)));
        SubPlan.Merge innerB = new SubPlan.Merge(
            stub,
            List.of(new SubPlan.Leaf(stub), new SubPlan.Leaf(stub), new SubPlan.Leaf(stub), new SubPlan.Leaf(stub))
        );
        return new SubPlan.Merge(stub, List.of(innerA, innerB));
    }
}

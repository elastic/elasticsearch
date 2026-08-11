/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes the sub plans of one {@code MergeExec} level (fork branches or subqueries) and feeds their output into the
 * exchange source consumed by the plan segment above them. The parallel degree is controlled by the
 * {@code BRANCH_PARALLEL_DEGREE} pragma.
 * <p>
 * A sub plan may itself contain nested {@code MergeExec}s — a subquery of a subquery. Those are broken apart and run by
 * a nested {@code MergeLevelExecutor}, recursing for as long as the plan keeps nesting. Three invariants hold this
 * together and are easy to break when editing:
 * <ul>
 * <li>
 *     <b>All sub plan listeners are acquired up front</b>, in the constructor, before any sub plan starts. Otherwise the
 *     {@link ComputeListener} ref count could drop to zero — closing it — when an early sub plan fails before a later
 *     one has been dispatched. The counterpart is that every index {@link #tryExecuteNextSubPlan} claims must go on to
 *     resolve its listener: those references are already held, so one that is never resolved stalls the refill chain and
 *     leaves the level's {@code ComputeListener} — and the exchange source handler its completion removes — outstanding.
 * </li>
 * <li>
 *     <b>An empty sink keeps the exchange source alive</b> for the lifetime of this executor, and is released only once
 *     every sub plan has completed. Without it the source could finish between batches, while sub plans are still to
 *     come. For the nested case this means the nested executor must be constructed — registering its empty sink —
 *     <em>before</em> the segment plan that reads from the nested source starts running.
 * </li>
 * <li>
 *     <b>One {@link SubPlanTaskRunner} is shared by every executor in the query</b>, at every nesting depth, via
 *     {@link QueryContext}. Each executor independently opens a window of {@code branchParallelDegree} sub plans, so
 *     without a shared runner the number of concurrently executing leaf plans would be
 *     {@code branchParallelDegree}<sup>depth</sup>. The runner enforces the ceiling globally.
 * </li>
 * <li>
 *     <b>Only leaf sub plans are submitted to the runner</b>; a sub plan with nested merges is dispatched directly by
 *     {@link #executeSubPlanWithNestedSubPlans}. This is a correctness requirement, not a shortcut: a merge sub plan
 *     consumes what the leaves beneath it produce, so giving it a permit would make it wait for leaves queued behind that
 *     permit — a deadlock at the default {@code branch_parallel_degree} of 2. The trade-off is that merge sub plan
 *     expansion itself is ungated, so up to {@code branchParallelDegree}<sup>depth</sup> nesting levels may be open at
 *     once, each holding an exchange source, a sink and a coordinator driver — and, because bypassing the runner also
 *     means bypassing its executor, that expansion runs on whichever thread refilled the window. See
 *     {@link #executeSubPlanWithNestedSubPlans} for why that thread is an acceptable place to do it.
 * </li>
 * </ul>
 *
 * <h2>Worked example: three nested merges</h2>
 *
 * Take a plan with three nested {@code MergeExec}s where the outer two each have one leaf branch and one merge branch, and the
 * innermost has two leaf branches because it cannot nest any further:
 *
 * <pre>
 * M1
 * |-- L1          leaf
 * `-- M2
 *     |-- L2      leaf
 *     `-- M3
 *         |-- L3  leaf
 *         `-- L4  leaf
 * </pre>
 *
 * Below, the query's session id is {@code S} and {@code branch_parallel_degree} is 2. Child session suffixes are written
 * {@code S/1}, {@code S/2}, ... in allocation order; {@link ComputeService#newChildSession} draws from a node-wide counter, so real
 * suffixes increase but are not contiguous.
 *
 * <h3>Ids are flat; only profiles record the nesting</h3>
 *
 * Every child session is derived from the <em>query's</em> session, never from the enclosing sub plan's, because
 * {@link QueryContext} is passed to nested executors unchanged and {@code newChildSession(context.sessionId())} therefore always
 * starts from {@code S}. Session ids carry no nesting information at all. The profile qualifier does: each level appends to
 * {@code profilePrefix}, so L3 is profiled as {@code subplan-1.subplan-1.subplan-0}, and a merge segment adds a {@code .merge}
 * suffix via {@code ComputeService#profileDescription}. When reading a profile, the qualifier is the tree; the session id is not.
 *
 * <h3>What gets created, and where the data flows</h3>
 *
 * Three exchange <em>sources</em> are registered, one per merge level, each consuming the sinks of that level's branches:
 * <ul>
 * <li>{@code S} - the main source, registered by {@code ComputeService.execute} under the <em>root</em> session id rather than the
 *     main segment's own {@code S/1}. It consumes {@code S/2} (L1's output) and {@code S/3} (M2's segment output).</li>
 * <li>{@code S/4} - M2's nested source. Consumes {@code S/5} (L2) and {@code S/6} (M3's segment output).</li>
 * <li>{@code S/7} - M3's nested source. Consumes {@code S/8} (L3) and {@code S/9} (L4).</li>
 * </ul>
 * Six sinks are opened, one per branch. A leaf's sink is opened by {@link SubPlan#execute} when the runner dispatches it, whereas a
 * merge branch's sink is opened here, up front, because it starts immediately - see {@link #openSink}. The two merge segments are
 * coordinator-only pipelines that bridge a level to the one above: segment M2 reads {@code S/4} and writes {@code S/3}; segment M3
 * reads {@code S/7} and writes {@code S/6}; the main segment reads {@code S} and hands pages to {@code OutputExec}.
 *
 * <h3>Listeners</h3>
 *
 * Each level has its own {@link ComputeListener} holding three references: one per branch, plus one for the level's own segment
 * plan. The per-branch references are the {@code subPlanListeners} acquired in this class's constructor; the segment's is acquired
 * where {@code runCompute} is called. A level completes only when all three are released, at which point its
 * {@code completionListener} fires: it first removes that level's nested exchange source (it is wrapped in
 * {@code ActionListener.runBefore}), then arranges for the parent-facing sink to be finished once it has drained, then resolves the
 * parent's branch listener. So completion propagates upward one level at a time: M3 -> M2 -> the query.
 * <p>
 * Also live per level: one {@code emptySinkRef} holding that level's source open until all its branches have completed, and per
 * leaf, the runner's own {@code completion} listener, whose sole job is to release the leaf's permit. Query-wide, {@code rootTask}
 * has a cancellation listener that fails the shared {@link SubPlanTaskRunner}, which drains and rejects anything still queued.
 * When the main segment finishes because it already has enough rows ({@code LIMIT}), {@code ComputeService} calls
 * {@link SubPlanTaskRunner#finish()}, which skips queued leaves. {@link #tryExecuteNextSubPlan} also checks
 * {@link SubPlanTaskRunner#finished()} so an unstarted merge is resolved as empty success rather than expanded.
 *
 * <h3>Order of submission to the runner</h3>
 *
 * The whole expansion is synchronous. {@code execute(2)} on the outermost executor returns only after all three executors exist,
 * all three nested sources are registered, both merge segments are running, and all four leaves have been submitted:
 * <ol>
 * <li>Outer executor, branch 0: L1 is a leaf, so it is submitted. {@code running} 0 -> 1, admitted, dispatched.</li>
 * <li>Outer executor, branch 1: M2 is a merge, so it bypasses the runner. Its sink {@code S/3} is opened, source {@code S/4} is
 *     registered, segment M2 starts, and a nested executor is built over {@code [L2, M3]} - a merge branch becomes another
 *     submitter, not a task.</li>
 * <li>M2's executor, branch 0: L2 is a leaf, submitted. {@code running} 1 -> 2, admitted, dispatched.</li>
 * <li>M2's executor, branch 1: M3 is a merge - sink {@code S/6}, source {@code S/7}, segment M3, another nested executor over
 *     {@code [L3, L4]}.</li>
 * <li>M3's executor, branch 0: L3 is a leaf, submitted - but {@code running == maxRunning}, so it <b>waits in the queue</b>.</li>
 * <li>M3's executor, branch 1: L4 is a leaf, submitted - also <b>queued</b>.</li>
 * </ol>
 * Submission order is therefore L1, L2, L3, L4; L1 and L2 hold the two permits and L3, L4 wait. Whichever of L1 or L2 finishes
 * first releases a permit, and the queue is FIFO across the whole query rather than per level, so L3 is admitted next even though it
 * belongs to the deepest level - and only then does it open its sink {@code S/8}.
 */
final class MergeLevelExecutor {

    private static final Logger LOGGER = LogManager.getLogger(MergeLevelExecutor.class);

    /**
     * The state that is fixed for a whole query and therefore identical for every {@link MergeLevelExecutor} at every
     * nesting depth. Only the sub plans, the compute listener, the exchange source and the profile prefix differ
     * between a parent executor and the nested executors it spawns.
     */
    record QueryContext(
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        QueryPragmas queryPragmas,
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses,
        SubPlanTaskRunner subPlanTaskRunner
    ) {}

    private final ComputeService computeService;
    private final QueryContext context;
    private final List<PhysicalPlan> subplans;
    private final List<ActionListener<DriverCompletionInfo>> subPlanListeners;
    private final ExchangeSourceHandler exchangeSource;
    private final String profilePrefix;
    private final AtomicInteger nextId = new AtomicInteger();
    private final AtomicInteger completedSubPlanCount = new AtomicInteger();
    private final Releasable emptySinkRef;

    /**
     * Binds this level to {@code subplans} and the exchange source they feed. Does not start any branch: {@link #execute} does that.
     * Two things must happen here, before any branch runs:
     * <ul>
     * <li>an empty sink is registered on {@code exchangeSource}, so the source cannot finish between the last sink of one batch
     *     and the first sink of the next;</li>
     * <li>one {@link ComputeListener} reference is acquired per branch, so an early failure cannot drop the listener's ref count
     *     to zero and close it before later branches are dispatched.</li>
     * </ul>
     * Each acquired listener is wrapped in {@code runAfter(subPlanCompleted)} and {@code notifyOnce}: completion accounting and
     * listener resolution are one event. A throw still counts (otherwise {@code emptySinkRef} is never released and the query
     * hangs); a double report cannot overshoot the tally.
     * <p>
     * {@code profilePrefix} is {@code null} for the outermost level ({@code ComputeService.execute}) and the enclosing branch's
     * qualifier for a nested executor, so L3 under M2 under M1 is profiled as {@code subplan-1.subplan-1.subplan-0}.
     */
    MergeLevelExecutor(
        ComputeService computeService,
        QueryContext context,
        List<PhysicalPlan> subplans,
        ComputeListener computeListener,
        ExchangeSourceHandler exchangeSource,
        String profilePrefix
    ) {
        this.computeService = computeService;
        this.context = context;
        this.subplans = subplans;
        this.exchangeSource = exchangeSource;
        this.profilePrefix = profilePrefix;
        // Assigned before the listeners below, which capture `this`: subPlanCompleted() reads emptySinkRef.
        this.emptySinkRef = Releasables.releaseOnce(exchangeSource.addEmptySink());
        // Pre-acquire all subplan listeners upfront so that the ComputeListener's ref count accounts for all subplans
        // before any execution begins. This prevents the ComputeListener from closing prematurely if early subplans
        // finish with errors before later ones are started.
        //
        // Completion accounting hangs off the listener rather than being invoked separately, so that "this sub plan
        // reached a terminal state" and "the tally knows about it" are a single event that cannot drift apart:
        // - runAfter counts in a finally, so a listener that throws is still counted. A sub plan that goes uncounted
        // leaves emptySinkRef unreleased and hangs the query until it times out.
        // - notifyOnce counts at most once, so a sub plan reported twice - the hand-off at the end of
        // tryExecuteNextSubPlan resolves the listener itself, and a throw after that point reaches the catch there,
        // which reports it again - cannot overshoot the tally and skip the equality test in subPlanCompleted().
        // Resolution and completion are one-to-one today; anything that resolved a sub plan's listener early - to report
        // partial results, say - would also count it early.
        this.subPlanListeners = new ArrayList<>(subplans.size());
        for (int i = 0; i < subplans.size(); i++) {
            subPlanListeners.add(
                ActionListener.notifyOnce(ActionListener.runAfter(computeListener.acquireCompute(), this::subPlanCompleted))
            );
        }
    }

    /**
     * Opens a window of {@code branchParallelDegree} sub plans on this level. Each call to {@link #tryExecuteNextSubPlan} either
     * submits a leaf to the shared runner or starts a merge immediately; when a branch later completes, {@link #subPlanCompleted}
     * opens the next one, so the window stays full until every branch is accounted for.
     * <p>
     * This method is synchronous. For the three-level example in the class javadoc, {@code execute(2)} on M1 returns only after
     * M2 and M3 exist, both merge segments are running, and all four leaves have been submitted. It does not wait for those
     * leaves to finish.
     */
    void execute(int branchParallelDegree) {
        for (int i = 0; i < branchParallelDegree; i++) {
            tryExecuteNextSubPlan();
        }
    }

    /**
     * Fails every branch that has not been claimed yet so their pre-acquired {@link ComputeListener} references are released.
     * <p>
     * The constructor acquires one listener per branch before any of them run. {@link #execute} only claims
     * {@code branchParallelDegree} of those; the rest stay unused until a completion refills the window. If setup fails after
     * this executor exists and before every index has been claimed — {@code runCompute} throwing before its own try, constructing
     * the segment {@code ComputeContext}, {@link #execute} itself, or the setup listener's success callback — those unused
     * references would never be resolved. The level's {@link ComputeListener} would never reach zero, its
     * {@code completionListener} would never fire, and the nested exchange source handler would stay registered for the
     * node's lifetime.
     * <p>
     * Advances {@code nextId} to the end first so a {@link #subPlanCompleted} refill cannot start a new branch while this is
     * discharging the unused listeners. Already-claimed branches are left alone: they resolve through their own execute/skip/fail
     * path. {@link ActionListener#notifyOnce} absorbs a second report if a claimed index races with this method.
     * <p>
     * Called from the setup {@code catch} in {@link #executeSubPlanWithNestedSubPlans} and {@code ComputeService#execute}.
     */
    void abortUndispatched(Exception e) {
        context.subPlanTaskRunner().fail(e);
        int start = nextId.getAndSet(subplans.size());
        for (int i = start; i < subplans.size(); i++) {
            subPlanListeners.get(i).onFailure(e);
        }
    }

    /**
     * Claims the next unused index in {@code subplans} and starts that branch, or returns if none remain. Called from
     * {@link #execute} to fill the initial window and from {@link #subPlanCompleted} to refill it.
     *
     * <h4>A claimed index always resolves its listener</h4>
     *
     * Every path below either resolves {@code subPlanListener} or hands it to something that will, and the {@code catch}
     * discharges it from a {@code finally}. This is the one invariant the method exists to hold, and it reaches well beyond this
     * class: the branch listeners are pre-acquired in the constructor, so a claimed index that never resolves stops
     * {@link #subPlanCompleted} from ever running, nothing then claims the remaining indices, this level's {@link ComputeListener}
     * never reaches zero references, and its {@code completionListener} - which carries {@code removeExchangeSourceHandler} in a
     * {@code runBefore} - never fires. One missed resolution therefore hangs the query <em>and</em> leaks an exchange source
     * handler for the node's lifetime, at this level or, for the outermost one, in {@link ComputeService#execute}.
     * <p>
     * Once a resolution has <em>started</em> the accounting is safe without further help: {@code notifyOnce} claims its delegate
     * before invoking it and {@code runAfter} counts in a {@code finally}, so a listener chain that throws part way through has
     * still released its reference and still counted. The gap this guards is only the listener that is never touched at all.
     * As in {@code SubPlanTaskRunner}, the guard covers {@link Exception}; an {@link Error} still escapes.
     * <p>
     * Failure and {@link SubPlanTaskRunner#finished() finished} are checked <em>before</em> allocating a child session or
     * opening a sink. A failed query fails the branch listener; a finished query ({@code LIMIT} already satisfied) resolves it
     * as empty success, the same way {@link SubPlan#skip} does. Merge plans never enter the runner's queue, so without this
     * finished check they would still expand after the main segment had enough rows.
     * <p>
     * {@link PlannerUtils#breakPlanIntoSubPlansAndMainPlan} then decides the shape: a nested {@code MergeExec} is started by
     * {@link #executeSubPlanWithNestedSubPlans} (sink opened now, because the merge starts now); a leaf is {@link
     * SubPlanTaskRunner#submit submitted} and opens its sink only when the runner dispatches it.
     * <p>
     * Example: this level has {@code [L1, M2, L5]}, {@code branch_parallel_degree = 2}. The first call submits L1; the second
     * starts M2 immediately. L5 waits until {@link #subPlanCompleted} after L1 or M2 finishes.
     */
    void tryExecuteNextSubPlan() {
        int subPlanIndex = nextId.getAndIncrement();
        if (subPlanIndex >= subplans.size()) {
            return;
        }
        var subPlanListener = subPlanListeners.get(subPlanIndex);
        // Everything from here on is inside the try: an index claimed from nextId must resolve its listener before this method
        // returns or throws - see the javadoc above for what breaks if it does not. Assigned inside because allocating the child
        // session is itself part of the guarded region.
        String childSessionId = null;
        try {
            Exception failure = context.subPlanTaskRunner().failure();
            if (failure != null) {
                subPlanListener.onFailure(failure);
                return;
            }
            // finish() skips queued leaves in the runner; merge sub plans never enter the queue, so they are skipped
            // here. Resolving success (not failure) lets emptySinkRef be released the same way SubPlan.skip does.
            // Same TOCTOU as failure(): a merge that passes this check may still start if finish() races in afterwards,
            // which is equivalent to it starting just before LIMIT completed.
            if (context.subPlanTaskRunner().finished()) {
                subPlanListener.onResponse(DriverCompletionInfo.EMPTY);
                return;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("executing subplan [{}]", subPlanIndex);
            }
            var subplan = subplans.get(subPlanIndex);
            String subPlanProfile = profilePrefix == null ? "subplan-" + subPlanIndex : profilePrefix + ".subplan-" + subPlanIndex;
            childSessionId = computeService.newChildSession(context.sessionId());
            // A sub plan may itself contain nested MergeExecs (from nested subqueries); those are broken apart
            // and executed recursively, with a dedicated exchange source feeding this sub plan's segment.
            Tuple<List<PhysicalPlan>, PhysicalPlan> nested = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(subplan);
            if (nested.v1().isEmpty() == false) {
                // A merge sub plan bypasses the runner and starts immediately, so its sink is opened here.
                executeSubPlanWithNestedSubPlans(
                    subPlanIndex,
                    subPlanProfile,
                    nested.v2(),
                    nested.v1(),
                    childSessionId,
                    openSink(childSessionId),
                    subPlanListener
                );
                return;
            }
            // A leaf sub plan may sit in the runner's queue first, so it opens its own sink when it is dispatched - see
            // SubPlan.execute for why that matters.
            context.subPlanTaskRunner().submit(new SubPlan(this, subPlanIndex, subPlanProfile, subplan, childSessionId, subPlanListener));
        } catch (Exception e) {
            try {
                // Recorded first so the re-entry from subPlanCompleted, below the listener resolution in the finally, sees a
                // non-null failure() and stops rather than starting the next branch of a query that is already over. Keeping it
                // ahead of finishSinkHandler means that stays true even if closing the sink is what threw.
                context.subPlanTaskRunner().fail(e);
                if (childSessionId != null) {
                    // A no-op unless a sink was opened above.
                    computeService.exchangeService.finishSinkHandler(childSessionId, e);
                }
            } finally {
                // In a finally because this is the claim being discharged: if one of the two statements above throws and this is
                // skipped, the listener for this index is never touched, subPlanCompleted never runs, and nothing ever claims the
                // remaining indices - so the level's ComputeListener never reaches zero.
                subPlanListener.onFailure(e);
            }
        }
    }

    /**
     * Opens this sub plan's exchange sink and attaches it to the exchange source feeding the plan segment above.
     * <p>
     * Must not be called before the sub plan is about to run. {@link org.elasticsearch.compute.operator.exchange.ExchangeService}
     * reaps any sink that has no producer attached and has not been touched for {@code esql.exchange.sink_inactive_interval}
     * (5 minutes by default), and a sink opened for a sub plan that is still queued looks exactly like that: the source's fetch
     * parks on an empty, unfinished buffer, so nothing refreshes the sink's timestamp. Reaping finishes the buffer, the parked
     * fetch is answered with "finished, no pages", and the sub plan silently contributes no rows - no failure, no partial-result
     * flag. See {@code NestedSubqueriesIT}.
     * <p>
     * Example: with {@code branch_parallel_degree = 2}, L3 waits behind L1 and L2. Opening L3's sink at submit time would leave
     * it idle for as long as those two run. Opening it here, from {@link SubPlan#execute} (or from
     * {@link #executeSubPlanWithNestedSubPlans} for a merge that starts immediately), means the reaper never sees an abandoned
     * handler.
     */
    private ExchangeSinkHandler openSink(String childSessionId) {
        ExchangeSinkHandler exchangeSink = computeService.exchangeService.createSinkHandler(
            childSessionId,
            context.queryPragmas().exchangeBufferSize()
        );
        exchangeSource.addRemoteSink(exchangeSink::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
        return exchangeSink;
    }

    /**
     * Records that one sub plan reached a terminal state, and either starts the next one or, once every sub plan is accounted for,
     * releases the empty sink. Runs exactly once per sub plan, driven by the listener wrappers installed in the constructor - see
     * there for why it is tied to listener resolution rather than called directly.
     * <p>
     * Example: three branches, window of 2. L1 and M2 are in flight. When L1's listener resolves, the count is 1 of 3, so this
     * method starts L5. When M2 and L5 also resolve, the count hits 3 and {@code emptySinkRef} is closed: the exchange source
     * can then finish once the coordinator has drained the sinks that did run.
     */
    private void subPlanCompleted() {
        if (completedSubPlanCount.incrementAndGet() == subplans.size()) {
            // All subplans have completed — release the empty sink so the exchange source
            // can finish once all subplan sinks have been consumed by the coordinator.
            emptySinkRef.close();
        } else {
            tryExecuteNextSubPlan();
        }
    }

    /**
     * Executes a sub plan that itself contains nested {@code MergeExec}s (from nested subqueries). Mirrors the
     * top-level flow in {@link ComputeService#execute}: the segment plan — the part of the sub plan above the
     * nested merge points — runs on the coordinator, consuming a dedicated exchange source fed by the nested
     * sub plans and producing into this sub plan's {@code exchangeSink} like any other sub plan. The nested sub
     * plans are run by a nested {@link MergeLevelExecutor}, recursing further if they contain nested merges
     * themselves. The nested executor is created (and its empty sink registered) before the segment plan starts
     * so the segment's exchange source cannot finish before the nested sub plans attach their sinks.
     * <p>
     * Start order matches {@link ComputeService#execute}: construct the nested executor, {@code runCompute} the segment
     * (the consumer of the nested source), then {@link #execute} the nested branches (the producers). {@code runCompute}
     * returns immediately after scheduling drivers; {@code execute} is synchronous and will submit leaves that can start
     * producing on the search pool. The consumer must already be fetching, or those leaves fill the exchange buffer and
     * stall, and a {@code LIMIT} on the segment cannot complete in time to {@link SubPlanTaskRunner#finish()} leftover work.
     * <p>
     * Any throw during that setup must be reported on the nested {@link ComputeListener}, which is what {@code setupListener}
     * below is for. Closing that listener with no failure recorded resolves {@code completionListener} through its success
     * branch, so a branch that never started would be counted as an empty success instead of failing the query.
     * <p>
     * Example: M2's segment reads nested source {@code S/4} and writes parent sink {@code S/3}. After this method returns,
     * M2's driver is running and a nested executor is walking {@code [L2, M3]}.
     *
     * <h4>No executor hop: this runs on the calling thread</h4>
     *
     * A merge sub plan bypasses {@link SubPlanTaskRunner} entirely - no permit, and no hop onto the search pool either. When
     * {@link #subPlanCompleted} refills the window with a merge branch, everything below happens on whichever thread resolved the
     * previous branch's listener, which is usually a driver or exchange completion thread. {@code SubPlanTaskRunner.dispatch}
     * explains why a <em>leaf</em> must not start there; a merge segment is a different kind of work and is deliberately exempt:
     * <ul>
     * <li>The segment's {@link ComputeContext} is built with {@link EmptyIndexedByShardId}, so {@code runCompute} plans with no
     *     {@code SearchExecutionContext}s at all - no Lucene query rewriting, no weight construction, no {@code SearchStats}
     *     lookups. It is coordinator-only planning over an exchange source, the same shape as the main plan's {@code runCompute}
     *     in {@link ComputeService#execute}, which has always run inline on its caller's thread.</li>
     * <li>{@code runCompute} returns as soon as it has scheduled drivers onto the compute pool; it does not run them.</li>
     * <li>A hop here would need to be permit-free to avoid the deadlock described in the class javadoc, so it would buy an
     *     {@code EsRejectedExecutionException} failure path for a merge branch without bounding anything.</li>
     * </ul>
     * What the calling thread does pay for is the recursion: expanding a merge branch constructs the nested executor and, through
     * {@link #execute}, walks its branches - submitting nested leaves and expanding nested merges in turn. That is bookkeeping and
     * plan mapping, not query execution, but it is unbounded in depth, which is what {@code max_query_branches} limits.
     */
    private void executeSubPlanWithNestedSubPlans(
        int subPlanIndex,
        String subPlanProfile,
        PhysicalPlan segmentPlan,
        List<PhysicalPlan> nestedSubplans,
        String childSessionId,
        ExchangeSinkHandler exchangeSink,
        ActionListener<DriverCompletionInfo> subPlanListener
    ) {
        var nestedSessionId = computeService.newChildSession(context.sessionId());
        ExchangeSourceHandler nestedExchangeSource = new ExchangeSourceHandler(
            context.queryPragmas().exchangeBufferSize(),
            computeService.searchExecutor
        );
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("executing subplan [{}] with [{}] nested subplans", subPlanIndex, nestedSubplans.size());
        }
        ActionListener<DriverCompletionInfo> completionListener = ActionListener.runBefore(ActionListener.wrap(completionInfo -> {
            exchangeSink.addCompletionListener(
                ActionListener.running(() -> { computeService.exchangeService.finishSinkHandler(childSessionId, null); })
            );
            subPlanListener.onResponse(completionInfo);
        }, e -> {
            context.subPlanTaskRunner().fail(e);
            nestedExchangeSource.finishEarly(true, ActionListener.noop());
            computeService.exchangeService.finishSinkHandler(childSessionId, e);
            subPlanListener.onFailure(e);
        }), () -> computeService.exchangeService.removeExchangeSourceHandler(nestedSessionId));
        try (var nestedComputeListener = new ComputeListener(computeService.cancelQueryOnFailure(context.rootTask()), completionListener)) {
            // Holds the nested ComputeListener open for the duration of the setup below. Without it, a throw before the
            // first acquireCompute() closes the listener with no failure recorded and no other references held, so
            // completionListener takes its success branch and reports this branch as an empty success - and the catch in
            // tryExecuteNextSubPlan then has its onFailure swallowed by notifyOnce.
            ActionListener<Void> setupListener = nestedComputeListener.acquireAvoid();
            MergeLevelExecutor nestedExecutor = null;
            ActionListener<DriverCompletionInfo> segmentListener = null;
            try {
                // Registered inside the guarded region, and only now that completionListener - which removes it again - exists.
                // Registering any earlier means a throw before this point (constructing the ComputeListener, say) leaves the
                // handler in the exchange service for the node's lifetime, because nothing would resolve completionListener and
                // run its removal. From here on the setupListener failure path does resolve it.
                computeService.exchangeService.addExchangeSourceHandler(nestedSessionId, nestedExchangeSource);
                // Create the nested executor before running the segment plan: its constructor registers an empty
                // sink on the nested exchange source, keeping the source open until all nested sub plans attach.
                nestedExecutor = new MergeLevelExecutor(
                    computeService,
                    context,
                    nestedSubplans,
                    nestedComputeListener,
                    nestedExchangeSource,
                    subPlanProfile
                );
                var computeContext = new ComputeContext(
                    nestedSessionId,
                    computeService.profileDescription(subPlanProfile, "merge"),
                    ComputeService.LOCAL_CLUSTER,
                    context.flags(),
                    EmptyIndexedByShardId.instance(),
                    context.configuration(),
                    context.foldContext(),
                    nestedExchangeSource::createExchangeSource,
                    () -> exchangeSink.createExchangeSink(() -> {}),
                    false
                );
                // notifyOnce so the catch below can fail this listener if runCompute throws before invoking it (its
                // pre-try setup sits outside its own catch) without double-completing when runCompute also reports.
                segmentListener = ActionListener.notifyOnce(nestedComputeListener.acquireCompute());
                // Unlike the main segment in ComputeService#execute, this completion must not call
                // SubPlanTaskRunner#finish(). That flag is query-wide and drains the whole queue: a nested segment satisfying
                // its own LIMIT says nothing about what the levels above it still need, so finishing here would skip leaves
                // belonging to sibling branches at other levels and silently drop their rows. The cost of the asymmetry is that
                // leaves already queued for this level keep running after its LIMIT is met - wasted work, bounded by
                // nestedExchangeSource finishing early once its last source is done. Reclaiming it would need the runner to
                // scope "finished" per level.
                computeService.runCompute(
                    context.rootTask(),
                    computeContext,
                    segmentPlan,
                    computeService.plannerSettings.get(),
                    LocalPhysicalOptimization.ENABLED,
                    context.configuration().profile() ? new PlanTimeProfile() : null,
                    segmentListener
                );
                nestedExecutor.execute(context.queryPragmas().branchParallelDegree());
                setupListener.onResponse(null);
            } catch (Exception e) {
                // The constructor pre-acquires one ComputeListener reference per nested branch. A throw after that
                // (runCompute's pre-try setup, execute, or the success callback) would leave those unused references
                // outstanding, so completionListener would never fire and the nested exchange source would leak.
                // abortUndispatched fails every index that was never claimed; notifyOnce on segmentListener covers the
                // acquireCompute passed to runCompute if that method threw before invoking it.
                if (nestedExecutor != null) {
                    nestedExecutor.abortUndispatched(e);
                }
                if (segmentListener != null) {
                    segmentListener.onFailure(e);
                }
                setupListener.onFailure(e);
                // Rethrown so the catch in tryExecuteNextSubPlan also runs: it finishes this sub plan's sink and records the
                // failure on the runner, neither of which the nested listener chain does for the parent level. Both paths
                // reaching the branch listener is harmless - notifyOnce on the listener, first-failure-wins in
                // SubPlanTaskRunner.fail, and finishSinkHandler is a no-op once the sink is finished.
                throw e;
            }
        }
    }

    /**
     * A sub plan with no nested {@code MergeExec}s below it, and therefore the unit of work the query-wide
     * {@link SubPlanTaskRunner} admits, skips or rejects. It receives at least one of {@link #execute}, {@link #skip} or
     * {@link #fail}, and normally exactly one - see {@link SubPlanTaskRunner.Task} for the case that is not, which is what
     * {@code completed} guards.
     */
    static final class SubPlan implements SubPlanTaskRunner.Task {
        private final MergeLevelExecutor owner;
        private final int subPlanIndex;
        private final String profile;
        private final PhysicalPlan plan;
        private final String childSessionId;
        private final ActionListener<DriverCompletionInfo> subPlanListener;
        private final AtomicBoolean completed = new AtomicBoolean();
        /**
         * Opened by {@link #execute}, so it stays {@code null} for as long as this sub plan waits in the runner's queue - see
         * {@link MergeLevelExecutor#openSink} for why it must not be opened any earlier. Written and read from different threads:
         * the runner dispatches {@code execute} onto the search executor, while completion arrives on a driver thread.
         */
        private volatile ExchangeSinkHandler exchangeSink;

        /**
         * Records the plan and the listener that must be resolved when this leaf reaches a terminal state. Does not open a
         * sink or start compute: that happens in {@link #execute} after the runner admits this task.
         */
        private SubPlan(
            MergeLevelExecutor owner,
            int subPlanIndex,
            String profile,
            PhysicalPlan plan,
            String childSessionId,
            ActionListener<DriverCompletionInfo> subPlanListener
        ) {
            this.owner = owner;
            this.subPlanIndex = subPlanIndex;
            this.profile = profile;
            this.plan = plan;
            this.childSessionId = childSessionId;
            this.subPlanListener = subPlanListener;
        }

        /**
         * Opens this leaf's sink (it is about to produce) and runs the plan through {@link ComputeService#executePlan}.
         * The {@code completionListener} is the runner's permit: it must be resolved exactly once in {@link #complete}, or
         * every leaf queued behind this one stalls.
         */
        @Override
        public void execute(ActionListener<Void> completionListener) {
            exchangeSink = owner.openSink(childSessionId);
            owner.computeService.executePlan(
                childSessionId,
                owner.context.rootTask(),
                owner.context.flags(),
                plan,
                owner.context.configuration(),
                owner.context.foldContext(),
                owner.context.execInfo(),
                profile,
                ActionListener.wrap(
                    result -> complete(result.completionInfo(), null, completionListener),
                    e -> complete(null, e, completionListener)
                ),
                () -> exchangeSink.createExchangeSink(() -> {}),
                owner.context.initialClusterStatuses(),
                owner.context.configuration().profile() ? new PlanTimeProfile() : null
            );
        }

        /**
         * The query already has enough rows ({@link SubPlanTaskRunner#finish()}). The runner only skips a task it has not
         * dispatched, so no sink was opened. Resolving the branch listener as empty success still counts in
         * {@link #subPlanCompleted}, which is what lets {@code emptySinkRef} be released.
         */
        @Override
        public void skip() {
            if (completed.compareAndSet(false, true) == false) {
                return;
            }
            // Resolved before the assertion below, not after. This runs on the runner's executor, where AbstractRunnable#run
            // catches Exception and not Throwable, so an AssertionError raised here escapes to the thread pool's uncaught
            // handler. With the resolution second, a tripped assertion would leave this branch's listener unresolved and hang
            // the query until it times out - a diagnostic turned into a far worse symptom than the one it reports. Resolving
            // first means the assertion still fires and still fails the test, loudly and on the spot.
            subPlanListener.onResponse(DriverCompletionInfo.EMPTY);
            // The runner only skips a task it has not dispatched, so no sink was ever opened and there is nothing to close - this
            // sub plan simply never attached to the exchange source.
            assert exchangeSink == null : "skipped a sub plan that had already opened its sink";
        }

        /**
         * The query has failed. {@link org.elasticsearch.compute.operator.exchange.ExchangeService#finishSinkHandler} is a no-op
         * if {@link #execute} never opened a sink (queued leaf). Always resolve {@code subPlanListener} so this branch is
         * counted; {@code completed} ignores a second call if {@link #complete} already ran.
         */
        @Override
        public void fail(Exception failure) {
            if (completed.compareAndSet(false, true) == false) {
                return;
            }
            try {
                owner.computeService.exchangeService.finishSinkHandler(childSessionId, failure);
            } finally {
                subPlanListener.onFailure(failure);
            }
        }

        /**
         * Terminal path for a leaf that was actually dispatched. Finishes the sink (on success, after it has drained; on
         * failure, immediately), resolves {@code subPlanListener} — which starts the next branch via {@link #subPlanCompleted}
         * — and only then releases the runner permit. Releasing the permit second means a queued sibling is admitted by
         * {@link SubPlanTaskRunner#takeReadyLocked} rather than started from {@link #tryExecuteNextSubPlan} on this thread
         * while this leaf still holds the slot.
         */
        private void complete(DriverCompletionInfo completionInfo, Exception failure, ActionListener<Void> concurrencyListener) {
            if (completed.compareAndSet(false, true) == false) {
                return;
            }
            try {
                if (failure == null) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("subplan [{}] finished successfully", subPlanIndex);
                    }
                    exchangeSink.addCompletionListener(
                        ActionListener.running(() -> owner.computeService.exchangeService.finishSinkHandler(childSessionId, null))
                    );
                    subPlanListener.onResponse(completionInfo);
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("subplan [{}] finished with an error [{}]", subPlanIndex, failure.getMessage());
                    }
                    owner.computeService.exchangeService.finishSinkHandler(childSessionId, failure);
                    subPlanListener.onFailure(failure);
                }
            } finally {
                // Releases this leaf's permit. Runs after subPlanListener resolution above, which is what starts the next
                // sub plan, so a queued leaf is admitted by takeReadyLocked here rather than dispatched directly.
                if (failure == null) {
                    concurrencyListener.onResponse(null);
                } else {
                    concurrencyListener.onFailure(failure);
                }
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.SubPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plugin.ComputeService.LOCAL_CLUSTER;

/**
 * Executes an immutable nested-subplan topology. Merge segments own their local exchanges and run outside the branch-parallel limit;
 * up to {@code branchParallelDegree} leaf producer plans run concurrently using a self-refilling dispatch loop.
 */
final class SubPlansExecutor {
    private static final Logger LOGGER = LogManager.getLogger(SubPlansExecutor.class);

    private final ComputeService computeService;
    private final ExchangeService exchangeService;
    private final Executor searchExecutor;
    private final String sessionId;
    // Unique per executor instance. ComputeService.execute runs multiple times with the same sessionId within one query (once per subquery
    // round plus the main plan), while sink deregistration on the success path is deferred until the handler drains. Deriving child
    // exchange ids from a fresh child session prevents a later round from colliding with a not-yet-deregistered handler of an earlier
    // round.
    private final String sessionPrefix;
    private final CancellableTask rootTask;
    private final EsqlFlags flags;
    private final Configuration configuration;
    private final FoldContext foldContext;
    private final EsqlExecutionInfo execInfo;
    private final Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses;
    private final QueryPragmas queryPragmas;
    /**
     * Rollback ledger for phase 1: every {@link MergeContext} whose {@link ExchangeSourceHandler} has been registered with the
     * {@link ExchangeService}, in registration order.
     * <p>
     * Phase 1 registers exchanges as a side effect while the tree is still half-built, so when {@link #buildSubPlanContext} throws
     * partway through there is no tree to walk to find what was registered - this list is how {@link #cleanupUnstarted} finds it.
     * Emptied as soon as phase 1 succeeds: from then on the tree owns these contexts and the abort and terminal paths release them,
     * so a second reference would only pin them for the life of the query.
     */
    private final List<MergeContext> unstartedMergeContexts = new ArrayList<>();
    /**
     * Rollback ledger for phase 1, the {@link ParentSink} counterpart of {@link #unstartedMergeContexts}: one entry per child of
     * every merge node, in creation order. Each entry holds whatever that child has already acquired - a registered
     * {@link ExchangeSinkHandler} for a nested merge, or a keep-alive ref on the parent's exchange source for a leaf. The root merge
     * is absent, since it writes into {@code collectedPages} rather than into a sink. Same lifecycle as
     * {@link #unstartedMergeContexts}: read only by {@link #cleanupUnstarted}, emptied once phase 1 succeeds.
     */
    private final List<ParentSink> unstartedParentSinks = new ArrayList<>();
    // Flat list of leaves populated during startMerge; dispatched after all merges are wired.
    private final List<ScheduledLeaf> scheduledLeaves = new ArrayList<>();
    private final AtomicInteger nextLeafIndex = new AtomicInteger();

    SubPlansExecutor(
        ComputeService computeService,
        ExchangeService exchangeService,
        Executor searchExecutor,
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses
    ) {
        this.computeService = computeService;
        this.exchangeService = exchangeService;
        this.searchExecutor = searchExecutor;
        this.sessionId = sessionId;
        this.sessionPrefix = computeService.newChildSession(sessionId);
        this.rootTask = rootTask;
        this.flags = flags;
        this.configuration = configuration;
        this.foldContext = foldContext;
        this.execInfo = execInfo;
        this.initialClusterStatuses = initialClusterStatuses;
        this.queryPragmas = configuration.pragmas();
    }

    /**
     * Executes a nested {@link SubPlan.Merge} topology in three sequential phases.
     * <p>
     * <b>Phase 1 – register exchanges ({@code buildSubPlanContext}):</b> recursively walks the {@link SubPlan} tree and registers an
     * {@link ExchangeSourceHandler} for every {@link SubPlan.Merge} node and an {@link ExchangeSinkHandler} for every nested-merge
     * child. Leaf children only reserve a keep-alive ref on their parent source; their sink handlers are created lazily at dispatch
     * time (see {@link ParentSink}), so leaves queued behind {@code branchParallelDegree} are invisible to the exchange service's
     * inactive-sink reaper. This phase is synchronous and has no async side effects, so any exception partway through is caught and
     * rolled back by {@link #cleanupUnstarted} before propagating to {@code listener}.
     * <p>
     * <b>Phase 2 – wire merge segments ({@code startMerge}):</b> top-down recursive walk that calls
     * {@code ComputeService.runCompute} for each merge node and accumulates leaves into {@link #scheduledLeaves}. All merge segments
     * are wired before any leaf is dispatched, so no leaf can complete and attempt to read from an exchange source that has not yet
     * been set up.
     * <p>
     * <b>Phase 3 – dispatch leaves:</b> launches {@code min(branchParallelDegree, leafCount)} initial workers. Each worker calls
     * {@link #tryExecuteNextLeaf}, which atomically claims the next leaf from {@link #scheduledLeaves} via {@link #nextLeafIndex}
     * and re-invokes itself on completion, so the concurrency level stays at most {@code branchParallelDegree} throughout.
     * <p>
     * Example — {@code FROM a, (FROM b, (FROM c, (FROM d)))}, three nested merges, with {@code branchParallelDegree=2}. Each level
     * has two branches, one plain index and one subquery, so none of them is collapsed by the optimizer:
     * <pre>
     * Merge(root)                   path=null
     * ├─ Leaf(a)                    path="subplan-0"
     * └─ Merge(inner1)              path="subplan-1"
     *    ├─ Leaf(b)                 path="subplan-1.subplan-0"
     *    └─ Merge(inner2)           path="subplan-1.subplan-1"
     *       ├─ Leaf(c)              path="subplan-1.subplan-1.subplan-0"
     *       └─ Leaf(d)              path="subplan-1.subplan-1.subplan-1"
     *
     * Phase 1: registers 3 exchange sources, one per merge; registers 2 eager sinks, for inner1 and inner2; reserves 4 keep-alive refs,
     *          one per leaf. No leaf sink handler exists yet.
     * Phase 2: runCompute(root), then depth first into runCompute(inner1) and runCompute(inner2), so all three segments are consuming
     *          before any producer starts. Leaves flatten into scheduledLeaves in the order visited, [a, b, c, d], which is why phase 3
     *          no longer needs the tree shape.
     * Phase 3: two initial workers claim a and b. Each completion refills its own slot, so c is claimed when the first of those finishes
     *          and d when the next does; the two claims after that find the index past the end of scheduledLeaves and exit.
     * </pre>
     * Note how a leaf can be a sibling of a merge at any level: {@code a} feeds the root's source directly while {@code inner1}
     * feeds it through a sink of its own, and both are just children to iterate over.
     */
    void execute(SubPlan.Merge executionPlan, PlanTimeProfile planTimeProfile, ActionListener<Result> listener) {
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        // Phase 1: register all exchanges. If buildSubPlanContext throws partway, some ExchangeSourceHandlers and ExchangeSinkHandlers
        // may already be registered; cleanupUnstarted rolls them back before failing.
        final MergeContext root;
        try {
            root = buildSubPlanContext(executionPlan, null, null, collectedPages);
        } catch (Exception e) {
            try {
                cleanupUnstarted(e);
            } catch (Exception cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            } finally {
                listener.onFailure(e);
            }
            return;
        }

        // Phase 1 succeeded. cleanupUnstarted is no longer reachable; release the references it held.
        int mergeCount = unstartedMergeContexts.size();
        unstartedMergeContexts.clear();
        unstartedParentSinks.clear();

        // On failure, release any pages already collected to avoid memory leaks.
        ActionListener<DriverCompletionInfo> completionListener = ActionListener.wrap(profiles -> {
            execInfo.markEndQuery();
            listener.onResponse(new Result(root.plan.output(), collectedPages, null, configuration, profiles, execInfo, null));
        }, e -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            listener.onFailure(e);
        });

        // One RunOnce shared across the entire query: cancelTaskAndDescendants must fire at most once regardless of how many merge nodes
        // fail. Creating one per merge node (depth D) would fire any non-idempotent side-effect D times on a cascading failure.
        final Runnable cancelOnFailure = computeService.cancelQueryOnFailure(rootTask);

        // Phase 2: wire all merge segments and collect leaves into scheduledLeaves.
        startMerge(root, planTimeProfile, completionListener, cancelOnFailure);

        LOGGER.debug(
            "topology built: [{}] merge nodes, [{}] leaves, branchParallelDegree=[{}]",
            mergeCount,
            scheduledLeaves.size(),
            queryPragmas.branchParallelDegree()
        );

        // Phase 3: dispatch the initial wave; each worker self-refills its slot on completion.
        int initial = Math.min(queryPragmas.branchParallelDegree(), scheduledLeaves.size());
        try {
            for (int i = 0; i < initial; i++) {
                tryExecuteNextLeaf();
            }
        } catch (Exception e) {
            // Draining the undispatched leaves settles their refs and lets the query fail. Leaves already dispatched are unaffected, they
            // self-refill and find the queue drained.
            failRemainingLeaves(e);
        }
    }

    /**
     * Recursively converts a {@link SubPlan.Merge} tree into a {@link MergeContext} tree, registering the exchange handlers those
     * nodes need as it goes. Both results matter: the tree is what phases 2 and 3 walk, and the registrations are what
     * {@link #cleanupUnstarted} has to undo if this method throws partway through.
     * <p>
     * From the consumer side, every {@link SubPlan.Merge} becomes a {@link MergeContext} owning the {@link ExchangeSourceHandler} it
     * reads, and from the producer side every child of it gets a {@link ParentSink} to write into. The two kinds of child differ in when
     * that sink is created:
     * <ul>
     *   <li>A nested {@link SubPlan.Merge} child gets an <b>eager</b> sink: its {@link ExchangeSinkHandler} is registered here and
     *       wired into the parent's source right away. The child then recurses through this method.</li>
     *   <li>A {@link SubPlan.Leaf} child becomes a {@link LeafContext} with a <b>lazy</b> sink: nothing is registered yet, only a
     *       keep-alive ref on the parent's source, and the handler is created when the leaf is dispatched in phase 3. See
     *       {@link ParentSink} for why leaves have to wait.</li>
     * </ul>
     * The {@code emptySink} keeps the source alive while children are being wired, preventing premature completion.
     * <p>
     * Example — input {@link SubPlan} tree and the resulting {@link MergeContext} tree (sessionId = "s", sessionPrefix = "s/1",
     * path = null for root):
     * <pre>
     * Input SubPlan:
     *   Merge(plan=LimitExec→ExchangeSourceExec)
     *   ├─ Leaf(ExchangeSinkExec→LeafA)
     *   └─ Merge(plan=ExchangeSinkExec→ExchangeSourceExec)
     *      ├─ Leaf(ExchangeSinkExec→LeafB)
     *      └─ Leaf(ExchangeSinkExec→LeafC)
     *
     * Output MergeContext tree:
     *   MergeContext(path=null, exchangeId="s", computeSessionId="s/1",
     *                plan=OutputExec→LimitExec→ExchangeSourceExec, exchangeSource=src0, parentSink=null)
     *   ├─ LeafContext(path="subplan-0", plan=ExchangeSinkExec→LeafA,
     *                  parentSink=ParentSink(id="s/1/subplan-0", lazy→src0))
     *   └─ MergeContext(path="subplan-1", exchangeId="s/1/subplan-1/merge", computeSessionId="s/1/subplan-1/merge",
     *                   plan=ExchangeSinkExec→ExchangeSourceExec,
     *                   exchangeSource=src1, parentSink=ParentSink(id="s/1/subplan-1", sink1→src0))
     *      ├─ LeafContext(path="subplan-1.subplan-0", plan=ExchangeSinkExec→LeafB,
     *                     parentSink=ParentSink(id="s/1/subplan-1.subplan-0", lazy→src1))
     *      └─ LeafContext(path="subplan-1.subplan-1", plan=ExchangeSinkExec→LeafC,
     *                     parentSink=ParentSink(id="s/1/subplan-1.subplan-1", lazy→src1))
     *
     * Registered in ExchangeService, in this order:
     *   src0  registered under "s"                (root merge source; bare sessionId so finishSessionEarly finds it)
     *   sink1 registered under "s/1/subplan-1"    (inner merge's sink → src0)
     *   src1  registered under "s/1/subplan-1/merge" (inner merge source)
     * Leaf sinks ("s/1/subplan-0", "s/1/subplan-1.subplan-0", "s/1/subplan-1.subplan-1") are registered lazily when each leaf is
     * dispatched in phase 3.
     * </pre>
     * <p>
     * The root node wraps its plan in an {@code OutputExec} to collect final pages into {@code collectedPages}. All other merge nodes
     * use their plan as-is (already contains an {@code ExchangeSinkExec} at the top that feeds the parent source).
     */
    private MergeContext buildSubPlanContext(SubPlan.Merge executionPlan, String path, ParentSink parentSink, List<Page> collectedPages) {
        boolean root = path == null;
        // The root source must stay registered under the bare sessionId: ExchangeService.finishSessionEarly (async stop)
        // looks it up by that key. All other ids derive from the per-executor sessionPrefix to stay unique across rounds.
        String exchangeId = root ? sessionId : nodeSessionId(path) + "/merge";
        String computeSessionId = root ? sessionPrefix : exchangeId;
        ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(queryPragmas.exchangeBufferSize(), searchExecutor);
        exchangeService.addExchangeSourceHandler(exchangeId, exchangeSource);
        // Root segment collects final pages via OutputExec; nested segments use their plan as-is.
        PhysicalPlan segmentPlan = root ? new OutputExec(executionPlan.plan(), collectedPages::add) : executionPlan.plan();
        var context = new MergeContext(segmentPlan, path, exchangeId, computeSessionId, exchangeSource, parentSink, new ArrayList<>());
        unstartedMergeContexts.add(context);

        // emptySink keeps the source alive while children are being wired.
        try (var emptySink = exchangeSource.addEmptySink()) {
            for (int i = 0; i < executionPlan.children().size(); i++) {
                buildChildContext(executionPlan.children().get(i), childPath(path, i), exchangeSource, context, collectedPages);
            }
        }
        return context;
    }

    /**
     * Creates the {@link SubPlanContext} for one child of a merge node and appends it to the parent's {@code children} list.
     * A nested-merge child gets an eager {@link ParentSink} wired into the parent's {@link ExchangeSourceHandler}; a leaf child
     * gets a lazy {@link ParentSink} that registers nothing until the leaf is dispatched (see {@link ParentSink}).
     * <p>
     * Each {@link ParentSink} is recorded in {@link #unstartedParentSinks} immediately after it is created, so an exception anywhere
     * later in phase 1 is rolled back by {@link #cleanupUnstarted}. Only an {@link Error} between creating a sink and recording it
     * can escape that, and phase 1's caller does not recover from those either.
     */
    private void buildChildContext(
        SubPlan child,
        String childPath,
        ExchangeSourceHandler parentSource,
        MergeContext parent,
        List<Page> collectedPages
    ) {
        String childSessionId = nodeSessionId(childPath);
        if (child instanceof SubPlan.Merge merge) {
            var sinkHandler = exchangeService.createSinkHandler(childSessionId, queryPragmas.exchangeBufferSize());
            var childSink = new ParentSink(childSessionId, sinkHandler);
            unstartedParentSinks.add(childSink);
            parentSource.addRemoteSink(sinkHandler::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
            parent.children.add(buildSubPlanContext(merge, childPath, childSink, collectedPages));
        } else {
            var childSink = new ParentSink(childSessionId, parentSource);
            unstartedParentSinks.add(childSink);
            parent.children.add(new LeafContext(child.plan(), childPath, childSink));
        }
    }

    /**
     * Rolls back the exchange registrations that a failed {@link #buildSubPlanContext} left behind, using the two ledgers that
     * recorded them. Called only from {@link #execute}'s phase 1 catch block, so no merge segment has started and no driver exists
     * yet: the only live work is the fetchers {@link ExchangeSourceHandler#addRemoteSink} forked for the sinks already wired up.
     * That is what keeps this method a straight-line walk rather than a coordinated shutdown.
     * <p>
     * Each source is torn down with the same pair as {@link ExchangeService#finishSessionEarly} - deregister, then
     * {@link ExchangeSourceHandler#finishEarly} - and {@code drainingPages} is {@code true} here because a query that failed to
     * build has no results to hand back, so buffered pages should be discarded rather than kept for a reader that will never come.
     * Async stop passes {@code false} for the opposite reason. This is the same teardown that {@link #mergeTerminalListener} and
     * {@link #abortUnstartedMergeContext} perform for a single node once phase 1 has succeeded.
     * <p>
     * The two loops are not redundant, because {@code finishEarly} reaches only the read side. It closes each remote sink the
     * source has registered, and {@code RemoteSink.close} is implemented as a fetch with {@code allSourcesFinished}, so it does
     * finish an eagerly wired nested-merge handler's buffer - but it never touches {@link ExchangeService}'s registry, and it
     * cannot see a leaf whose {@link ParentSink#attach} never ran, since that leaf was never registered as a remote sink at all.
     * Deregistering handlers and releasing the leaves' keep-alive refs is what the second loop is for.
     * <p>
     * Two things about the order. Each ledger is walked with {@code reversed()}, so the innermost node goes first and a nested node
     * is undone before the parent it feeds, mirroring the order they were built in. And every source is closed before any sink is
     * failed: failing a sink completes the fetch listener its parent's source has parked, and because these sinks were registered
     * with {@code failFast}, that would drive the parent's sink-failed path and abort it with a {@code TaskCancelledException} -
     * inventing a second failure inside exchanges that are being discarded anyway, on top of the real one being reported. Going in
     * this order instead leaves every eager handler already finished by the time the second loop fails it, so that failure lands on
     * a settled handler rather than racing a live fetch.
     *
     * @param failure the exception that caused the build to fail, propagated to each parent sink
     */
    private void cleanupUnstarted(Exception failure) {
        for (MergeContext mergeContext : unstartedMergeContexts.reversed()) {
            removeExchangeSource(mergeContext);
            mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
        }
        for (ParentSink parentSink : unstartedParentSinks.reversed()) {
            finishParentSink(parentSink, failure);
        }
    }

    /**
     * Starts execution of a merge segment and recurses synchronously into child merges before returning. Leaves are not dispatched here;
     * they are collected into {@link #scheduledLeaves} and dispatched by the caller after all merges are wired.
     * <p>
     * For each merge node this method:
     * <ol>
     *   <li>Calls {@code ComputeService.runCompute} to start the local coordinator segment (reads from {@code exchangeSource}, writes
     *       into {@code parentSink}).</li>
     *   <li>Recurses into child {@link MergeContext} nodes via {@link #startChildContext}.</li>
     *   <li>Collects child {@link LeafContext} nodes into {@link #scheduledLeaves} via {@link #startChildContext}.</li>
     * </ol>
     * <p>
     * <b>Guard-ref idiom.</b> Despite the name, {@link ComputeListener} is not a callback: it is a {@link Releasable} reference
     * counter whose delegate is {@code terminalListener}, and which also collects this segment's failures and driver profiles. One is
     * opened per merge node, and it invokes {@code terminalListener} once its count reaches zero. The count is made up of:
     * <ul>
     *   <li>one implicit ref taken by the constructor, released when the try-with-resources closes;</li>
     *   <li>{@code guard}, from {@link ComputeListener#acquireAvoid()}, held across this whole method body;</li>
     *   <li>{@code segmentListener}, for this node's own {@code runCompute};</li>
     *   <li>one {@code childListener} per child, handed out by {@link #startChildContext}.</li>
     * </ul>
     * The implicit ref is what makes the rest safe: while the body is still wiring the node, the count cannot reach zero even if
     * every explicit ref has already completed, so {@code terminalListener} cannot fire early. {@code guard} overlaps with it on
     * that much, but is not redundant, because the two answer different questions: the implicit ref decides <em>when</em>
     * {@code terminalListener} may fire, while {@code guard} decides <em>with what outcome</em>. Only an
     * {@link ActionListener} can record a failure and trigger {@code cancelQueryOnFailure}; {@link ComputeListener#close()} can
     * merely drop a ref. Without {@code guard}, a throw from the very first {@link ComputeListener#acquireCompute()} would leave no
     * ref able to carry it, and the count would reach zero with nothing recorded - reporting success for a segment that failed.
     * When everything succeeds,
     * {@code guard.onResponse(null)} releases the guard and the count then drops as each async operation completes. When something
     * fails synchronously, the inner catch completes all acquired refs before releasing the guard in a {@code finally}, so
     * {@code terminalListener} still fires exactly once, carrying the recorded failure.
     * <p>
     * These per-node counters chain into a single bottom-up signal, which is why no node needs a reference to its parent: a child
     * merge's {@code terminalListener} <em>is</em> one of the parent's {@code childListener} refs, so a nested node reaching zero is
     * what releases the parent's ref on its behalf, and the root's {@code terminalListener} is what completes the user's request. A
     * leaf does the same through {@link ScheduledLeaf}, which pairs it with the {@code childListener} it must complete.
     * <p>
     * {@code segmentListener} and {@code childListeners} are acquired inside the inner try block so that a mid-loop
     * {@link ComputeListener#acquireCompute()} failure is caught by the inner catch, which has the already-acquired refs in scope
     * and can release them before releasing the guard. Only {@code guard} is acquired outside the inner try; if
     * {@link ComputeListener#acquireAvoid()} itself fails, the outer backstop fires (at that point no inner refs exist).
     * <p>
     * Note the outer backstop does <em>not</em> complete {@code childListeners} - they are scoped to the resource block and so are not
     * even visible there - and must not be changed to. Enumerating how that backstop is reachable shows why it does not need to:
     * <ul>
     *   <li>{@code new ComputeListener(...)} or {@code acquireAvoid()} threw - nothing was acquired, so there is nothing to settle;</li>
     *   <li>the inner {@code finally} threw - the inner catch had already settled every acquired ref before it ran;</li>
     *   <li>{@code close()} threw - if children were still outstanding the count had not reached zero, so {@code close()} was a bare
     *       decrement with nothing to throw from; if it did throw it was from the terminal chain it triggered, which means the count
     *       <em>had</em> reached zero and every child had already completed its own ref.</li>
     * </ul>
     * So in every reachable case the refs are either unacquired or already settled. Force-failing them here would instead settle refs
     * for children that may still be running, firing {@code terminalListener} out from under them - exactly the teardown-under-live-work
     * failure described below for the inner catch.
     * <p>
     * Try-with-resources {@code close()} runs before the {@code catch} clause attached to the same statement (Java Language
     * Specification §14.20.3.2), so by the time the outer backstop runs, {@link ComputeListener#close()} has already dropped the
     * listener's own initial ref. The inner catch is the opposite case: it sits inside the resource block, so that initial ref is
     * still held while it runs, which is why completing every acquired ref there cannot fire {@code terminalListener} early. The
     * {@code guard} is the only ref this method provably owns throughout the body.
     * <p>
     * Hence why the inner catch settles refs rather than simply calling {@code terminalListener.onFailure(e)} itself, which is what
     * the outer backstop does. By the time the inner catch runs, {@code runCompute} may already have started this segment's drivers,
     * and a nested subtree may already be running its own, so firing {@code terminalListener} there would deregister the exchange
     * source and signal the parent sink from under work still using them - the failure mode that a torn-down running segment
     * produces. Draining the count instead defers that teardown until nothing is left to use them. Two smaller reasons point the
     * same way: {@code cancelQueryOnFailure} hangs off the refs rather than off {@code terminalListener}, which is why the outer
     * backstop has to invoke it by hand; and {@code FailureCollector} prefers a client error over a cancellation when it decides
     * which exception to report, so reporting this catch's exception directly could surface a cancelled child instead of the cause.
     * The outer backstop is exempt from all of it: per the enumeration above, whenever it runs there is no live ref of this segment's
     * left to tear anything down from under.
     * <p>
     * Example — two-child merge (one leaf, one nested merge) with ref counts:
     * <pre>
     * MergeContext(root)
     * ├─ LeafContext(LeafA)         → childListeners[0]
     * └─ MergeContext(inner)        → childListeners[1]
     *    └─ LeafContext(LeafB)      → inner.childListeners[0]
     *
     * startMerge(root):
     *   ComputeListener refs: 1 (initial) + guard + segmentListener + childListeners[0] + childListeners[1] = 5
     *   runCompute(root.plan) → segmentListener completes async
     *   startChildContext(LeafA,  childListeners[0]) → scheduledLeaves.add(ScheduledLeaf(LeafA, childListeners[0]))
     *   startChildContext(inner,  childListeners[1]) → startMerge(inner, childListeners[1])  [recursive]
     *     ComputeListener refs (inner): 1 + guard + segmentListener + childListeners[0] = 4
     *     runCompute(inner.plan) → segmentListener completes async
     *     startChildContext(LeafB, childListeners[0]) → scheduledLeaves.add(ScheduledLeaf(LeafB, childListeners[0]))
     *     guard.onResponse(null) → inner refs: 3
     *     close() drops the initial ref → inner refs: 2 (its segmentListener, LeafB)
     *   guard.onResponse(null) → root refs: 4
     *   close() drops the initial ref → root refs: 3 (its segmentListener, LeafA, inner)
     *
     * scheduledLeaves = [LeafA, LeafB]  (ready for phase-3 dispatch)
     * </pre>
     */
    private void startMerge(
        MergeContext mergeContext,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> completionListener,
        Runnable cancelOnFailure
    ) {
        LOGGER.debug(
            "starting merge segment [{}] with [{}] children",
            mergeContext.path == null ? "main" : mergeContext.path,
            mergeContext.children.size()
        );
        // The merge tree is started synchronously, before any leaf is dispatched. The CAS does double duty: it records that this merge
        // is running - abortUnstartedMergeContext relies on `started` to skip merges that are already running - and it rejects a second
        // start. Failing hard rather than asserting, because with assertions disabled a second start would run runCompute against a
        // context abortUnstartedMergeContext has already torn down (exchange source deregistered, parentSink finished, children's sinks
        // failed), giving a silently wrong or hanging query instead of a loud failure. This throws before terminalListener exists, so
        // settling it is the caller's job: for a child merge that is the parent's inner catch below, and for the root it is
        // ComputeService.execute's notifyOnce-wrapped listener.
        if (mergeContext.started.compareAndSet(false, true) == false) {
            throw new IllegalStateException("merge [" + mergeContext.path + "] started twice");
        }
        final ActionListener<DriverCompletionInfo> terminalListener = mergeTerminalListener(mergeContext, completionListener);
        try (var computeListener = new ComputeListener(cancelOnFailure, terminalListener)) {
            final ActionListener<Void> guard = ActionListener.notifyOnce(computeListener.acquireAvoid());
            // segmentListener and childListeners are acquired inside the inner try so that a mid-loop acquireCompute() failure
            // is caught by the inner catch, which has all already-acquired refs in scope and can release them. Acquiring them all
            // up front also keeps the ledger fixed: at any throw the list is either still being built, with no child started, or
            // complete - never half built with some children already running.
            ActionListener<DriverCompletionInfo> segmentListener = null;
            final List<ActionListener<DriverCompletionInfo>> childListeners = new ArrayList<>(mergeContext.children.size());
            try {
                segmentListener = ActionListener.notifyOnce(computeListener.acquireCompute());
                for (int i = 0; i < mergeContext.children.size(); i++) {
                    childListeners.add(ActionListener.notifyOnce(computeListener.acquireCompute()));
                }
                // The root segment is this query's coordinator segment, so it reports the caller's PlanTimeProfile: that
                // instance holds the query-level logical/physical optimization time, and attaching it to a segment's
                // PlanProfile is the only way that time reaches the PROFILE output. This mirrors the single-plan
                // (SubPlan.Leaf) path in ComputeService.execute, which forwards the same instance.
                // Each nested merge segment gets its own PlanTimeProfile so that the PlanProfile attached to its
                // DriverCompletionInfo captures only that segment's local-optimization timing. A single shared
                // instance would accumulate every segment's timings, making all PlanProfiles show identical,
                // over-counted numbers in the PROFILE output.
                final PlanTimeProfile segmentProfile;
                if (mergeContext.path == null) {
                    segmentProfile = planTimeProfile;
                } else {
                    segmentProfile = planTimeProfile != null ? new PlanTimeProfile() : null;
                }
                // The plan ends in an ExchangeSourceExec that polls exchangeSource; output goes into parentSink (null for root).
                computeService.runCompute(
                    rootTask,
                    new ComputeContext(
                        mergeContext.computeSessionId,
                        mergeContext.path == null ? "main.final" : computeService.profileDescription(mergeContext.path, "merge"),
                        LOCAL_CLUSTER,
                        flags,
                        EmptyIndexedByShardId.instance(),
                        configuration,
                        foldContext,
                        mergeContext.exchangeSource::createExchangeSource,
                        mergeContext.parentSink == null ? null : () -> mergeContext.parentSink.handler.createExchangeSink(() -> {}),
                        false
                    ),
                    mergeContext.plan,
                    computeService.plannerSettings().get(),
                    LocalPhysicalOptimization.ENABLED,
                    segmentProfile,
                    segmentListener
                );

                // Starting the children is a second loop rather than being fused with the acquisition above, because runCompute has
                // to run between the two: the children are this node's producers, and they must not start before the consumer
                // drivers exist, or their pages would fill the exchange buffer and block them on waitForWriting until this segment
                // caught up.
                for (int i = 0; i < mergeContext.children.size(); i++) {
                    startChildContext(mergeContext.children.get(i), childListeners.get(i), planTimeProfile, cancelOnFailure);
                }
                guard.onResponse(null);
            } catch (Exception e) {
                LOGGER.debug("synchronous failure starting merge segment [{}]", mergeContext.path == null ? "main" : mergeContext.path, e);
                try {
                    // 1. Complete this segment's own ref. notifyOnce makes it a no-op if runCompute already absorbed it, and it may be
                    // null if acquireCompute() itself failed. Guard remains held, preventing premature termination.
                    if (segmentListener != null) {
                        segmentListener.onFailure(e);
                    }
                    // 2. Settle the child refs and abort the subtree, in that order and as one step - see the method for why the order
                    // matters and why it must not be split back into two statements.
                    settleThenAbortChildren(mergeContext, childListeners, e);
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                } finally {
                    // 3. Releasing the guard records the failure in the FailureCollector and runs cancelQueryOnFailure. It does not
                    // fire terminalListener yet: the ComputeListener's own initial ref is still held here, and only drops when the
                    // try-with-resources closes just below. That is what runs removeExchangeSource, finishEarly, finishParentSink
                    // and the user-visible completion.
                    guard.onFailure(e);
                }
            }
        } catch (Exception e) {
            // Backstop: usually ComputeListener construction or acquireAvoid() (the guard) failing before any inner ref exists, but
            // it also catches a throw from the inner finally or from close(). mergeContext.started is already set, so the parent's
            // abort would no-op — clean up this subtree explicitly.
            LOGGER.debug(
                "failure initialising ComputeListener for merge segment [{}]",
                mergeContext.path == null ? "main" : mergeContext.path,
                e
            );
            try {
                cancelOnFailure.run();
                abortChildrenWithSettledListeners(mergeContext, e);
            } catch (Exception cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            } finally {
                // Settling terminalListener is the one thing this path must not skip, so it goes in a finally - mirroring the inner
                // catch, which releases the guard the same way. Both cleanup calls above can throw: cancelQueryOnFailure reaches
                // TaskManager.cancelTaskAndDescendants, and the abort reaches exchangeService.finishSinkHandler, which
                // asserts the handler is finished. For a nested merge this terminalListener *is* the parent's childListener, so losing
                // it would leave the parent's ComputeListener short a ref forever - the user's request would never complete.
                // terminalListener is notifyOnce'd; it performs removeExchangeSource + finishEarly + finishParentSink.
                terminalListener.onFailure(e);
            }
        }
    }

    /**
     * Builds the {@link ComputeListener} terminal listener for a merge segment. When the listener fires (either success or failure),
     * it deregisters the merge's exchange source and signals the merge's parent sink, then forwards to {@code completionListener}.
     * <p>
     * On success: {@link #removeExchangeSource} deregisters the source; {@link #finishParentSink} signals the parent sink that all
     * data has been written (the parent's {@link ExchangeSourceHandler} will see EOF after the sink drains).
     * <p>
     * On failure: the exchange source is also drained via {@link ExchangeSourceHandler#finishEarly} before the sink is signalled,
     * so any reader blocked on this source is unblocked and receives the failure.
     * <p>
     * Wrapped in {@link ActionListener#notifyOnce} so that concurrent completion paths (e.g. a race between the merge segment
     * completing normally and a child failing) fire the downstream listener at most once.
     *
     * @param mergeContext    the merge node whose resources should be released on completion
     * @param completionListener the listener to forward the final {@link DriverCompletionInfo} (or failure) to
     * @return a notifyOnce-wrapped terminal listener
     */
    private ActionListener<DriverCompletionInfo> mergeTerminalListener(
        MergeContext mergeContext,
        ActionListener<DriverCompletionInfo> completionListener
    ) {
        return ActionListener.notifyOnce(ActionListener.wrap(completionInfo -> {
            removeExchangeSource(mergeContext);
            finishParentSink(mergeContext.parentSink, null);
            completionListener.onResponse(completionInfo);
        }, e -> {
            removeExchangeSource(mergeContext);
            mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
            finishParentSink(mergeContext.parentSink, e);
            completionListener.onFailure(e);
        }));
    }

    /** Recurses into a child {@link MergeContext} via {@link #startMerge}, or adds a child {@link LeafContext} to
     * {@link #scheduledLeaves}. */
    private void startChildContext(
        SubPlanContext child,
        ActionListener<DriverCompletionInfo> childListener,
        PlanTimeProfile planTimeProfile,
        Runnable cancelOnFailure
    ) {
        switch (child) {
            case MergeContext merge -> startMerge(merge, planTimeProfile, childListener, cancelOnFailure);
            case LeafContext leaf -> scheduledLeaves.add(new ScheduledLeaf(leaf, childListener));
        }
    }

    /**
     * Settles this merge's child refs and then aborts the subtree, in that order. The pairing is a method rather than two adjacent
     * statements because the order is load-bearing and the two halves must not drift apart.
     * <p>
     * Settling first is what makes the abort safe. {@code abortChildrenWithSettledListeners} marks each leaf's {@link ParentSink} as
     * finished, and {@link #executeLeaf}'s skip path then returns for any leaf not yet dispatched <em>without</em> completing its
     * listener - so a ref left outstanding here is never completed by anyone, and the query hangs. (Assertions catch it eventually:
     * {@code ComputeListener}'s refs are wrapped in {@code ActionListener.assertAtLeastOnce}, which registers them with
     * {@code LeakTracker} - but only on GC, long after the request stalled.)
     * <p>
     * Settling cannot be moved <em>into</em> the abort either: the abort is also reached for children whose refs are already settled or
     * were never acquired, and completing a ref for a leaf that is still running would drop the {@code ComputeListener} to zero and
     * fire {@code terminalListener} out from under live drivers.
     *
     * @param childListeners the refs acquired for this merge's children - only those successfully acquired, so possibly fewer than
     *                       {@code mergeContext.children.size()}, or empty
     */
    private void settleThenAbortChildren(
        MergeContext mergeContext,
        List<ActionListener<DriverCompletionInfo>> childListeners,
        Exception failure
    ) {
        // notifyOnce makes these no-ops if runCompute or a concurrent completion already absorbed them.
        childListeners.forEach(l -> l.onFailure(failure));
        abortChildrenWithSettledListeners(mergeContext, failure);
    }

    /**
     * Finishes the exchange sinks this merge registered for its children. Idempotent: nested merges that have already started are
     * skipped by their {@code started} CAS and will clean themselves up through their own terminal listener; leaves are skipped by
     * {@code ParentSink.finished}.
     * <p>
     * <b>Precondition:</b> every child ref of {@code mergeContext} is already settled, or was never acquired. This method does not
     * complete them, and it marks leaf sinks finished, which makes {@link #executeLeaf} skip any undispatched leaf without completing
     * its listener - so calling this with a live child ref strands that ref and hangs the query. Each caller satisfies the
     * precondition differently, which is why it is stated here rather than inferred:
     * <ul>
     *   <li>{@link #settleThenAbortChildren} - settles them immediately before calling;</li>
     *   <li>{@code startMerge}'s outer backstop - only reachable once the refs are settled or were never acquired (see the
     *       enumeration in that method's javadoc);</li>
     *   <li>{@link #abortUnstartedMergeContext} - the merge never started, so it never called {@code acquireCompute} for its children
     *       and its leaves were never added to {@link #scheduledLeaves}, leaving nothing to settle or dispatch.</li>
     * </ul>
     * A new caller must establish one of those before using this method; if it holds live child refs, it wants
     * {@link #settleThenAbortChildren} instead.
     */
    private void abortChildrenWithSettledListeners(MergeContext mergeContext, Exception failure) {
        for (SubPlanContext child : mergeContext.children) {
            switch (child) {
                case MergeContext merge -> abortUnstartedMergeContext(merge, failure);
                case LeafContext leaf -> finishParentSink(leaf.parentSink, failure);
            }
        }
    }

    /**
     * Aborts a merge subtree that has not yet been started by {@link #startMerge}. Idempotent: a CAS on {@link MergeContext#started}
     * ensures at most one caller performs the abort. If the merge has already started, its own terminal listener will clean up its
     * resources, so this method returns immediately.
     * <p>
     * When the CAS succeeds, recursively aborts all children via {@link #abortChildrenWithSettledListeners}, deregisters the exchange
     * source, drains it via {@link ExchangeSourceHandler#finishEarly}, and signals the parent sink with the failure.
     *
     * @param mergeContext the merge node to abort
     * @param failure      the exception to propagate to the parent sink and any child sinks
     */
    private void abortUnstartedMergeContext(MergeContext mergeContext, Exception failure) {
        if (mergeContext.started.compareAndSet(false, true) == false) {
            return;
        }
        LOGGER.debug("aborting unstarted merge subtree [{}]", mergeContext.path);
        abortChildrenWithSettledListeners(mergeContext, failure);
        removeExchangeSource(mergeContext);
        mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
        finishParentSink(mergeContext.parentSink, failure);
    }

    /**
     * Deregisters the {@link ExchangeSourceHandler} for {@code mergeContext} from the {@link ExchangeService}. Idempotent via a CAS on
     * {@link MergeContext#sourceRemoved}: only the first caller performs the removal. Called from both the normal terminal listener and
     * the abort path so that the exchange service does not hold stale handlers after the query ends.
     */
    private void removeExchangeSource(MergeContext mergeContext) {
        if (mergeContext.sourceRemoved.compareAndSet(false, true)) {
            exchangeService.removeExchangeSourceHandler(mergeContext.exchangeId);
        }
    }

    /**
     * Signals a {@link ParentSink} that its producer has finished. Idempotent via a CAS on {@link ParentSink#finished}: only the first
     * caller performs the signal. A {@code null} {@code parentSink} (root merge, which writes directly into {@code collectedPages}) is
     * treated as a no-op.
     * <p>
     * For a lazy leaf sink, releases the keep-alive ref on the parent source; if the leaf was never dispatched (no handler was ever
     * created), nothing is registered in the {@link ExchangeService} and there is nothing further to do. Otherwise: on success, the
     * sink handler is finished asynchronously after all in-flight pages have drained ({@link
     * ExchangeSinkHandler#addCompletionListener}). On failure, the sink handler is finished immediately so the parent's
     * {@link ExchangeSourceHandler} receives the error without waiting for pages that will never arrive.
     *
     * @param parentSink the sink to signal; {@code null} for the root merge
     * @param failure    the exception if the producer failed; {@code null} on success
     */
    private void finishParentSink(ParentSink parentSink, Exception failure) {
        if (parentSink == null || parentSink.finished.compareAndSet(false, true) == false) {
            return;
        }
        // Null for eager (merge) sinks; releaseOnce makes this idempotent with the release in attach().
        Releasables.close(parentSink.pendingRef);
        ExchangeSinkHandler handler = parentSink.handler;
        if (handler == null) {
            return;
        }
        if (failure == null) {
            handler.addCompletionListener(ActionListener.running(() -> exchangeService.finishSinkHandler(parentSink.sessionId, null)));
        } else {
            exchangeService.finishSinkHandler(parentSink.sessionId, failure);
        }
    }

    /**
     * Atomically claims the next leaf from {@link #scheduledLeaves} and executes it. If the index is beyond the list (all leaves
     * claimed), returns immediately. Each executing leaf calls this method as its {@code onDone} callback, so the number of
     * concurrently running leaves stays at most {@code branchParallelDegree} throughout the query.
     */
    private void tryExecuteNextLeaf() {
        int index = nextLeafIndex.getAndIncrement();
        if (index >= scheduledLeaves.size()) {
            return;
        }
        executeLeaf(scheduledLeaves.get(index), this::tryExecuteNextLeaf);
    }

    /**
     * Dispatches a single leaf to {@link ComputeService#executePlan}. If the root task has already been cancelled, or the leaf's
     * merge segment already aborted it during phase 2, skips dispatch. On completion (success or failure), {@code onDone} is
     * invoked so the caller can claim the next leaf.
     * <p>
     * The leaf's exchange sink is created here, via {@link ParentSink#attach}, not in phase 1: an idle registered sink handler
     * would be reaped by the exchange service's inactive-sink reaper while the leaf waits behind {@code branchParallelDegree}.
     * <p>
     * A synchronous throw from {@code attach} or {@code executePlan} is routed to {@link #finishLeaf}: refill dispatches run on
     * the search executor, where an escaping exception would be swallowed and the leaf's listener — a {@code ComputeListener}
     * ref — would never complete, hanging the query. {@code finishLeaf} is safe to call from the catch even if {@code executePlan}
     * already notified its listener before throwing: the leaf listener is notifyOnce-wrapped and {@link #finishParentSink} is
     * CAS-guarded. The refill itself is wrapped in a {@link RunOnce} so the slot cannot be refilled twice by one leaf.
     * <p>
     * Symmetrically, a throw out of {@code finishLeaf} itself must not cost the slot its refill, which is why every outcome here goes
     * through {@link #settleLeafAndRefill} rather than calling {@code finishLeaf} directly.
     *
     * @param scheduledLeaf the leaf to dispatch, containing its plan and parent sink
     * @param onDone        callback invoked after the leaf finishes (used for self-refilling dispatch)
     */
    private void executeLeaf(ScheduledLeaf scheduledLeaf, Runnable onDone) {
        LeafContext leafContext = scheduledLeaf.leafContext;
        ParentSink parentSink = leafContext.parentSink;
        LOGGER.debug("dispatching leaf [{}]", leafContext.path);
        Runnable onDoneOnce = new RunOnce(() -> submitOnDone(onDone));
        // All paths (cancellation, skip, success, failure) use submitOnDone rather than calling onDone inline. If executePlan or
        // notifyIfCancelled completes before returning, the listener fires on the current thread, and a direct onDone.run() would
        // recurse through the entire remaining queue (tryExecuteNextLeaf → executeLeaf → … → onDone.run() → …), overflowing the
        // stack when many leaves are queued.
        //
        // Every path settles the leaf through settleLeafAndRefill, which refills from a finally - see that method for why.
        if (rootTask.notifyIfCancelled(ActionListener.wrap(ignored -> {}, e -> settleLeafAndRefill(scheduledLeaf, null, e, onDoneOnce)))) {
            return;
        }
        if (parentSink.finished.get()) {
            // The leaf's merge segment failed and aborted this sink during phase 2. Skip dispatch and just refill the slot: the abort
            // came from startMerge's inner catch, which completes every childListener - this leaf's included - before aborting. (Its
            // outer backstop also aborts without completing them, but as that javadoc enumerates, it can only run once those refs are
            // already settled or were never acquired, so no undispatched leaf reaches here through it.)
            onDoneOnce.run();
            return;
        }
        try {
            Supplier<ExchangeSink> exchangeSinkSupplier = parentSink.attach();
            computeService.executePlan(
                parentSink.sessionId,
                rootTask,
                flags,
                leafContext.plan,
                configuration,
                foldContext,
                execInfo,
                leafContext.path,
                ActionListener.wrap(
                    result -> settleLeafAndRefill(scheduledLeaf, result.completionInfo(), null, onDoneOnce),
                    e -> settleLeafAndRefill(scheduledLeaf, null, e, onDoneOnce)
                ),
                exchangeSinkSupplier,
                initialClusterStatuses,
                configuration.profile() ? new PlanTimeProfile() : null
            );
        } catch (Exception e) {
            settleLeafAndRefill(scheduledLeaf, null, e, onDoneOnce);
        }
    }

    /**
     * Settles a finished leaf via {@link #finishLeaf} and then refills its dispatch slot, whatever happened. Used by every outcome in
     * {@link #executeLeaf} - cancellation, success, failure, and a synchronous throw - so the refill is unconditional in all of them.
     * <p>
     * The refill sits in a {@code finally} because a throw out of {@code finishLeaf} would otherwise cost the slot its refill, leaving
     * every leaf still queued behind {@code branchParallelDegree} undispatched and its {@code ComputeListener} ref unreleased - a
     * silent hang. That {@code finishLeaf} can throw is not hypothetical: {@link #failRemainingLeaves} guards each call for the same
     * reason. The cancellation path needs this most, because it is the one path with no surrounding {@code try/catch}:
     * {@code notifyIfCancelled} invokes its listener before returning, and {@code ActionListener.wrap} routes a throwing
     * failure-consumer through {@code safeAcceptException}, which swallows the exception rather than rethrowing it - so without the
     * {@code finally} nothing upstream would notice the lost refill.
     * <p>
     * Note this cannot be expressed with {@code ActionListener.runAfter}, which is otherwise exactly this shape: it wraps the result in
     * {@code assertOnce}, and {@link #executeLeaf}'s outer catch deliberately settles a leaf that {@code executePlan} may already have
     * notified before throwing. Double settling is safe here - the leaf listener is notifyOnce-wrapped, {@link #finishParentSink} is
     * CAS-guarded, and {@code onDoneOnce} is a {@link RunOnce} - but it would trip that assertion.
     *
     * @param onDoneOnce the {@link RunOnce}-wrapped refill, so repeat settling cannot refill the slot twice
     */
    private void settleLeafAndRefill(
        ScheduledLeaf scheduledLeaf,
        DriverCompletionInfo completionInfo,
        Exception failure,
        Runnable onDoneOnce
    ) {
        try {
            finishLeaf(scheduledLeaf, completionInfo, failure);
        } finally {
            onDoneOnce.run();
        }
    }

    /**
     * Submits {@code onDone} to the search executor, breaking the call chain that would otherwise recurse synchronously through the
     * entire remaining leaf queue when a leaf completes inline. All dispatch contexts — cancellation, skip, success, and failure — use
     * this method so that each subsequent leaf is dispatched on a fresh stack frame.
     * <p>
     * The refill is force-executed: dropping it under transient queue pressure would either strand every leaf still queued behind it
     * (hang) or require failing them all (failing the query for a momentarily full queue). One short dispatch task per completed leaf
     * is bounded work. Rejection therefore only happens on executor shutdown, where draining the remaining leaves via
     * {@link #failRemainingLeaves} is the right response: the query terminates with an error rather than hanging indefinitely.
     */
    private void submitOnDone(Runnable onDone) {
        var refill = new AbstractRunnable() {
            @Override
            protected void doRun() {
                onDone.run();
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onRejection(Exception e) {
                failRemainingLeaves(e);
            }

            @Override
            public void onFailure(Exception e) {
                // executeLeaf routes synchronous throws to finishLeaf, so onDone must not throw. Drain before asserting:
                // an AssertionError raised first would skip the drain and hang the query on exactly the path meant to rescue it.
                failRemainingLeaves(e);
                assert false : e;
            }
        };
        try {
            searchExecutor.execute(refill);
        } catch (Exception e) {
            // EsThreadPoolExecutor routes rejection to onRejection above; this covers plain executors that throw
            // synchronously from execute(). failRemainingLeaves claims leaves exclusively, so double entry is harmless.
            failRemainingLeaves(e);
        }
    }

    /**
     * Atomically claims every undispatched leaf and reports {@code cause} to its listener, allowing the {@link ComputeListener} to reach
     * zero and the terminal listener to fire. Called when the slot's self-refilling dispatch chain would otherwise be permanently broken:
     * the search executor is shutting down, a non-standard executor threw, or the phase-3 dispatch of the initial wave threw partway
     * through.
     */
    private void failRemainingLeaves(Exception cause) {
        Exception drainFailure = null;
        int index;
        while ((index = nextLeafIndex.getAndIncrement()) < scheduledLeaves.size()) {
            ScheduledLeaf scheduledLeaf = scheduledLeaves.get(index);
            try {
                finishLeaf(scheduledLeaf, null, cause);
            } catch (Exception e) {
                // Keep draining: a throw for one leaf must not strand the leaves after it, whose ComputeListener refs would never
                // release and would hang the query. That is also why the assertion is deferred until the loop is done - raising it
                // here would abandon the rest of the drain and cause the very hang this catch exists to avoid.
                LOGGER.warn("failed to fail leaf [{}]", scheduledLeaf.leafContext.path, e);
                if (drainFailure == null) {
                    drainFailure = e;
                } else {
                    drainFailure.addSuppressed(e);
                }
            }
        }
        assert drainFailure == null : drainFailure;
    }

    /**
     * Signals the leaf's parent sink and then forwards the result to the leaf's {@link ScheduledLeaf#listener}.
     * Always called after {@link ComputeService#executePlan} completes, whether successfully or not.
     *
     * @param scheduledLeaf  the leaf that just finished
     * @param completionInfo profiling data from the leaf's drivers; {@code null} on failure
     * @param failure        the exception if the leaf failed; {@code null} on success
     */
    private void finishLeaf(ScheduledLeaf scheduledLeaf, DriverCompletionInfo completionInfo, Exception failure) {
        if (failure == null) {
            finishParentSink(scheduledLeaf.leafContext.parentSink, null);
            scheduledLeaf.listener.onResponse(completionInfo);
        } else {
            finishParentSink(scheduledLeaf.leafContext.parentSink, failure);
            scheduledLeaf.listener.onFailure(failure);
        }
    }

    /**
     * The exchange id of the node at {@code path}: the key its {@link ParentSink} is registered under. For a leaf it is also the
     * session id handed to {@code ComputeService.executePlan}, which registers the leaf's own exchange source under it and runs the
     * leaf's computes under it or under sessions derived from it. A nested merge derives the key for its own exchange source by
     * appending {@code "/merge"} to this, so a single node's sink and source keys can never be confused.
     * <p>
     * The {@code "/"} follows the convention {@code ComputeService.newChildSession} uses to nest sessions - which is how
     * {@link #sessionPrefix} itself was built, and how the data node handlers extend it further still.
     * <p>
     * With {@code sessionId = "s"} and {@code sessionPrefix = "s/1"}:
     * <pre>
     * path                   nodeSessionId(path)         source key derived from it
     * "subplan-0"            "s/1/subplan-0"             (leaf - owns no source here)
     * "subplan-1"            "s/1/subplan-1"             "s/1/subplan-1/merge"
     * "subplan-1.subplan-0"  "s/1/subplan-1.subplan-0"   (leaf)
     * </pre>
     * The root merge is not addressed this way at all: its {@code path} is {@code null}, it has no sink, and its source stays
     * registered under the bare {@code sessionId}.
     */
    private String nodeSessionId(String path) {
        return sessionPrefix + "/" + path;
    }

    /**
     * The address of child number {@code child} of the node at {@code parentPath}: children of the root are {@code "subplan-0"},
     * {@code "subplan-1"}, and a grandchild is {@code "subplan-1.subplan-0"}.
     * <p>
     * Levels are joined with {@code "."} rather than {@code "/"} for two reasons. {@link #nodeSessionId} embeds this address in a
     * session id whose own levels are {@code "/"}-separated, so a dotted address stays one readable segment there instead of
     * blurring into the session nesting that {@code newChildSession} and the data node handlers add around it. And the same string
     * is the profile qualifier, where a dot is already the separator before the role - which is what yields descriptions like
     * {@code "subplan-1.subplan-0.final"}.
     */
    private static String childPath(String parentPath, int child) {
        String childName = "subplan-" + child;
        return parentPath == null ? childName : parentPath + "." + childName;
    }

    /**
     * Common base for the two kinds of execution node that {@link #buildSubPlanContext} produces. Every node has a physical plan to
     * execute, a path that identifies it within the query (used for session IDs and profiling), and a {@link ParentSink} that the node
     * writes its output into ({@code null} for the root merge, which writes directly into {@code collectedPages}).
     */
    private abstract static sealed class SubPlanContext permits LeafContext, MergeContext {
        final PhysicalPlan plan;
        final String path;
        final ParentSink parentSink;

        private SubPlanContext(PhysicalPlan plan, String path, ParentSink parentSink) {
            this.plan = plan;
            this.path = path;
            this.parentSink = parentSink;
        }
    }

    /**
     * An immutable descriptor for a leaf producer plan. Carries what {@link #executeLeaf} needs to call
     * {@code ComputeService.executePlan}: the physical plan, the path, and the lazy {@link ParentSink} the leaf writes into.
     * <p>
     * It has no state of its own. The two things that do change over a leaf's life are held elsewhere: its exchange sink by
     * {@link ParentSink}, and the listener that must be completed when it finishes by {@link ScheduledLeaf}.
     */
    private static final class LeafContext extends SubPlanContext {
        private LeafContext(PhysicalPlan plan, String path, ParentSink parentSink) {
            super(plan, path, parentSink);
        }
    }

    /**
     * The stateful execution node for a coordinator merge segment. Built by {@link #buildSubPlanContext} and consumed by
     * {@link #startMerge}. Holds the wired exchange infrastructure and mutable lifecycle guards:
     * <ul>
     *   <li>{@code exchangeId} — the key {@code exchangeSource} is registered under in {@link ExchangeService}. The root uses the
     *       bare {@code sessionId}, because that is the key {@link ExchangeService#finishSessionEarly} looks up to stop a query
     *       early; nested segments use {@code <sessionPrefix>/<path>/merge}.</li>
     *   <li>{@code computeSessionId} — the session this segment's compute runs under, passed to {@code ComputeService.runCompute}.
     *       It differs from {@code exchangeId} only for the root, whose exchange has to keep the bare {@code sessionId} while its
     *       compute uses the per-executor prefix so that repeated rounds of one query do not collide.</li>
     *   <li>{@code exchangeSource} — the {@link ExchangeSourceHandler} this segment reads from; children write into it via their
     *       {@link ParentSink}s.</li>
     *   <li>{@code children} — direct children, each either a {@link MergeContext} (another coordinator segment) or a
     *       {@link LeafContext} (a producer to dispatch); iterated by {@code startMerge} and the abort paths.</li>
     *   <li>{@code started} — arbitrates between starting and aborting this node: {@link #startMerge} sets it, and
     *       {@link #abortUnstartedMergeContext} declines to tear down a node that is already set, leaving that to the node's own
     *       terminal listener. The CAS therefore has to run unconditionally rather than inside an assertion, or the abort path
     *       would tear down running segments in builds with assertions disabled.</li>
     *   <li>{@code sourceRemoved} — atomic boolean guard: {@code compareAndSet(false, true)} ensures {@code removeExchangeSource} runs
     *       at most once per node.</li>
     * </ul>
     */
    private static final class MergeContext extends SubPlanContext {
        private final String exchangeId;
        private final String computeSessionId;
        private final ExchangeSourceHandler exchangeSource;
        private final List<SubPlanContext> children;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean sourceRemoved = new AtomicBoolean();

        private MergeContext(
            PhysicalPlan plan,
            String path,
            String exchangeId,
            String computeSessionId,
            ExchangeSourceHandler exchangeSource,
            ParentSink parentSink,
            List<SubPlanContext> children
        ) {
            super(plan, path, parentSink);
            this.exchangeId = exchangeId;
            this.computeSessionId = computeSessionId;
            this.exchangeSource = exchangeSource;
            this.children = children;
        }
    }

    /**
     * The producer-side of an exchange: the {@link ExchangeSinkHandler} that a child node (leaf or nested merge) writes its output
     * into, together with the session ID under which it is registered in {@link ExchangeService}. Created in
     * {@link #buildChildContext} for every child of a merge node and stored in the child's {@link SubPlanContext#parentSink}.
     * <p>
     * Nested-merge sinks are <b>eager</b>: their handler is registered in phase 1 and their {@code runCompute} attaches an
     * {@link ExchangeSink} to it synchronously in phase 2, so the {@code InactiveSinksReaper} sees them as active. Leaf sinks are
     * <b>lazy</b>: a leaf can sit in {@link #scheduledLeaves} behind {@code branchParallelDegree} for longer than the reaper's
     * inactive interval, and an idle registered handler (no attached sink, empty buffer) would be reaped, silently dropping or
     * failing that branch. A lazy sink therefore registers nothing in phase 1; it holds the parent source open with
     * {@code pendingRef}, an {@link ExchangeSourceHandler#addEmptySink} ref, and creates the handler in {@link #attach} when the
     * leaf is actually dispatched.
     * <p>
     * {@link #finishParentSink} uses the {@code finished} guard to act exactly once, regardless of whether the child completed
     * successfully, failed, or was aborted. It always releases {@code pendingRef}. If a handler exists it is then deregistered: on
     * success after waiting for it to drain (via {@link ExchangeSinkHandler#addCompletionListener}), on failure immediately, so the
     * parent's source sees the error rather than waiting for pages that will never arrive. A lazy sink whose leaf was never
     * dispatched has no handler, so releasing the ref is all there is to undo.
     */
    private final class ParentSink {
        private final String sessionId;
        private final AtomicBoolean finished = new AtomicBoolean();
        @Nullable
        private final ExchangeSourceHandler parentSource; // non-null only for lazy (leaf) sinks
        @Nullable
        private final Releasable pendingRef; // non-null only for lazy (leaf) sinks
        // Set in the constructor for eager (merge) sinks; published by attach() for lazy (leaf) sinks.
        private volatile ExchangeSinkHandler handler;

        /** Eager (nested merge): the handler is registered in phase 1 and wired into the parent source by the caller. */
        private ParentSink(String sessionId, ExchangeSinkHandler handler) {
            this.sessionId = sessionId;
            this.handler = handler;
            this.parentSource = null;
            this.pendingRef = null;
        }

        /** Lazy (leaf): no handler yet; {@code pendingRef} keeps the parent source open until {@link #attach} or abort. */
        private ParentSink(String sessionId, ExchangeSourceHandler parentSource) {
            this.sessionId = sessionId;
            this.parentSource = parentSource;
            this.pendingRef = Releasables.releaseOnce(parentSource.addEmptySink());
        }

        /**
         * Registers this leaf's {@link ExchangeSinkHandler} and wires it into the parent source. Called only from
         * {@link #executeLeaf}, on the thread that exclusively claimed the leaf, and only while {@code finished} is false
         * (aborts happen synchronously in phase 2, before any leaf is dispatched).
         *
         * @return the exchange-sink supplier to pass to {@code ComputeService.executePlan}
         */
        private Supplier<ExchangeSink> attach() {
            assert handler == null : "sink [" + sessionId + "] attached twice";
            assert finished.get() == false : "sink [" + sessionId + "] already finished";
            ExchangeSinkHandler attached = exchangeService.createSinkHandler(sessionId, queryPragmas.exchangeBufferSize());
            // Publish before addRemoteSink so every later finishParentSink call sees and deregisters the handler.
            handler = attached;
            parentSource.addRemoteSink(attached::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
            // addRemoteSink holds its own keep-alive ref on the parent source until the remote sink completes
            // (ExchangeSourceHandler wraps the fetcher in releaseAfter(..., addEmptySink())), so the phase-1
            // pending ref is no longer needed. releaseOnce makes this idempotent with finishParentSink.
            pendingRef.close();
            return () -> attached.createExchangeSink(() -> {});
        }
    }

    /**
     * Pairs a {@link LeafContext} with the {@link ActionListener} that must be notified when the leaf finishes. Populated into
     * {@link #scheduledLeaves} during {@link #startMerge} (phase 2) and consumed by {@link #tryExecuteNextLeaf} (phase 3).
     */
    private record ScheduledLeaf(LeafContext leafContext, ActionListener<DriverCompletionInfo> listener) {}

}

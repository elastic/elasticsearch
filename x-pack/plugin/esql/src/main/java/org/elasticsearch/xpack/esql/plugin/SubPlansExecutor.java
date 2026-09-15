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
import org.elasticsearch.tasks.TaskCancelledException;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    /**
     * Unique per executor instance ({@code ComputeService.newChildSession(sessionId)}). {@code ComputeService.execute} runs multiple
     * times with the same request {@code sessionId} within one query — once per coordinator subplan (INLINE STATS, IN subquery etc.) plus
     * the main plan — while sink deregistration on the success path is deferred until the handler drains. Deriving non-root exchange ids
     * (and the root's {@code computeSessionId}) from this prefix prevents a later round from colliding with a not-yet-deregistered handler
     * of an earlier round. The root exchange source stays under the bare {@code sessionId}; see {@link #buildSubPlanContext}.
     */
    private final String sessionPrefix;
    private final CancellableTask rootTask;
    private final EsqlFlags flags;
    private final Configuration configuration;
    private final FoldContext foldContext;
    private final EsqlExecutionInfo execInfo;
    private final Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses;
    @Nullable
    private final Runnable warnIndexCoordinatorOnce;
    private final QueryPragmas queryPragmas;
    /**
     * All {@link MergeContext} nodes registered during setup, in pre-order (root first, children after their parent). Populated by
     * {@link #buildSubPlanContext}, never cleared — it is the authoritative list for {@link #drainUnstartedMergesOnce} and
     * {@link #abortUnstartedMerges}, which need to visit every merge, not just those that haven't started. It also serves as the
     * rollback ledger for {@link #cleanupUnstartedExchanges}: when {@link #buildSubPlanContext} throws partway through, this list records
     * which exchange sources were registered and must be undone.
     */
    private final List<MergeContext> allMergeContexts = new ArrayList<>();
    /**
     * Rollback ledger for phase 1, the {@link ParentSink} counterpart of {@link #allMergeContexts}: one entry per child of every merge
     * node, in creation order. Each entry holds the keep-alive ref that child took on its parent's exchange source; since all sinks are
     * lazy, that ref is the whole of what phase 1 acquires for a child. The root merge is absent, since it writes into
     * {@code collectedPages} rather than into a sink. Read only by {@link #cleanupUnstartedExchanges}, emptied once phase 1 succeeds.
     */
    private final List<ParentSink> unstartedParentSinks = new ArrayList<>();
    // Flat list of leaves/branches populated during allocateComputeRefs; dispatched in phase 2b.
    private final List<ScheduledLeaf> scheduledLeaves = new ArrayList<>();
    private final AtomicInteger nextLeafIndex = new AtomicInteger();
    /**
     * Set when the root merge's {@code runCompute} succeeds after leaf dispatch has started. LIMIT and STOP {@code finishEarly} the
     * coordinator source and complete that {@code runCompute} while leaves may still be queued; together with
     * {@code EsqlExecutionInfo#isStopped}, {@link #executeLeaf} then skips them.
     */
    private final AtomicBoolean noMoreLeaves = new AtomicBoolean();
    private final AtomicBoolean leafDispatchStarted = new AtomicBoolean();
    /**
     * Guards the success-path drain so the tree is walked once however many callers reach {@link #drainUnstartedMergesOnce}. The walk
     * is already idempotent - {@link #startMerge}'s {@code started} CAS skips a merge that ran - so this only avoids repeating it.
     */
    private final AtomicBoolean unstartedMergesDrained = new AtomicBoolean();

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
        this(
            computeService,
            exchangeService,
            searchExecutor,
            sessionId,
            rootTask,
            flags,
            configuration,
            foldContext,
            execInfo,
            initialClusterStatuses,
            null
        );
    }

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
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses,
        Runnable warnIndexCoordinatorOnce
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
        this.warnIndexCoordinatorOnce = warnIndexCoordinatorOnce;
        this.queryPragmas = configuration.pragmas();
    }

    /**
     * Executes a nested {@link SubPlan.Merge} topology in two(1a, 1b) setup steps and a lazy dispatch.
     * <p>
     * <b>Phase 1a – register exchanges ({@code buildSubPlanContext}):</b> a synchronous tree walk that registers an
     * {@link ExchangeSourceHandler} for every {@link SubPlan.Merge} node. No {@link ExchangeSinkHandler} is registered here: every
     * child, leaf or nested merge, gets a lazy keep-alive ref on its parent's source instead. Each {@link LeafContext} records the
     * ancestor chain from root to its direct parent, so {@link #ensureAncestorsStarted} can start those ancestors top-down at
     * dispatch time. This step has no async side effects; an exception partway through is rolled back by {@link #cleanupUnstartedExchanges}
     * before propagating to {@code listener}.
     * <p>
     * <b>Phase 1b – allocate refs ({@code allocateComputeRefs}):</b> a second walk that opens a {@link ComputeListener} per merge,
     * stores each node's {@code segmentListener} and {@code childListeners}, and flattens leaves into {@link #scheduledLeaves}.
     * No drivers start. Failures here do not use {@link #cleanupUnstartedExchanges}; they settle listeners and abort via
     * {@code cancelOnFailure}. {@code allocateComputeRefs} itself does not throw.
     * <p>
     * <b>Phase 2a – start root:</b> {@link #startMerge} runs the root coordinator before any leaf is dispatched, so the
     * exchange the leaves write into is already consuming. Nested merge segments stay unstarted. If Phase 1b already aborted the
     * root, {@code startMerge} loses the {@code started} CAS and this method returns without dispatching leaves.
     * <p>
     * <b>Phase 2b – lazy dispatch:</b> launches {@code min(branchParallelDegree, leafCount)} initial workers. Each worker calls
     * {@link #tryExecuteNextLeaf}, which atomically claims the next leaf from {@link #scheduledLeaves} via {@link #nextLeafIndex}
     * and re-invokes itself on completion. Before dispatching each leaf, {@link #ensureAncestorsStarted} starts any <em>nested</em>
     * ancestor merge segments (root-first, CAS-guarded) that have not yet been started. Root is already running from Phase 2a, so
     * that CAS is a no-op. Nested merge drivers appear in the task list only when there is actual leaf work under them.
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
     * Phase 1a: registers 3 exchange sources and 6 lazy keep-alive refs (inner1, inner2 and one per leaf).
     * Phase 1b: allocates ComputeListener ref trees for root, inner1, inner2; flattens leaves into scheduledLeaves = [a, b, c, d].
     *           Ancestors: a→[root], b→[root,inner1], c→[root,inner1,inner2], d→[root,inner1,inner2].
     * Phase 2a: starts root.
     * Dispatch: two workers claim a and b. Claiming a, ensureAncestorsStarted is a no-op CAS on root. Claiming b, root is already started,
     *           so ensureAncestorsStarted starts inner1. c is claimed next; root and inner1 are started, so ensureAncestorsStarted starts
     *           inner2 only. d finds all ancestors started.
     * </pre>
     */
    void execute(SubPlan.Merge executionPlan, PlanTimeProfile planTimeProfile, ActionListener<Result> listener) {
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());

        // Phase 1a: register all exchange sources. If buildSubPlanContext throws partway, some ExchangeSourceHandlers
        // may already be registered; all sinks are lazy so none exist yet. cleanupUnstartedExchanges rolls the sources back before failing.
        final MergeContext root;
        try {
            root = buildSubPlanContext(executionPlan, null, null, collectedPages, List.of());
        } catch (Exception e) {
            try {
                cleanupUnstartedExchanges(e);
            } catch (Exception cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            } finally {
                listener.onFailure(e);
            }
            return;
        }

        // Phase 1a succeeded. cleanupUnstartedExchanges is no longer reachable for exchange teardown; release its sink ledger.
        unstartedParentSinks.clear();

        // On failure, release any pages already collected to avoid memory leaks.
        ActionListener<DriverCompletionInfo> completionListener = ActionListener.wrap(profiles -> {
            execInfo.markEndQuery();
            listener.onResponse(new Result(root.plan.output(), collectedPages, null, configuration, profiles, execInfo, null));
        }, e -> {
            collectedPages.forEach(p -> Releasables.closeExpectNoException(p::releaseBlocks));
            listener.onFailure(e);
        });

        // One drain-and-cancel for the entire query, guarded by the compare-and-set that records the first failure: whichever thread
        // stores that failure is the one that drains, so the drain always has the original cause to settle unstarted merges with,
        // rather than a synthetic TaskCancelledException that can become the only cause a notifyOnce terminal sees. Later failures
        // lose the CAS and do nothing, which is what keeps cancelTaskAndDescendants to at most one call however many merge nodes fail;
        // a guard per merge node (depth D) would fire any non-idempotent side-effect D times on a cascading failure.
        // Draining is bundled in here so that merge segments which were never started still have their pre-allocated refs settled,
        // letting their ComputeListeners reach zero and fire their terminal listeners — otherwise a failure that cuts the query short
        // before all merges start would hang.
        final AtomicReference<Exception> primaryFailure = new AtomicReference<>();
        final Runnable innerCancelOnFailure = computeService.cancelQueryOnFailure(rootTask);
        final Consumer<Exception> cancelOnFailure = e -> {
            if (primaryFailure.compareAndSet(null, e)) {
                try {
                    abortUnstartedMerges(e);
                } finally {
                    innerCancelOnFailure.run();
                }
            }
        };

        // Phase 1b: allocate ComputeListener ref trees for all merge nodes without starting any drivers. Leaves are added to
        // scheduledLeaves, allocateComputeRefs never throws, all failure paths route through listeners. A failure there can already abort
        // the root via cancelOnFailure; Phase 2a must honor that CAS.
        allocateComputeRefs(root, planTimeProfile, completionListener, cancelOnFailure);

        LOGGER.debug(
            "topology built: [{}] merge nodes, [{}] leaves, branchParallelDegree=[{}]",
            allMergeContexts.size(),
            scheduledLeaves.size(),
            queryPragmas.branchParallelDegree()
        );

        // Phase 2a: start the root merge segment eagerly, before leaf dispatch begins. The root reads from the exchange that leaves write
        // into, so it must be consuming before any leaf starts producing. Nested merge segments are started lazily, just-in-time by
        // ensureAncestorsStarted, to avoid an upfront burst of drivers. Starting root before leafDispatchStarted=true preserves the
        // invariant checked in its segmentListener wrapper: if a synchronous stub (or any caller) calls the root listener before leaf
        // dispatch has begun, the wrapper sees leafDispatchStarted=false and does NOT set noMoreLeaves or drain unstarted merges —
        // a synchronous completion during setup means the driver was started, not that LIMIT fired. The real LIMIT path fires the
        // listener later, when leafDispatchStarted is already true, and only then does the wrapper set noMoreLeaves and drain.
        // If allocateComputeRefs already failed, drain claimed root.started and fired the terminal listener — do not start
        // drivers or dispatch leaves against aborted sinks. startMerge's failed CAS is a no-op for ensureAncestorsStarted
        // (sibling already started); here it means abort already won, so stop.
        try {
            if (startMerge(root) == false) {
                return;
            }
        } catch (Exception e) {
            return;
        }

        // Phase 2b: dispatch leaves/branches up to branchParallelDegree, ensureAncestorsStarted is called inside executeLeaf to start
        // ancestor nested-merge segments just-in-time.
        leafDispatchStarted.set(true);
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
     * nodes need as it goes. Both results matter: the tree is what phases 2a and 2b walk, and the registrations are what
     * {@link #cleanupUnstartedExchanges} has to undo if this method throws partway through.
     * <p>
     * From the consumer side, every {@link SubPlan.Merge} becomes a {@link MergeContext} owning the {@link ExchangeSourceHandler} it
     * reads, and from the producer side every child of it gets a {@link ParentSink} to write into. Both kinds of child get the same
     * <b>lazy</b> sink: nothing is registered here, only a keep-alive ref on the parent's source, with the
     * {@link ExchangeSinkHandler} created when the child is actually started. See {@link ParentSink} for why waiting is required
     * rather than merely cheaper.
     * <ul>
     *   <li>A nested {@link SubPlan.Merge} child recurses through this method, and attaches its sink when its segment is started by
     *       {@link #ensureAncestorsStarted}.</li>
     *   <li>A {@link SubPlan.Leaf} child becomes a {@link LeafContext}, and attaches its sink when it is dispatched in phase 2.</li>
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
     *                  parentSink=ParentSink(id="s/1/subplan-0", lazy→src0),
     *                  ancestors=[MergeContext@null])
     *   └─ MergeContext(path="subplan-1", exchangeId="s/1/subplan-1/merge", computeSessionId="s/1/subplan-1/merge",
     *                   plan=ExchangeSinkExec→ExchangeSourceExec,
     *                   exchangeSource=src1, parentSink=ParentSink(id="s/1/subplan-1", lazy→src0))
     *      ├─ LeafContext(path="subplan-1.subplan-0", plan=ExchangeSinkExec→LeafB,
     *                     parentSink=ParentSink(id="s/1/subplan-1.subplan-0", lazy→src1),
     *                     ancestors=[MergeContext@null, MergeContext@"subplan-1"])
     *      └─ LeafContext(path="subplan-1.subplan-1", plan=ExchangeSinkExec→LeafC,
     *                     parentSink=ParentSink(id="s/1/subplan-1.subplan-1", lazy→src1),
     *                     ancestors=[MergeContext@null, MergeContext@"subplan-1"])
     *
     * Registered in ExchangeService during setup, in this order:
     *   src0  registered under "s"                   (root merge source; bare sessionId so finishSessionEarly finds it)
     *   src1  registered under "s/1/subplan-1/merge" (inner merge source)
     * No sink is registered during setup. Each is created by ParentSink.attach when its child is started: "s/1/subplan-1" when the
     * inner merge segment starts, and "s/1/subplan-0", "s/1/subplan-1.subplan-0", "s/1/subplan-1.subplan-1" when those leaves are
     * dispatched.
     * </pre>
     * <p>
     * The root node wraps its plan in an {@code OutputExec} to collect final pages into {@code collectedPages}. All other merge nodes
     * use their plan as-is (already contains an {@code ExchangeSinkExec} at the top that feeds the parent source).
     * <p>
     * {@code ancestors} is the chain of {@link MergeContext}s from the root down to the direct parent of {@code executionPlan} (empty for
     * the root call). Each leaf child receives a copy of this list extended with the {@link MergeContext} built for {@code executionPlan},
     * so every {@link LeafContext} ends up with the complete path from root to its immediate parent. That path is later consumed by
     * {@link #ensureAncestorsStarted}, which walks it top-down to start any merge that has not yet been started before the leaf itself is
     * dispatched.
     */
    private MergeContext buildSubPlanContext(
        SubPlan.Merge executionPlan,
        String path,
        ParentSink parentSink,
        List<Page> collectedPages,
        List<MergeContext> ancestors
    ) {
        boolean root = path == null;
        // The root source must stay registered under the bare sessionId: ExchangeService.finishSessionEarly (async stop)
        // looks it up by that key. All other ids derive from the per-executor sessionPrefix to stay unique across rounds.
        String exchangeId = root ? sessionId : nodeSessionId(path) + "/merge";
        String computeSessionId = root ? sessionPrefix : exchangeId;
        ExchangeSourceHandler exchangeSource = new ExchangeSourceHandler(queryPragmas.exchangeBufferSize(), searchExecutor);
        // Root segment collects final pages via OutputExec; nested segments use their plan as-is.
        PhysicalPlan segmentPlan = root ? new OutputExec(executionPlan.plan(), collectedPages::add) : executionPlan.plan();
        var context = new MergeContext(segmentPlan, path, exchangeId, computeSessionId, exchangeSource, parentSink, new ArrayList<>());
        context.registerExchangeSource();
        allMergeContexts.add(context);

        // Build the ancestor chain for children of this merge: all ancestors of this merge, plus this merge itself.
        List<MergeContext> ancestorsForChildren = new ArrayList<>(ancestors.size() + 1);
        ancestorsForChildren.addAll(ancestors);
        ancestorsForChildren.add(context);

        // emptySink keeps the source alive while children are being wired.
        try (var emptySink = exchangeSource.addEmptySink()) {
            for (int i = 0; i < executionPlan.children().size(); i++) {
                buildChildContext(
                    executionPlan.children().get(i),
                    childPath(path, i),
                    exchangeSource,
                    context,
                    collectedPages,
                    ancestorsForChildren
                );
            }
        }
        return context;
    }

    /**
     * Creates the {@link SubPlanContext} for one child of a merge node and appends it to the parent's {@code children} list.
     * Every child - leaf or nested merge - gets a lazy {@link ParentSink} that registers nothing in phase 1, holding the parent's
     * {@link ExchangeSourceHandler} open with a keep-alive ref until the child is actually started (see {@link ParentSink}).
     * <p>
     * Each {@link ParentSink} is recorded in {@link #unstartedParentSinks} immediately after it is created, so an exception anywhere
     * later in phase 1 is rolled back by {@link #cleanupUnstartedExchanges}. Only an {@link Error} between creating a sink and recording it
     * can escape that, and phase 1's caller does not recover from those either.
     */
    private void buildChildContext(
        SubPlan child,
        String childPath,
        ExchangeSourceHandler parentSource,
        MergeContext parent,
        List<Page> collectedPages,
        List<MergeContext> ancestors
    ) {
        String childSessionId = nodeSessionId(childPath);
        var childSink = new ParentSink(childSessionId, parentSource);
        unstartedParentSinks.add(childSink);
        if (child instanceof SubPlan.Merge merge) {
            parent.children.add(buildSubPlanContext(merge, childPath, childSink, collectedPages, ancestors));
        } else {
            // ancestors is the ancestor chain from root down to parent (inclusive); it is immutable-copied into the leaf so
            // ensureAncestorsStarted can start them top-down at dispatch time.
            parent.children.add(new LeafContext(child.plan(), childPath, childSink, List.copyOf(ancestors)));
        }
    }

    /**
     * Rolls back the exchange registrations that a failed {@link #buildSubPlanContext} left behind, using the two ledgers that recorded
     * them. Called only from {@link #execute}'s phase 1 catch block, so no merge segment has started, no leaf has been dispatched, and no
     * driver exists yet. Because every sink is lazy, no {@link ExchangeSinkHandler} has been created either and no fetcher is running, so
     * the only state to undo is the registered sources and the keep-alive refs their children hold. That is what keeps this method a
     * straight-line walk rather than a coordinated shutdown.
     * <p>
     * Each source is torn down with the same pair as {@link ExchangeService#finishSessionEarly} - deregister, then
     * {@link ExchangeSourceHandler#finishEarly} - and {@code drainingPages} is {@code true} here because a query that failed to
     * build has no results to hand back, so buffered pages should be discarded rather than kept for a reader that will never come.
     * Async stop passes {@code false} for the opposite reason. This is the same teardown that {@link #mergeTerminalListener} and
     * {@link MergeContext#abort} perform for a single node once phase 1 has succeeded.
     * <p>
     * The two loops are not redundant, because {@code finishEarly} reaches only the read side and never touches {@link ExchangeService}'s
     * registry or the children's keep-alive refs. It also cannot see any child here, since a child whose {@link ParentSink#attach} never
     * ran was never registered as a remote sink at all. Releasing those refs is what the second loop is for, and for a child that never
     * attached that is all {@link ParentSink#finish} has to do.
     * <p>
     * Each ledger is walked with {@code reversed()}, so the innermost node goes first and a nested node is undone before the parent it
     * feeds, mirroring the order they were built in. Sources are still closed before sinks are failed, matching the order the post-phase-1
     * teardown paths use.
     *
     * @param failure the exception that caused the build to fail, propagated to each parent sink
     */
    private void cleanupUnstartedExchanges(Exception failure) {
        for (MergeContext mergeContext : allMergeContexts.reversed()) {
            mergeContext.removeExchangeSource();
            mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
        }
        for (ParentSink parentSink : unstartedParentSinks.reversed()) {
            parentSink.finish(failure);
        }
    }

    /**
     * Allocates {@link ComputeListener} ref trees for the entire merge hierarchy without starting any drivers. Each merge node's
     * {@link MergeContext#segmentListener} and {@link MergeContext#childListeners} are stored for later consumption by
     * {@link #executeMerge} when the first leaf under that node is dispatched. Leaf children are enrolled in {@link #scheduledLeaves}
     * paired with their pre-acquired {@code childListener} ref.
     */
    private void allocateComputeRefs(
        MergeContext mergeContext,
        PlanTimeProfile planTimeProfile,
        ActionListener<DriverCompletionInfo> completionListener,
        Consumer<Exception> cancelOnFailure
    ) {
        // The root segment is this query's coordinator, so it uses the caller's PlanTimeProfile which holds query-level optimization time.
        // Nested merges each get a fresh profile so PROFILE output shows per-segment timing only.
        mergeContext.segmentPlanTimeProfile = mergeContext.path == null
            ? planTimeProfile
            : (planTimeProfile != null ? new PlanTimeProfile() : null);
        final ActionListener<DriverCompletionInfo> terminalListener = mergeTerminalListener(mergeContext, completionListener);
        try (var computeListener = new ComputeListener(() -> {
            // ComputeListener's hook is a Runnable and does not forward the cause, so one has to be synthesised here. Prefer the task's
            // own cancellation exception, which carries the reason the task was cancelled - "keep_alive expired" for an expired async
            // query, say - because a bare synthetic would replace that reason with a useless one in the error the user finally sees.
            //
            // The substitution is possible because ComputeListener runs this hook *before* it collects the real failure, and the drain
            // it triggers propagates whatever it is given up through an unstarted merge's terminal listener - so the synthetic reaches
            // the collector first. FailureCollector buckets by category and returns the head of the preferred queue, and two
            // TaskCancelledExceptions share the CANCELLATION bucket, so first in wins. A non-cancellation failure is unaffected either
            // way: the task is not cancelled, and the real exception outranks the CANCELLATION bucket on category alone.
            cancelOnFailure.accept(
                rootTask.isCancelled() ? rootTask.getTaskCancelledException() : new TaskCancelledException("query cancelled due to failure")
            );
        }, terminalListener)) {
            final ActionListener<Void> guard = ActionListener.notifyOnce(computeListener.acquireAvoid());
            ActionListener<DriverCompletionInfo> segmentListener = null;
            final List<ActionListener<DriverCompletionInfo>> childListeners = new ArrayList<>(mergeContext.children.size());
            try {
                segmentListener = ActionListener.notifyOnce(computeListener.acquireCompute());
                // LIMIT and STOP finishEarly the root source, which completes this runCompute. Set noMoreLeaves and drain
                // unstarted merges only after leaf dispatch has begun: executeLeaf checks the flag before parentSink.finished,
                // and a success-skip on a failing query would settle remaining leaves as success. A synchronous stub
                // completion during executeMerge(root) is not LIMIT — nested sources stay registered until
                // ensureAncestorsStarted (or a later LIMIT/STOP drain) starts those merges.
                if (mergeContext.path == null) {
                    var inner = segmentListener;
                    segmentListener = ActionListener.wrap(info -> {
                        if (leafDispatchStarted.get()) {
                            noMoreLeaves.set(true);
                            drainUnstartedMergesOnce();
                        }
                        inner.onResponse(info);
                    }, inner::onFailure);
                }
                mergeContext.segmentListener = segmentListener;
                // allocate the child listeners before any recursive call
                for (int i = 0; i < mergeContext.children.size(); i++) {
                    childListeners.add(ActionListener.notifyOnce(computeListener.acquireCompute()));
                }
                mergeContext.childListeners = childListeners;
                // recursively process the children, which may be leaves or nested merges
                for (int i = 0; i < mergeContext.children.size(); i++) {
                    mergeContext.children.get(i).allocateComputeRefs(childListeners.get(i), planTimeProfile, cancelOnFailure);
                }
                guard.onResponse(null);
            } catch (Exception e) {
                LOGGER.debug("synchronous failure building ref tree for merge segment [{}]", mergeLabel(mergeContext), e);
                try {
                    if (segmentListener != null) {
                        segmentListener.onFailure(e);
                    }
                    // childListeners are pre-allocated before any recursive call. If a recursive allocateComputeRefs threw,
                    // the child's outer backstop already fired its terminalListener — which is the corresponding childListeners[i] —
                    // so notifyOnce makes that entry a no-op. Remaining entries are pending and must be settled now.
                    childListeners.forEach(l -> l.onFailure(e));
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                } finally {
                    guard.onFailure(e);
                }
            }
        } catch (Exception e) {
            LOGGER.debug("failure initialising ComputeListener for merge segment [{}]", mergeLabel(mergeContext), e);
            try {
                cancelOnFailure.accept(e);
            } catch (Exception cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            } finally {
                terminalListener.onFailure(e);
            }
        }
    }

    /**
     * Submits the coordinator drivers for {@code mergeContext} to the compute executor. Called lazily by {@link #ensureAncestorsStarted}
     * the first time a leaf under this merge is dispatched.
     * <p>
     * {@link MergeContext#segmentListener} was pre-acquired during {@link #allocateComputeRefs} and is consumed here by handing it to
     * {@code ComputeService.runCompute}. Once this method returns, {@code runCompute} owns the listener and will complete it
     * asynchronously.
     * <p>
     * This is also where the segment's {@link ExchangeSinkHandler} is registered, via {@link ParentSink#attach}. Registering it here
     * rather than in phase 1 is what keeps an unstarted nested merge invisible to {@code InactiveSinksReaper}: a handler with no attached
     * sink and an empty buffer reports {@code hasData() == false} and is reaped once it exceeds
     * {@code esql.exchange.sink_inactive_interval}, which a merge waiting behind {@code branchParallelDegree} can easily do.
     */
    private void executeMerge(MergeContext mergeContext) {
        LOGGER.debug("starting merge segment [{}] with [{}] children", mergeLabel(mergeContext), mergeContext.children.size());
        // The root writes into collectedPages through its OutputExec and has no parent sink; every other segment registers its
        // handler now and swaps its phase-1 keep-alive ref on the parent source for the addRemoteSink ref that attach() takes.
        // attach() returning null means finish() won the race; do not pass a null supplier into runCompute for a nested merge.
        final Supplier<ExchangeSink> sinkSupplier;
        if (mergeContext.parentSink == null) {
            sinkSupplier = null;
        } else {
            sinkSupplier = mergeContext.parentSink.attach();
            if (sinkSupplier == null) {
                throw new IllegalStateException("sink [" + mergeContext.parentSink.sessionId() + "] already finished");
            }
        }
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
                sinkSupplier,
                false,
                false
            ),
            mergeContext.plan,
            computeService.plannerSettings().get(),
            LocalPhysicalOptimization.ENABLED,
            mergeContext.segmentPlanTimeProfile,
            mergeContext.segmentListener
        );
    }

    /**
     * Starts a merge segment if it has not yet been started, and handles synchronous start failures.
     *
     * @return {@code true} if this caller won the {@link MergeContext#started} CAS and invoked {@link #executeMerge};
     *         {@code false} if another caller already claimed the node — a successful start or an abort. For
     *         {@link #ensureAncestorsStarted} that is "continue". For Phase 2a the only prior claimant is abort, so
     *         {@link #execute} must stop rather than dispatch leaves against aborted sinks.
     */
    private boolean startMerge(MergeContext mergeContext) {
        if (mergeContext.started.compareAndSet(false, true) == false) {
            return false;
        }
        try {
            executeMerge(mergeContext);
        } catch (Exception e) {
            handleAncestorStartFailure(mergeContext, e);
            throw e;
        }
        return true;
    }

    /**
     * Ensures that every ancestor of {@code leaf} — from root down to direct parent, in that order — has its merge drivers running before
     * the leaf itself is dispatched.
     * <p>
     * The {@link MergeContext#started} CAS guarantees that exactly one caller starts each ancestor: whoever wins the CAS calls
     * {@link #executeMerge}; any concurrent caller (a sibling leaf dispatched at the same time under {@code branchParallelDegree}) simply
     * skips past the already-started ancestor. Because {@code runCompute} is non-blocking and the exchange is buffered, the leaf may begin
     * writing before the merge driver has actually "run"
     * <p>
     * A synchronous throw from {@link #executeMerge} (before {@code runCompute} has absorbed the segment listener) is handled by
     * {@link #handleAncestorStartFailure} and then re-thrown. The caller ({@link #executeLeaf}) routes the exception to
     * {@link #settleLeafAndRefill}.
     */
    private void ensureAncestorsStarted(LeafContext leaf) {
        for (MergeContext ancestor : leaf.ancestors) {
            startMerge(ancestor);
        }
    }

    /**
     * Settles the pre-allocated refs for a merge that failed to start via {@link #executeMerge}, then aborts the subtree below it. Must be
     * called before the exception is re-thrown, so that the child sinks are marked finished before any concurrent worker reaches them.
     * <p>
     * Ordering: settle refs first, then abort children. Settling first ensures that when aborting child sinks marks leaf
     * {@link ParentSink}s finished (causing {@link #executeLeaf} to skip those leaves without completing their listeners), the
     * corresponding {@code childListeners} have already been completed here, so the parent's {@link ComputeListener} is not left short a
     * ref.
     */
    private void handleAncestorStartFailure(MergeContext mergeContext, Exception failure) {
        LOGGER.debug("synchronous failure starting merge segment [{}]", mergeLabel(mergeContext), failure);
        try {
            // notifyOnce makes this a no-op if runCompute already absorbed the listener before throwing.
            if (mergeContext.segmentListener != null) {
                mergeContext.segmentListener.onFailure(failure);
            }
            // Settle all child refs (leaves and nested merges). This must happen before aborting child sinks, because that
            // marks leaf sinks finished — after which executeLeaf skips those leaves without completing their listeners — so if
            // the childListeners were left open those refs would never be released.
            if (mergeContext.childListeners != null) {
                mergeContext.childListeners.forEach(l -> l.onFailure(failure));
            }
            for (SubPlanContext child : mergeContext.children) {
                child.abort(failure);
            }
        } catch (Exception cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    /**
     * Starts every {@link MergeContext} that {@link #ensureAncestorsStarted} has not yet reached, at most once per query, so that no
     * {@link ComputeListener} ref-count chain hangs waiting for a terminal listener that will never fire and no exchange infrastructure
     * is left registered.
     * <p>
     * Both callers reach this on a stop and either can be first, so neither may rely on the other having run: the root merge's
     * {@code segmentListener} success wrapper, when {@code leafDispatchStarted} is already true (LIMIT or STOP finished the root after
     * producers were running), and {@link #checkCancelledOrStopped}, when a queued leaf is skipped because the session was already
     * stopped. Only the second fires for a STOP that landed before {@link #execute} ran, which leaves {@code EsqlExecutionInfo#isStopped}
     * set with no coordinator completion to follow. A synchronous completion during {@link #executeMerge} of the root is not that signal:
     * nested sources stay registered until {@link #ensureAncestorsStarted} starts those merges, or until a later LIMIT/STOP drain.
     * <p>
     * A merge that {@link #ensureAncestorsStarted} never reached still holds a registered {@link ExchangeSourceHandler} of its own, plus
     * the {@link ExchangeSourceHandler#addEmptySink} keep-alive ref its {@link ParentSink} took on the parent's source in
     * {@link #buildChildContext}. Running the segment is what releases both: {@link #startMerge} consumes the {@code segmentListener} and
     * records the segment profile, and the {@link #mergeTerminalListener} that then fires deregisters the source and finishes the parent
     * sink. This method therefore calls {@link #startMerge}, exactly as {@link #ensureAncestorsStarted} would. If that merge's
     * {@code segmentListener} is later also called from a concurrent or subsequent {@link #ensureAncestorsStarted} (e.g. the same leaf
     * that triggered the root to complete is still mid-way through its ancestor chain), {@link ActionListener#notifyOnce} makes the
     * second call a no-op. {@code childListeners} are not settled here; they drain naturally as the still-queued skipped leaves flow
     * through {@link #finishLeaf}.
     * <p>
     * See {@link #abortUnstartedMerges} for why both walks are bottom-up, and why running either after the other is safe.
     */
    private void drainUnstartedMergesOnce() {
        if (unstartedMergesDrained.compareAndSet(false, true)) {
            // startMerge rethrows after handleAncestorStartFailure; swallowing keeps the root success wrapper able to
            // complete the segment listener. An escape from here would hang that ref.
            for (MergeContext m : allMergeContexts.reversed()) {
                try {
                    startMerge(m);
                } catch (Exception e) {
                    // handleAncestorStartFailure already ran
                }
            }
        }
    }

    /**
     * Aborts every {@link MergeContext} that has not yet been started, settling the refs they hold so that a query cut short by failure
     * fails rather than hangs. Called from the {@code cancelOnFailure} consumer in {@link #execute}; that consumer's compare-and-set is
     * what keeps this to one call per query, so there is no guard here.
     * <p>
     * {@code failure} is the first exception that consumer stored, so a {@code notifyOnce} terminal which only ever sees this abort still
     * reports a real cause rather than a bare synthetic {@link TaskCancelledException}.
     * <p>
     * Each unstarted merge is handed to {@link MergeContext#abort}, which settles {@code segmentListener} and {@code childListeners}
     * <em>and</em> finishes child leaf sinks. Settling refs alone is not enough: this runs before {@code cancelQueryOnFailure}, so a
     * refill can still pass {@code notifyIfCancelled} and {@link #sessionAlreadyStopped}; {@link #executeLeaf} then skips only when
     * {@link ParentSink#finished} is set. Without that, the refill would {@code attach()} to a source already {@code finishEarly}'d by
     * the merge's terminal listener.
     * <p>
     * {@link #allMergeContexts} is walked in reverse (bottom-up / post-order) so that child merges are handled before their parents: the
     * child's terminal listener completes the parent's {@code childListener[i]}, and the subsequent parent abort then finds it already
     * settled ({@link ActionListener#notifyOnce} makes it a no-op). {@link #drainUnstartedMergesOnce} walks the same way for consistency,
     * though the ordering matters less there. The {@link MergeContext#started} CAS - claimed by {@link MergeContext#abort} here and by
     * {@link #startMerge} there - makes the two walks idempotent and mutually safe in either order: an already-started merge is skipped,
     * and aborting a merge that is already running is a no-op. Root is usually started in Phase 2a, but an {@link #allocateComputeRefs}
     * failure can abort it before that; the walk is what reaches an unstarted sibling.
     */
    private void abortUnstartedMerges(Exception failure) {
        // Do not CAS started here - MergeContext.abort owns that CAS; winning it first would make abort a no-op and leave leaf sinks open.
        for (MergeContext m : allMergeContexts.reversed()) {
            try {
                m.abort(failure);
            } catch (Exception e) {
                failure.addSuppressed(e);
            }
        }
    }

    /**
     * Builds the {@link ComputeListener} terminal listener for a merge segment. When the listener fires (either success or failure),
     * it deregisters the merge's exchange source and signals the merge's parent sink, then forwards to {@code completionListener}.
     * <p>
     * On success: {@link MergeContext#removeExchangeSource} deregisters the source; finishing the parent sink signals it that all
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
            mergeContext.removeExchangeSource();
            if (mergeContext.parentSink != null) {
                mergeContext.parentSink.finish(null);
            }
            completionListener.onResponse(completionInfo);
        }, e -> {
            mergeContext.removeExchangeSource();
            mergeContext.exchangeSource.finishEarly(true, ActionListener.noop());
            if (mergeContext.parentSink != null) {
                mergeContext.parentSink.finish(e);
            }
            completionListener.onFailure(e);
        }));
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
        executeLeaf(scheduledLeaves.get(index));
    }

    /**
     * True once async STOP has marked the query stopped, or the root merge's {@code runCompute} has succeeded
     * after leaf dispatch started (LIMIT or STOP {@code finishEarly}'d the coordinator source). Either signal
     * means queued leaves must not start {@code executePlan}.
     */
    private boolean sessionAlreadyStopped() {
        return execInfo.isStopped() || noMoreLeaves.get();
    }

    /**
     * Dispatches a single leaf to {@link ComputeService#executePlan}. If the root task has already been cancelled, the session
     * has already been stopped ({@code EsqlExecutionInfo#isStopped} or the root merge succeeded after LIMIT/STOP), or the leaf's
     * merge segment already aborted it during phase 2, skips dispatch. On completion (success or failure), the next leaf is
     * claimed and dispatched asynchronously.
     * <p>
     * The leaf's exchange sink is created here, via {@link ParentSink#attach}, not in phase 1: an idle registered sink handler
     * would be reaped by the exchange service's inactive-sink reaper while the leaf waits behind {@code branchParallelDegree}.
     * Cancel, STOP/LIMIT, and abort are checked again after {@link #ensureAncestorsStarted}: that method's {@code started} CAS
     * is the same one abort claims, so a failed CAS is not a signal that it is safe to {@code attach()}. If {@code attach()}
     * still loses the race with {@link ParentSink#finish}, it returns null and this method skips like an already-finished sink
     * — refill only; abort already settled the leaf listener.
     * <p>
     * A synchronous throw from {@code executePlan} is routed to {@link #finishLeaf}: refill dispatches run on
     * the search executor, where an escaping exception would be swallowed and the leaf's listener — a {@code ComputeListener}
     * ref — would never complete, hanging the query. {@code finishLeaf} is safe to call from the catch even if {@code executePlan}
     * already notified its listener before throwing: the leaf listener is notifyOnce-wrapped and {@link ParentSink#finish} is
     * CAS-guarded. The refill itself is wrapped in a {@link RunOnce} so the slot cannot be refilled twice by one leaf.
     * <p>
     * Symmetrically, a throw out of {@code finishLeaf} itself must not cost the slot its refill, which is why every outcome here goes
     * through {@link #settleLeafAndRefill} rather than calling {@code finishLeaf} directly.
     *
     * @param scheduledLeaf the leaf to dispatch, containing its plan and parent sink
     */
    private void executeLeaf(ScheduledLeaf scheduledLeaf) {
        LeafContext leafContext = scheduledLeaf.leafContext;
        ParentSink parentSink = leafContext.parentSink;
        LOGGER.debug("dispatching leaf [{}]", leafContext.path);
        Runnable onDoneOnce = new RunOnce(() -> submitOnDone(this::tryExecuteNextLeaf));
        // All paths (cancellation, skip, success, failure) use submitOnDone rather than calling onDone inline. If executePlan or
        // notifyIfCancelled completes before returning, the listener fires on the current thread, and a direct refill would recurse
        // through the entire remaining queue (tryExecuteNextLeaf → executeLeaf → … → tryExecuteNextLeaf() → …), overflowing the
        // stack when many leaves are queued.
        //
        // Every path settles the leaf through settleLeafAndRefill, which refills from a finally - see that method for why.
        if (checkCancelledOrStopped(scheduledLeaf, onDoneOnce)) {
            return;
        }
        try {
            // Start any ancestor merge segments that have not yet been started (top-down, CAS-guarded). This must happen before attach()
            // so that the exchange source is consuming before the sink starts writing.
            ensureAncestorsStarted(leafContext);
            // started is shared by start and abort. A failed CAS here means another thread claimed the node, not that drivers are running.
            // Abort can win that CAS, finishEarly the source, and finish this sink while this leaf was already past the checks above.
            // Replay the same three skips before attach().
            if (checkCancelledOrStopped(scheduledLeaf, onDoneOnce)) {
                return;
            }
            Supplier<ExchangeSink> exchangeSinkSupplier = parentSink.attach();
            if (exchangeSinkSupplier == null) {
                // finish() won after the skip checks. Abort already settled this leaf's childListener.
                onDoneOnce.run();
                return;
            }
            ActionListener<Result> listener = ActionListener.wrap(
                result -> settleLeafAndRefill(scheduledLeaf, result.completionInfo(), null, onDoneOnce),
                e -> settleLeafAndRefill(scheduledLeaf, null, e, onDoneOnce)
            );
            computeService.executePlan(
                parentSink.sessionId(),
                rootTask,
                flags,
                leafContext.plan,
                configuration,
                foldContext,
                execInfo,
                leafContext.path,
                listener,
                exchangeSinkSupplier,
                initialClusterStatuses,
                configuration.profile() ? new PlanTimeProfile() : null,
                warnIndexCoordinatorOnce == null ? () -> {} : warnIndexCoordinatorOnce
            );
        } catch (Exception e) {
            settleLeafAndRefill(scheduledLeaf, null, e, onDoneOnce);
        }
    }

    /**
     * Checks if the query has been cancelled, the session is already stopped, or the leaf's parent sink is finished. If any of these are
     * true, performs the appropriate leaf settlement and returns true.
     */
    private boolean checkCancelledOrStopped(ScheduledLeaf scheduledLeaf, Runnable onDoneOnce) {
        if (rootTask.notifyIfCancelled(ActionListener.wrap(ignored -> {}, e -> settleLeafAndRefill(scheduledLeaf, null, e, onDoneOnce)))) {
            return true;
        }
        if (sessionAlreadyStopped()) {
            // Async STOP and LIMIT complete the root runCompute without cancelling rootTask. Queued leaves must not call executePlan: a
            // failure there would fail a query that should return partial results.
            //
            // Releasing unstarted merges is this branch's job, not something it can leave to the root. Skipping here means the leaf
            // never reaches ensureAncestorsStarted, and the only other caller of the success drain is the root segment listener - which
            // cannot fire, because every unstarted merge still holds the keep-alive ref its ParentSink took on the root's source in
            // buildChildContext, so the root coordinator parks on that source forever. Draining before settling the leaf keeps this
            // leaf's own ref held for the duration of the walk, so no merge source can reach EOF part-way through it.
            //
            // The cancelled branch above needs no equivalent: it settles the leaf as a failure, and ComputeListener runs its
            // runOnFailure hook eagerly, which reaches cancelOnFailure and drains through the abort path instead.
            drainUnstartedMergesOnce();
            settleLeafAndRefill(scheduledLeaf, DriverCompletionInfo.EMPTY, null, onDoneOnce);
            return true;
        }
        if (scheduledLeaf.leafContext.parentSink.isFinished()) {
            // The leaf's merge segment failed and aborted this sink. Skip dispatch and just refill the slot: abort settles every
            // childListener — this leaf's included — before finishing the sink. allocateComputeRefs failures reach this only through
            // cancelOnFailure → drain → abort, which does the same settle-then-finish ordering.
            onDoneOnce.run();
            return true;
        }
        return false;
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
     * notified before throwing. Double settling is safe here - the leaf listener is notifyOnce-wrapped, {@link ParentSink#finish} is
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
     * The refill is ordinary search-pool work: it is not force-executed, so a full SEARCH queue rejects it like any other
     * search task. The refill <em>is</em> the dispatch loop's continuation, so dropping it would strand every leaf still queued behind
     * {@code branchParallelDegree} with its {@link ComputeListener} ref unreleased. {@link #failRemainingLeaves} is therefore the
     * rejection path — queue full, executor shutdown, or a plain executor throwing from {@code execute()} — and the query fails
     * instead of hanging.
     */
    private void submitOnDone(Runnable onDone) {
        var refill = new AbstractRunnable() {
            @Override
            protected void doRun() {
                onDone.run();
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
            // EsThreadPoolExecutor routes rejection to onRejection above; this covers plain executors that throw synchronously from
            // execute(). failRemainingLeaves claims leaves exclusively, so double entry is harmless.
            failRemainingLeaves(e);
        }
    }

    /**
     * Atomically claims every undispatched leaf and reports {@code cause} to its listener, allowing the {@link ComputeListener} to reach
     * zero and the terminal listener to fire. Called when the slot's self-refilling dispatch chain would otherwise be permanently broken:
     * the search executor is shutting down, a non-standard executor threw, or the phase-2b dispatch of the initial wave threw partway
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
     * Signals the leaf's parent sink and then forwards the result to the leaf's {@link ScheduledLeaf#listener}. Always called after
     * {@link ComputeService#executePlan} completes, whether successfully or not.
     *
     * @param scheduledLeaf  the leaf that just finished
     * @param completionInfo profiling data from the leaf's drivers; {@code null} on failure
     * @param failure        the exception if the leaf failed; {@code null} on success
     */
    private void finishLeaf(ScheduledLeaf scheduledLeaf, DriverCompletionInfo completionInfo, Exception failure) {
        scheduledLeaf.leafContext.parentSink.finish(failure);
        if (failure == null) {
            scheduledLeaf.listener.onResponse(completionInfo);
        } else {
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

    /** Log / debug name for a merge node: {@code "main"} for the root ({@code path == null}), otherwise the path. */
    private static String mergeLabel(MergeContext mergeContext) {
        return mergeContext.path == null ? "main" : mergeContext.path;
    }

    /**
     * Common base for the two kinds of execution node that {@link #buildSubPlanContext} produces. Every node has a physical plan to
     * execute, a path that identifies it within the query (used for session IDs and profiling), and a {@link ParentSink} that the node
     * writes its output into ({@code null} for the root merge, which writes directly into {@code collectedPages}).
     */
    private abstract sealed class SubPlanContext permits LeafContext, MergeContext {
        final PhysicalPlan plan;
        final String path;
        final ParentSink parentSink;

        private SubPlanContext(PhysicalPlan plan, String path, ParentSink parentSink) {
            this.plan = plan;
            this.path = path;
            this.parentSink = parentSink;
        }

        /** Aborts the node and releases any booked resources. */
        abstract void abort(Exception failure);

        /** Allocates ComputeListener reference counters for the node. */
        abstract void allocateComputeRefs(
            ActionListener<DriverCompletionInfo> childListener,
            PlanTimeProfile planTimeProfile,
            Consumer<Exception> cancelOnFailure
        );
    }

    /**
     * An immutable descriptor for a leaf producer plan. Carries what {@link #executeLeaf} needs to call
     * {@code ComputeService.executePlan}: the physical plan, the path, the lazy {@link ParentSink} the leaf writes into, and the
     * {@link #ancestors} chain that {@link #ensureAncestorsStarted} walks before dispatch.
     */
    private final class LeafContext extends SubPlanContext {
        /**
         * Ancestor merge segments from root (index 0) down to direct parent. Built during {@link #buildChildContext} and immutable
         * thereafter. {@link #ensureAncestorsStarted} iterates this list top-down so that ancestor merge drivers are started in
         * consumer-before-producer order.
         */
        private final List<MergeContext> ancestors;

        private LeafContext(PhysicalPlan plan, String path, ParentSink parentSink, List<MergeContext> ancestors) {
            super(plan, path, parentSink);
            this.ancestors = ancestors;
        }

        @Override
        void abort(Exception failure) {
            parentSink.finish(failure);
        }

        @Override
        void allocateComputeRefs(
            ActionListener<DriverCompletionInfo> childListener,
            PlanTimeProfile planTimeProfile,
            Consumer<Exception> cancelOnFailure
        ) {
            scheduledLeaves.add(new ScheduledLeaf(this, childListener));
        }
    }

    /**
     * The stateful execution node for a coordinator merge segment. Built by {@link #buildSubPlanContext} and started lazily by
     * {@link #ensureAncestorsStarted} when the first leaf under it is dispatched. Holds the wired exchange infrastructure, mutable
     * lifecycle guards, and pre-allocated {@link ComputeListener} refs:
     * <ul>
     *   <li>{@code exchangeId} — the key {@code exchangeSource} is registered under in {@link ExchangeService}. The root uses the
     *       bare {@code sessionId}, because that is the key {@link ExchangeService#finishSessionEarly} looks up to stop a query
     *       early; nested segments use {@code <sessionPrefix>/<path>/merge}.</li>
     *   <li>{@code computeSessionId} — the session this segment's compute runs under, passed to {@code ComputeService.runCompute}.
     *       It differs from {@code exchangeId} only for the root, whose exchange has to keep the bare {@code sessionId} while its
     *       compute uses the per-executor prefix so that repeated rounds of one query do not collide.</li>
     *   <li>{@code exchangeSource} — the {@link ExchangeSourceHandler} this segment reads from; children write into it via their
     *       {@link ParentSink}s.</li>
     *   <li>{@code children} — direct children, each either a {@link MergeContext} (another coordinator segment) or a {@link LeafContext}
     *       (a producer to dispatch); iterated by the abort paths.</li>
     *   <li>{@code started} — arbitrates between starting and aborting this node: {@link #ensureAncestorsStarted} sets it via CAS,
     *       and {@code abort} declines to tear down a node that is already set, leaving that to the node's own terminal listener. The CAS
     *       therefore has to run unconditionally rather than inside an assertion, or the abort path would tear down running segments in
     *       builds with assertions disabled.</li>
     *   <li>{@code sourceRemoved} — atomic boolean guard: {@code compareAndSet(false, true)} ensures {@code removeExchangeSource} runs
     *       at most once per node.</li>
     * </ul>
     */
    private final class MergeContext extends SubPlanContext {
        private final String exchangeId;
        private final String computeSessionId;
        private final ExchangeSourceHandler exchangeSource;
        private final List<SubPlanContext> children;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean sourceRemoved = new AtomicBoolean();
        /**
         * Pre-acquired {@link ComputeListener} ref for this merge's own drivers; stored during {@link #allocateComputeRefs} and handed to
         * {@link #executeMerge} the first time a leaf under this merge is dispatched. {@code volatile} so the store in
         * {@link #allocateComputeRefs} is visible to the thread that wins the {@link #started} CAS in {@link #ensureAncestorsStarted}.
         */
        private volatile ActionListener<DriverCompletionInfo> segmentListener;
        /**
         * Pre-acquired {@link ComputeListener} refs, one per entry in {@link #children}, in the same order; stored during
         * {@link #allocateComputeRefs}. Used by {@link #handleAncestorStartFailure}, {@code abort}, and {@link #abortUnstartedMerges} to
         * settle the count when this merge never runs.
         */
        private volatile List<ActionListener<DriverCompletionInfo>> childListeners;
        /** Per-segment {@link PlanTimeProfile} for PROFILE output; stored during {@link #allocateComputeRefs}. */
        private PlanTimeProfile segmentPlanTimeProfile;

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

        /** Registers this merge's exchange source in the exchange service. */
        void registerExchangeSource() {
            exchangeService.addExchangeSourceHandler(exchangeId, exchangeSource);
        }

        /** Deregisters this merge's exchange source from the exchange service. */
        void removeExchangeSource() {
            if (sourceRemoved.compareAndSet(false, true)) {
                exchangeService.removeExchangeSourceHandler(exchangeId);
            }
        }

        @Override
        void abort(Exception failure) {
            if (started.compareAndSet(false, true) == false) {
                return;
            }
            LOGGER.debug("aborting unstarted merge subtree [{}]", path);
            // Complete pre-allocated refs (stored during allocateComputeRefs) before aborting children, this must happen first. Aborting
            // children marks leaf sinks finished, after which executeLeaf skips those leaves without completing their listeners — so any
            // childListener left open here would never be released, hanging the query. NotifyOnce makes these idempotent with concurrent
            // completions from abortUnstartedMerges or terminalListener.
            if (segmentListener != null) {
                segmentListener.onFailure(failure);
            }
            if (childListeners != null) {
                childListeners.forEach(l -> l.onFailure(failure));
            }
            // Recursively abort children polymorphically
            for (SubPlanContext child : children) {
                child.abort(failure);
            }
            // removeExchangeSource and ParentSink.finish may be no-ops if the terminalListener already fired (triggered by the ref drain
            // above). The CAS guards in both methods make that safe.
            removeExchangeSource();
            exchangeSource.finishEarly(true, ActionListener.noop());
            if (parentSink != null) {
                parentSink.finish(failure);
            }
        }

        @Override
        void allocateComputeRefs(
            ActionListener<DriverCompletionInfo> childListener,
            PlanTimeProfile planTimeProfile,
            Consumer<Exception> cancelOnFailure
        ) {
            SubPlansExecutor.this.allocateComputeRefs(this, planTimeProfile, childListener, cancelOnFailure);
        }
    }

    /**
     * The producer-side of an exchange: the {@link ExchangeSinkHandler} that a child node (leaf or nested merge) writes its output
     * into, together with the session ID under which it is registered in {@link ExchangeService}. Created in
     * {@link #buildChildContext} for every child(leaf/branch or nested merge) of a merge node and stored in the child's
     * {@link SubPlanContext#parentSink}.
     * <p>
     * All sinks are <b>lazy</b>: nothing is registered in {@link #buildChildContext}. The sink holds the parent source open with
     * {@code pendingRef}, an {@link ExchangeSourceHandler#addEmptySink} ref, and creates the handler in {@link #attach} only when the
     * child is actually started - {@link #executeLeaf} for a leaf, {@link #executeMerge} for a nested merge.
     * <p>
     * Laziness is required, not an optimization. An idle registered handler (no attached sink, empty buffer) reports
     * {@code hasData() == false} and is reaped by {@code InactiveSinksReaper} once it exceeds
     * {@code esql.exchange.sink_inactive_interval}, silently failing that branch. Both kinds of child can idle for longer than that
     * interval: a leaf waits in {@link #scheduledLeaves} behind {@code branchParallelDegree}, and since merge segments became
     * lazily started a nested merge waits for the first leaf beneath it to be dispatched.
     * <p>
     * {@link #finish} uses the {@code finished} guard to act exactly once, regardless of whether the child completed
     * successfully, failed, or was aborted. It always releases {@code pendingRef}. If a handler exists it is then deregistered: on
     * success after waiting for it to drain (via {@link ExchangeSinkHandler#addCompletionListener}), on failure immediately, so the
     * parent's source sees the error rather than waiting for pages that will never arrive. A sink whose child was never started has
     * no handler, so releasing the ref is all there is to undo.
     * <p>
     * {@link #attach} and {@link #finish} publish {@code finished} and {@code handler} under the same lock so a finish that wins
     * before the handler is created cannot leave a later {@code attach} registered with nobody to deregister it. In that case
     * {@code attach} returns null and does not call {@code createSinkHandler}.
     * <p>
     * That is the whole of what the lock buys, and it is worth being precise about the limit, because the wider reading - that the
     * attach and the finish are serialised against each other outright - is the natural one and is wrong. {@link #attach}
     * deliberately leaves the lock before it wires the fetcher with {@link ExchangeSourceHandler#addRemoteSink} and releases
     * {@code pendingRef}, so a {@link #finish} can land in between and deregister the handler before any fetcher exists.
     * <p>
     * Widening the lock over that second half is unnecessary, because each consequence is already absorbed a layer down:
     * <ul>
     *   <li>the fetcher fails against the failed handler and completes through {@code RemoteSinkFetcher.onSinkFailed}, which
     *       releases the keep-alive ref {@code addRemoteSink} took on the parent source, so the refs still balance;</li>
     *   <li>releasing {@code pendingRef} first can take the parent source's outstanding-sink count to zero before
     *       {@code addRemoteSink} raises it again, but re-completing an already-complete {@link ExchangeSourceHandler} tracker is a
     *       no-op, and that tracker only asserts on the decrement, never on the increment that follows it;</li>
     *   <li>pages the child writes after the teardown are dropped by {@code ExchangeBuffer.addPage}, which releases anything added
     *       once the buffer has no more inputs.</li>
     * </ul>
     */
    private final class ParentSink {
        private final String sessionId;
        private final AtomicBoolean finished = new AtomicBoolean();
        private final ExchangeSourceHandler parentSource;
        private final Releasable pendingRef;
        private volatile ExchangeSinkHandler handler; // Published by attach() when the child is started; null until then.

        private ParentSink(String sessionId, ExchangeSourceHandler parentSource) {
            this.sessionId = sessionId;
            this.parentSource = parentSource;
            // Every child (leaf or nested merge) holds the parent source open until attach() or abort.
            this.pendingRef = Releasables.releaseOnce(parentSource.addEmptySink());
        }

        /**
         * Registers this child's {@link ExchangeSinkHandler} and wires it into the parent source, immediately before the child is started.
         * <p>
         * Exactly one caller attaches a live sink. For a leaf, {@link #executeLeaf} runs on the thread that exclusively claimed it
         * from {@link #scheduledLeaves} and checks {@code parentSink.finished} both before {@link #ensureAncestorsStarted} and again
         * after it: that method's {@link MergeContext#started} CAS is the same CAS abort claims, so a failed CAS means the node was
         * claimed, not that drivers are running. Abort can still {@link #finish} this sink between that check and this call; then
         * this method returns {@code null} and does not register a handler. For a nested merge, {@link #executeMerge} is reached only
         * by whoever wins that segment's {@code started} CAS, which is the same CAS every abort path claims before it can finish
         * this sink — a {@code null} return there is unexpected and is treated as a start failure.
         *
         * @return the exchange-sink supplier to pass to {@code ComputeService.executePlan} or {@code ComputeService.runCompute},
         *         or {@code null} if {@link #finish} already ran
         */
        private Supplier<ExchangeSink> attach() {
            final ExchangeSinkHandler attached;
            synchronized (this) {
                if (finished.get()) {
                    return null;
                }
                assert handler == null : "sink [" + sessionId + "] attached twice";
                attached = exchangeService.createSinkHandler(sessionId, queryPragmas.exchangeBufferSize());
                // Publish before addRemoteSink so every later finish call sees and deregisters the handler.
                handler = attached;
            }
            parentSource.addRemoteSink(attached::fetchPageAsync, true, () -> {}, 1, ActionListener.noop());
            // addRemoteSink holds its own keep-alive ref on the parent source until the remote sink completes
            // (ExchangeSourceHandler wraps the fetcher in releaseAfter(..., addEmptySink())), so the phase-1 pending ref is no longer
            // needed. releaseOnce makes this idempotent with finish.
            pendingRef.close();
            return () -> attached.createExchangeSink(() -> {});
        }

        /**
         * Signals that the producer has finished writing to this sink. Idempotent: only the first caller performs the actual
         * teardown/signaling.
         *
         * @param failure the exception if the producer failed; null on success
         */
        private void finish(Exception failure) {
            final ExchangeSinkHandler currentHandler;
            synchronized (this) {
                if (finished.compareAndSet(false, true) == false) {
                    return;
                }
                // releaseOnce makes this idempotent with the release in attach().
                Releasables.close(pendingRef);
                currentHandler = handler;
            }
            if (currentHandler == null) {
                return;
            }
            if (failure == null) {
                currentHandler.addCompletionListener(ActionListener.running(() -> exchangeService.finishSinkHandler(sessionId, null)));
            } else {
                exchangeService.finishSinkHandler(sessionId, failure);
            }
        }

        /** Checks if this sink has been closed, completed, or failed. */
        boolean isFinished() {
            return finished.get();
        }

        /** Returns the session ID associated with this parent sink. */
        String sessionId() {
            return sessionId;
        }
    }

    /**
     * Pairs a {@link LeafContext} with the {@link ActionListener} that must be notified when the leaf finishes. Populated into
     * {@link #scheduledLeaves} during {@link #allocateComputeRefs} (setup) and consumed by {@link #tryExecuteNextLeaf} (phase 2).
     */
    private record ScheduledLeaf(LeafContext leafContext, ActionListener<DriverCompletionInfo> listener) {}

}

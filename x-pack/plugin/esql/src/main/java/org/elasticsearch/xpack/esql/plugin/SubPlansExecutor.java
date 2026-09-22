/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.LocalExchange;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.StreamingOutputExec;
import org.elasticsearch.xpack.esql.planner.SubPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.plugin.ComputeService.LOCAL_CLUSTER;

/**
 * Runs a {@link SubPlan.Merge} tree on the coordinator.
 * <p>
 * The constructor mirrors the immutable {@link SubPlan} as {@link ExecutionMerge} / {@link ExecutionLeaf} nodes. That walk is recursive
 * over nested merges but does not start drivers. Each merge opens a {@link LocalExchange}: the merge driver is the source, every child
 * (leaf or nested merge) is a sink. A dummy sink per child ({@code blocked}) is opened immediately so an unstarted branch cannot look
 * like “all producers finished.” Only the root exchange is registered on {@link ExchangeService}, so async STOP can finish it. Nested
 * exchanges stay coordinator-private.
 * <p>
 * Dispatch is a permit-gated depth-first search from the root. {@link #executeNext()} takes the current {@link #executionPermits} and
 * calls {@link ExecutionMerge#tryExecuteLeaves(int)} on the root. A merge starts itself ({@code runCompute}) the first time it is visited,
 * so the consumer is running before any child writes, then recurses into children left to right, spending remaining permits. A leaf
 * consumes one permit and calls {@code ComputeService.executePlan}, which does any cross-node work and writes the coordinator-local result
 * into the parent {@code LocalExchange}. A merge does not consume a permit, only leaves do. When a leaf completes it returns its permit
 * and {@link #executeNext()} walks from the root again, skipping nodes that already started.
 * <p>
 * Example with {@code branch_parallel_degree = 2}.
 * <pre>
 *   Merge(root)
 *   ├─ Leaf r
 *   ├─ Merge(A)
 *   │  ├─ Leaf a
 *   │  └─ Merge(A1)
 *   │     ├─ Leaf a1x
 *   │     └─ Leaf a1y
 *   └─ Merge(B)
 *      ├─ Leaf bx
 *      └─ Leaf by
 *
 *   first walk (2 permits):
 *     start root; start r; start A; start a. A1 and B stay unstarted.
 *   after r completes (1 permit):
 *     start A1; start a1x. a still running.
 *   after a completes (1 permit):
 *     start a1y.
 *   after a1x completes (1 permit):
 *     start B; start bx.
 *   after a1y completes (1 permit):
 *     start by.
 * </pre>
 * If the query is stopped or the parent exchange is already finished ({@code LIMIT} satisfied), the visit completes the node without
 * {@code runCompute} / {@code executePlan}. Failure cancels the root task; remaining nodes observe that and fail without starting new work.
 */
final class SubPlansExecutor {
    private final ComputeService computeService;
    private final String sessionPrefix;
    private final CancellableTask rootTask;
    private final EsqlFlags flags;
    private final Configuration configuration;
    private final FoldContext foldContext;
    private final EsqlExecutionInfo execInfo;
    private final Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses;
    private final Runnable warnIndexCoordinatorOnce;
    private final QueryPragmas queryPragmas;
    private final Runnable cancelOnFailure;

    /**
     * Number of leaves that may be started before another must complete. Initialized to {@code branch_parallel_degree}.
     */
    private final AtomicInteger executionPermits;
    /**
     * Serializes depth-first tree walks. Only one thread may run {@link ExecutionMerge#tryExecuteLeaves(int)} at a time.
     */
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private volatile boolean noMoreLeaves;

    private final PlanTimeProfile rootPlanTimeProfile;
    private final ExecutionMerge rootExecution;

    SubPlansExecutor(
        ComputeService computeService,
        ExchangeService exchangeService,
        String sessionId,
        CancellableTask rootTask,
        EsqlFlags flags,
        Configuration configuration,
        FoldContext foldContext,
        EsqlExecutionInfo execInfo,
        Map<String, EsqlExecutionInfo.Cluster.Status> initialClusterStatuses,
        Runnable warnIndexCoordinatorOnce,
        PlanTimeProfile planTimeProfile,
        SubPlan.Merge rootPlan,
        ActionListener<Result> listener
    ) {
        this.computeService = computeService;
        this.sessionPrefix = computeService.newChildSession(sessionId);
        this.rootTask = rootTask;
        this.flags = flags;
        this.configuration = configuration;
        this.foldContext = foldContext;
        this.execInfo = execInfo;
        this.initialClusterStatuses = initialClusterStatuses;
        this.warnIndexCoordinatorOnce = warnIndexCoordinatorOnce;
        this.queryPragmas = configuration.pragmas();
        this.executionPermits = new AtomicInteger(queryPragmas.branchParallelDegree());
        this.cancelOnFailure = new RunOnce(computeService.cancelQueryOnFailure(rootTask));
        this.rootPlanTimeProfile = planTimeProfile;
        final List<Page> collectedPages = Collections.synchronizedList(new ArrayList<>());
        final ActionListener<DriverCompletionInfo> outerListener = ActionListener.wrap(info -> {
            execInfo.markEndQuery();
            listener.onResponse(new Result(rootPlan.plan().output(), collectedPages, null, configuration, info, execInfo, null));
        }, e -> {
            Releasables.close(collectedPages);
            listener.onFailure(e);
        });
        this.rootExecution = new ExecutionMerge(
            null,
            rootPlan,
            rootPlan.plan() instanceof StreamingOutputExec ? rootPlan.plan() : new OutputExec(rootPlan.plan(), collectedPages::add),
            "main.final",
            rootPlanTimeProfile,
            null,
            ActionListener.runBefore(outerListener, () -> exchangeService.removeLocalExchange(sessionId))
        );
        exchangeService.addLocalExchange(sessionId, rootExecution.exchange);
    }

    void executePlan() {
        executeNext();
    }

    /**
     * Spends available {@link #executionPermits} on not-yet-started leaves.
     */
    private void executeNext() {
        while (noMoreLeaves == false && executionPermits.get() > 0 && scheduling.compareAndSet(false, true)) {
            try {
                final int permits = executionPermits.getAndSet(0);
                final int started = rootExecution.tryExecuteLeaves(permits);
                if (started < permits) {
                    noMoreLeaves = true;
                }
            } finally {
                scheduling.set(false);
            }
        }
    }

    private String nodeSessionId(String path) {
        return path == null ? sessionPrefix : sessionPrefix + "/" + path;
    }

    private static String childPath(String parentPath, int child) {
        String childName = "subplan-" + child;
        return parentPath == null ? childName : parentPath + "." + childName;
    }

    private boolean outputFinished(ExecutionMerge parent) {
        return execInfo.isStopped() || (parent != null && parent.exchange.isFinished());
    }

    private abstract sealed class ExecutionNode permits ExecutionLeaf, ExecutionMerge {
        final ExecutionMerge parent;
        final String path;
        final String childSessionId;
        final Releasable blocked; // empty sink, closed when this node is finished, so the parent exchange can see all producers finished
        boolean started; // guarded by scheduling, only set to true once per node

        ExecutionNode(ExecutionMerge parent, String path) {
            this.parent = parent;
            this.path = path;
            this.childSessionId = nodeSessionId(path);
            if (parent != null) {
                this.blocked = parent.exchange.exchangeSink(() -> {})::finish;
            } else {
                this.blocked = () -> {};
            }
        }

        /**
         * Starts this node if needed and spends up to {@code permits} on descendant leaves that have not started yet. Returns how many
         * leaves were started (or skipped as already finished).
         */
        abstract int tryExecuteLeaves(int permits);
    }

    /**
     * Coordinator segment. Owns the {@link LocalExchange} its children write into. Nested instances also write that merged stream into
     * the parent's exchange. Built recursively from {@link SubPlan.Merge} children in the constructor; {@link #start()} runs only on the
     * first {@link #tryExecuteLeaves(int)}.
     */
    private final class ExecutionMerge extends ExecutionNode {
        final PhysicalPlan plan;
        final String description;
        final PlanTimeProfile planTimeProfile;
        final LocalExchange exchange;
        final List<ExecutionNode> children = new ArrayList<>();
        int unstartedLeaves; // guarded by scheduling
        final ComputeListener computeListener;

        ExecutionMerge(
            ExecutionMerge parent,
            SubPlan.Merge merge,
            PhysicalPlan plan,
            String description,
            PlanTimeProfile planTimeProfile,
            String path,
            ActionListener<DriverCompletionInfo> listener
        ) {
            super(parent, path);
            this.plan = plan;
            this.description = description;
            this.planTimeProfile = planTimeProfile;
            this.exchange = new LocalExchange(queryPragmas.exchangeBufferSize());
            this.computeListener = new ComputeListener(cancelOnFailure, ActionListener.runBefore(listener, blocked::close));
            List<SubPlan> children = merge.children();
            final MergeExec.Kind mergeKind = merge.kind();
            final int childCount = children.size();
            for (int i = 0; i < childCount; i++) {
                final String childPath = childPath(path, i);
                final ActionListener<DriverCompletionInfo> childListener = computeListener.acquireCompute();
                final int siblingIndex = i;
                this.children.add(switch (children.get(i)) {
                    case SubPlan.Leaf leaf -> {
                        unstartedLeaves++;
                        yield new ExecutionLeaf(this, leaf, childPath, childListener, mergeKind, siblingIndex, childCount);
                    }
                    case SubPlan.Merge nested -> {
                        var nestedExecution = new ExecutionMerge(
                            this,
                            nested,
                            nested.plan(),
                            computeService.profileDescription(childPath, "merge"),
                            rootPlanTimeProfile != null ? new PlanTimeProfile() : null,
                            childPath,
                            childListener
                        );
                        unstartedLeaves += nestedExecution.unstartedLeaves;
                        yield nestedExecution;
                    }
                });
            }
        }

        /**
         * Depth-first: start this merge if needed, then recurse into children left to right until {@code permits} leaves have been started.
         * Nested merges are visited and started only when a leftover permit reaches them, they do not consume a permit themselves.
         */
        @Override
        int tryExecuteLeaves(int permits) {
            if (started == false) {
                start();
            }
            if (unstartedLeaves == 0) {
                return 0;
            }
            int started = 0;
            for (ExecutionNode child : children) {
                if (started < permits) {
                    started += child.tryExecuteLeaves(permits - started);
                }
            }
            unstartedLeaves -= started;
            return started;
        }

        private void start() {
            started = true;
            try (computeListener) {
                final ActionListener<DriverCompletionInfo> listener = computeListener.acquireCompute();
                if (rootTask.isCancelled()) {
                    listener.onFailure(rootTask.getTaskCancelledException());
                    return;
                }
                if (outputFinished(parent)) {
                    exchange.finish(true);
                    listener.onResponse(DriverCompletionInfo.EMPTY);
                    return;
                }
                // runCompute doesn't throw
                computeService.runCompute(
                    rootTask,
                    new ComputeContext(
                        childSessionId,
                        description,
                        LOCAL_CLUSTER,
                        flags,
                        EmptyIndexedByShardId.instance(),
                        configuration,
                        foldContext,
                        exchange::exchangeSource,
                        parent == null ? null : () -> parent.exchange.exchangeSink(() -> {}),
                        false,
                        false
                    ),
                    plan,
                    computeService.plannerSettings().get(),
                    LocalPhysicalOptimization.ENABLED,
                    planTimeProfile,
                    listener
                );
            }
        }
    }

    /**
     * Producer branch. {@link #tryExecuteLeaves(int)} consumes one permit and calls {@link ComputeService#executePlan}. On completion,
     * {@link #after()} returns that permit so another leaf can start.
     */
    private final class ExecutionLeaf extends ExecutionNode {
        final SubPlan.Leaf plan;
        final ActionListener<DriverCompletionInfo> leafListener;
        final MergeExec.Kind mergeKind;
        final int siblingIndex;
        final int siblingCount;

        ExecutionLeaf(
            ExecutionMerge parent,
            SubPlan.Leaf plan,
            String path,
            ActionListener<DriverCompletionInfo> leafListener,
            MergeExec.Kind mergeKind,
            int siblingIndex,
            int siblingCount
        ) {
            super(parent, path);
            this.plan = plan;
            this.leafListener = leafListener;
            this.mergeKind = mergeKind;
            this.siblingIndex = siblingIndex;
            this.siblingCount = siblingCount;
        }

        @Override
        int tryExecuteLeaves(int permits) {
            assert permits >= 1 : permits;
            if (started) {
                return 0;
            }
            started = true;
            startLeaf();
            return 1;
        }

        private void after() {
            executionPermits.incrementAndGet();
            try {
                blocked.close();
            } finally {
                executeNext();
            }
        }

        private void startLeaf() {
            final ActionListener<DriverCompletionInfo> resultListener = ActionListener.runAfter(leafListener, this::after);
            if (rootTask.isCancelled()) {
                resultListener.onFailure(rootTask.getTaskCancelledException());
                return;
            }
            if (outputFinished(parent)) {
                resultListener.onResponse(DriverCompletionInfo.EMPTY);
                return;
            }
            try {
                computeService.executePlan(
                    childSessionId,
                    rootTask,
                    flags,
                    plan.plan(),
                    configuration,
                    foldContext,
                    execInfo,
                    path,
                    resultListener.safeMap(Result::completionInfo),
                    () -> parent.exchange.exchangeSink(() -> {}),
                    initialClusterStatuses,
                    configuration.profile() ? new PlanTimeProfile() : null,
                    warnIndexCoordinatorOnce,
                    SiblingPlacement.forMerge(mergeKind, siblingIndex, siblingCount)
                );
            } catch (Exception e) {
                resultListener.onFailure(e);
            }
        }
    }
}

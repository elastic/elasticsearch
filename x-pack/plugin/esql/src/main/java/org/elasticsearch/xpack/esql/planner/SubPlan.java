/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Objects;

/**
 * An immutable coordinator-local execution topology built from a physical plan that may contain nested
 * {@link org.elasticsearch.xpack.esql.plan.physical.MergeExec} nodes. The tree has two node kinds:
 * <ul>
 *   <li>{@link Leaf} — a producer branch with no merge point; executed on data nodes via {@code ComputeService.executePlan}.</li>
 *   <li>{@link Merge} — a coordinator segment whose topmost {@link org.elasticsearch.xpack.esql.plan.physical.MergeExec} has been
 *       replaced by an {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}; run locally via
 *       {@code ComputeService.runCompute}.</li>
 * </ul>
 *
 * <h2>Why this class exists</h2>
 * <p>
 * A physical plan for a query that contains {@code UNION ALL} (or {@code FORK}) carries one or more
 * {@link org.elasticsearch.xpack.esql.plan.physical.MergeExec} nodes. Each {@code MergeExec} is a fan-in point: it waits for
 * multiple producer branches to finish, then merges their output. Executing such a plan directly is not possible because:
 * <ol>
 *   <li>The producer branches ({@code MergeExec} children) must be dispatched and run <em>independently</em> — possibly on separate
 *       data nodes — before the coordinator merge can read from them.</li>
 *   <li>Each branch must write its output into an exchange sink, and the coordinator merge must read from the matching exchange source.
 *       The exchange infrastructure ({@link org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler} /
 *       {@link org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler}) must be registered before any branch starts.</li>
 * </ol>
 * {@code SubPlan} solves this by decomposing the original plan into a tree of coordinator segments ({@link Merge}) and leaf producers
 * ({@link Leaf}) <em>before</em> any execution begins, so that {@code SubPlansExecutor} can wire the exchanges in one synchronous
 * pass and then dispatch everything in the correct order.
 *
 * <h2>How {@link PlannerUtils#buildSubPlan} creates the tree</h2>
 * <p>
 * {@code buildSubPlan} uses a pre-order traversal ({@code transformDownSkipBranch}) that stops descending into a branch the moment it
 * finds a {@link org.elasticsearch.xpack.esql.plan.physical.MergeExec}:
 * <ol>
 *   <li>When the traversal encounters a {@code MergeExec}, it records it, replaces it in place with an
 *       {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}, and sets {@code skipBranch = true} so the traversal
 *       does not descend into the {@code MergeExec}'s children. The plan above the {@code MergeExec} (processing commands such as
 *       {@code LIMIT}, {@code STATS}, etc.) is kept intact and becomes the coordinator segment plan.</li>
 *   <li>Each child of the recorded {@code MergeExec} is wrapped in an
 *       {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec} and processed by a <em>recursive</em> call to
 *       {@code buildSubPlan}. The recursion may find further nested {@code MergeExec} nodes (sub-{@code UNION ALL}s), each of which
 *       becomes its own {@link Merge} node in the tree.</li>
 *   <li>If no {@code MergeExec} is found, the plan is returned as a {@link Leaf} unchanged.</li>
 * </ol>
 * <p>
 * Because the traversal sets {@code skipBranch = true} the moment it finds a {@code MergeExec}, the children of that {@code MergeExec}
 * are never visited by the outer call — they are processed by separate, independent recursive calls, each with its own
 * {@code Holder<MergeExec>}. This is why arbitrarily deep nesting of {@code UNION ALL} is allowed: every level is handled by its own
 * invocation of {@code buildSubPlan}.
 * <p>
 * Sibling {@code MergeExec} nodes — two {@code UNION ALL}s that are both direct children of a binary operator (e.g. a join) at the
 * same nesting level — are forbidden. Because neither is inside the other's subtree, both are visible to the <em>same</em> traversal
 * and the same {@code Holder<MergeExec>}, causing {@link org.elasticsearch.xpack.esql.EsqlIllegalArgumentException}. This is also a
 * runtime constraint: one compute context supplies one exchange source, so two sibling merge points cannot be served simultaneously.
 * <p>
 * Example — a plan with one top-level {@code UNION ALL} and two nested {@code UNION ALL}s inside one branch:
 * <pre>
 * Input physical plan:
 *   LimitExec
 *   └─ MergeExec                          ← outer merge
 *      ├─ LeafA                            ← direct producer
 *      ├─ MergeExec                        ← inner merge A
 *      │  ├─ LeafB
 *      │  └─ LeafC
 *      └─ MergeExec                        ← inner merge B
 *         ├─ LeafD
 *         └─ LeafE
 *
 * Result SubPlan tree (created by buildSubPlan):
 *   Merge(plan = LimitExec → ExchangeSourceExec)
 *   ├─ Leaf(plan = ExchangeSinkExec → LeafA)
 *   ├─ Merge(plan = ExchangeSinkExec → ExchangeSourceExec)
 *   │  ├─ Leaf(plan = ExchangeSinkExec → LeafB)
 *   │  └─ Leaf(plan = ExchangeSinkExec → LeafC)
 *   └─ Merge(plan = ExchangeSinkExec → ExchangeSourceExec)
 *      ├─ Leaf(plan = ExchangeSinkExec → LeafD)
 *      └─ Leaf(plan = ExchangeSinkExec → LeafE)
 * </pre>
 * The outer {@code MergeExec} is replaced by an {@code ExchangeSourceExec} in the root coordinator segment
 * ({@code LimitExec → ExchangeSourceExec}). Each inner {@code MergeExec} becomes a nested {@link Merge} whose plan is
 * {@code ExchangeSinkExec → ExchangeSourceExec}: it reads from its own children's exchange source and writes the merged output
 * into the outer exchange source via the surrounding {@code ExchangeSinkExec}. No {@code MergeExec} node survives in any plan.
 *
 * <h2>How {@code SubPlansExecutor} executes the tree</h2>
 * <p>
 * {@code ComputeService.execute} dispatches on the root type returned by {@code buildSubPlan}:
 * <ul>
 *   <li>A {@link Leaf} root means no merge; {@code ComputeService} calls {@code executePlan} directly.</li>
 *   <li>A {@link Merge} root means at least one merge point; {@code ComputeService} creates a {@code SubPlansExecutor} and calls
 *       {@code SubPlansExecutor.execute}.</li>
 * </ul>
 * {@code SubPlansExecutor.execute} works in three phases:
 * <ol>
 *   <li><b>Phase 1 — register exchanges ({@code buildSubPlanContext}):</b> walks the {@code SubPlan} tree and registers an
 *       {@link org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler} for every {@link Merge} node and an
 *       {@link org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler} for every child. This phase is fully synchronous. If it
 *       fails partway through, {@code cleanupUnstarted} rolls back all registrations before propagating the error.</li>
 *   <li><b>Phase 2 — wire merge segments ({@code startMerge}):</b> top-down recursive walk that calls
 *       {@code ComputeService.runCompute} for each {@link Merge} node (starting its local coordinator segment) and accumulates all
 *       {@link Leaf} nodes into a flat {@code scheduledLeaves} list. All merge segments are wired before any leaf is dispatched, so
 *       no leaf can complete and attempt to read from an exchange source that has not yet been set up.</li>
 *   <li><b>Phase 3 — dispatch leaves:</b> launches up to {@code branchParallelDegree} initial workers. Each worker atomically claims
 *       the next leaf and re-invokes itself on completion, keeping the number of concurrently running leaves bounded.</li>
 * </ol>
 * The root {@link Merge}'s plan is additionally wrapped in an {@code OutputExec} by {@code buildSubPlanContext} so that the pages it
 * produces are collected into the final result list; nested {@link Merge} nodes use their plans as-is.
 */
public abstract sealed class SubPlan permits SubPlan.Leaf, SubPlan.Merge {

    private final PhysicalPlan plan;

    private SubPlan(PhysicalPlan plan) {
        this.plan = Objects.requireNonNull(plan);
    }

    /** The physical plan executed by this topology node. */
    public PhysicalPlan plan() {
        return plan;
    }

    /**
     * A producer branch with no merge point. Its {@link #plan()} is always an
     * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec} wrapping the original branch plan — the sink writes the
     * branch's output into the parent merge's {@link org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler}.
     * {@code SubPlansExecutor} dispatches it via {@code ComputeService.executePlan}, which fans the plan out to data nodes and runs
     * the coordinator-side reduction locally; the resulting pages flow through the {@code ExchangeSinkExec} into the parent exchange.
     * <p>
     * A {@code Leaf} has no children and carries no mutable execution state. Its lifecycle is fully managed by {@code SubPlansExecutor}
     * through a {@code ScheduledLeaf} record that pairs it with the {@code ActionListener} to notify on completion.
     */
    public static final class Leaf extends SubPlan {
        public Leaf(PhysicalPlan plan) {
            super(plan);
        }
    }

    /**
     * A coordinator segment whose topmost {@link org.elasticsearch.xpack.esql.plan.physical.MergeExec} has been replaced by an
     * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}. Its {@link #plan()} is run locally by {@code
     * SubPlansExecutor} via {@code ComputeService.runCompute}; it reads merged rows from the exchange source that its children write into.
     * <p>
     * The plan shape depends on the node's position in the tree:
     * <ul>
     *   <li><b>Root node</b>: the plan is the original coordinator plan with {@code MergeExec} replaced by {@code ExchangeSourceExec}
     *       (e.g. {@code LimitExec → ExchangeSourceExec}). {@code SubPlansExecutor} additionally wraps it in an {@code OutputExec} at
     *       runtime to collect final result pages.</li>
     *   <li><b>Nested node</b>: the plan is {@code ExchangeSinkExec → ExchangeSourceExec}. It reads from its own children's exchange
     *       source (the inner {@code ExchangeSourceExec}) and writes the merged output into the parent's exchange source (the
     *       {@code ExchangeSinkExec}). No {@code MergeExec} node survives in the plan.</li>
     * </ul>
     * Each child in {@link #children()} is either a {@link Leaf} (a direct producer dispatched to data nodes) or a nested {@link Merge}
     * (another coordinator segment that itself has an exchange source and its own children, produced by a recursive call to
     * {@link PlannerUtils#buildSubPlan}).
     */
    public static final class Merge extends SubPlan {
        private final List<SubPlan> children;

        public Merge(PhysicalPlan plan, List<SubPlan> children) {
            super(plan);
            this.children = List.copyOf(children);
            if (this.children.isEmpty()) {
                // EsqlIllegalArgumentException, not a plain IllegalArgumentException: a branchless MergeExec is unreachable from user
                // input (only Mapper.mapFork builds one, and Fork.checkBranchCount rejects a zero-branch Fork at verification), so this
                // is an internal invariant. The plain exception reports 400 and blames the caller; this one reports 500, matching the
                // sibling guards in PlannerUtils that this constructor is reached from.
                throw new EsqlIllegalArgumentException("a merge execution plan requires at least one child");
            }
        }

        public List<SubPlan> children() {
            return children;
        }
    }
}

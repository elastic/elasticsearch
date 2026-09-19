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
 *   <li>{@link Leaf} — a producer branch with no merge point; dispatched via {@code ComputeService.executePlan}, which fans the
 *       plan out to data nodes and runs the coordinator-side reduction locally.</li>
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
 *   <li>Each branch writes its output into an exchange sink, and the coordinator merge reads from the matching exchange source.
 *       After a {@link Leaf} has been reduced on the coordinator, that hop is same-node: {@code SubPlansExecutor} uses a
 *       {@link org.elasticsearch.compute.operator.exchange.LocalExchange} per {@link Merge}. Cross-node transport lives inside
 *       {@code ComputeService.executePlan} (an {@code ExchangeSourceHandler} pulling from data-node sinks), not in this tree.</li>
 * </ol>
 * {@code SubPlan} solves this by decomposing the original plan into a tree of coordinator segments ({@link Merge}) and leaf producers
 * ({@link Leaf}) <em>before</em> any execution begins, so that {@code SubPlansExecutor} can build a matching runtime tree, open one
 * {@code LocalExchange} per merge, and start coordinators and leaves lazily.
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
 * {@code ExchangeSinkExec → ExchangeSourceExec}: it reads from its own children's {@code LocalExchange} and writes the merged output into
 * the parent's {@code LocalExchange} via the surrounding {@code ExchangeSinkExec}. No {@code MergeExec} node survives in any plan.
 *
 * <h2>How {@code SubPlansExecutor} executes the tree</h2>
 * <p>
 * {@code ComputeService.execute} dispatches on the root type returned by {@code buildSubPlan}:
 * <ul>
 *   <li>A {@link Leaf} root means no merge; {@code ComputeService} calls {@code executePlan} directly.</li>
 *   <li>A {@link Merge} root means at least one merge point; {@code ComputeService} creates a {@code SubPlansExecutor} and calls
 *       {@code SubPlansExecutor.executePlan}.</li>
 * </ul>
 * The executor constructor mirrors this tree as runtime nodes ({@code ExecutionMerge} / {@code ExecutionLeaf}). That walk does not start
 * drivers. It opens a {@code LocalExchange} per merge, a {@code ComputeListener} per merge with one ref per child, and a dummy sink on
 * each parent exchange so an unstarted child cannot look like “all producers finished.” The root plan is wrapped in an {@code OutputExec}
 * to collect result pages. Only the root {@code LocalExchange} is registered on
 * {@link org.elasticsearch.compute.operator.exchange.ExchangeService} (under the query session id) so async STOP can finish it. Nested
 * exchanges stay coordinator-private.
 * <p>
 * {@code executePlan} then runs a permit-gated depth-first search ({@code branch_parallel_degree} permits). Each visit starts the
 * current merge if needed ({@code runCompute}, so the consumer is running before any child writes), then spends remaining permits on
 * children left to right. A {@link Leaf} consumes one permit and calls {@code executePlan}, passing a sink on the parent
 * {@code LocalExchange}. When that leaf completes it returns the permit and the walk continues. A nested {@link Merge} is therefore
 * started only when the walk first reaches it — typically just before its first descendant leaf is dispatched.
 * <p>
 * If the query is stopped or the parent exchange is already finished ({@code LIMIT} satisfied), the visit completes the node without
 * {@code runCompute} / {@code executePlan} so queued branches are skipped. Failure cancels the root task; remaining nodes observe that
 * and fail without starting new work.
 */
public abstract sealed class SubPlan permits SubPlan.Leaf, SubPlan.Merge {

    private final PhysicalPlan plan;

    private SubPlan(PhysicalPlan plan) {
        this.plan = Objects.requireNonNull(plan);
    }

    /** The physical plan executed by this node. */
    public PhysicalPlan plan() {
        return plan;
    }

    /**
     * A producer branch with no merge point. When this node is a child of a {@link Merge}, {@link #plan()} is an
     * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec} wrapping the original branch plan — the sink writes the
     * branch's output into the parent merge's {@link org.elasticsearch.compute.operator.exchange.LocalExchange}. If the plan has no
     * {@code MergeExec}, it is executed by {@code ComputeService} directly, not by {@code SubPlansExecutor}.
     */
    public static final class Leaf extends SubPlan {
        public Leaf(PhysicalPlan plan) {
            super(plan);
        }
    }

    /**
     * A coordinator segment whose topmost {@link org.elasticsearch.xpack.esql.plan.physical.MergeExec} has been replaced by an
     * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}. Its {@link #plan()} is run locally by
     * {@code SubPlansExecutor} via {@code ComputeService.runCompute}; it reads merged rows from the {@code LocalExchange} that its
     * children write into.
     * <p>
     * The plan shape depends on the node's position in the tree:
     * <ul>
     *   <li><b>Root merge</b>: the plan is the original coordinator plan with {@code MergeExec} replaced by {@code ExchangeSourceExec}
     *       (e.g. {@code LimitExec → ExchangeSourceExec}). {@code SubPlansExecutor} additionally wraps it in an {@code OutputExec} at
     *       runtime to collect final result pages, and registers this node's {@code LocalExchange} on {@code ExchangeService}.</li>
     *   <li><b>Nested merge</b>: the plan is {@code ExchangeSinkExec → ExchangeSourceExec}. It reads from its own children's
     *       {@code LocalExchange} (the inner {@code ExchangeSourceExec}) and writes the merged output into the parent's
     *       {@code LocalExchange} (the {@code ExchangeSinkExec}).</li>
     * </ul>
     * Each child in {@link #children()} is either a {@link Leaf} (a direct producer dispatched via {@code executePlan}) or a nested
     * {@link Merge} (another coordinator segment with its own {@code LocalExchange} and children. The executor starts this segment lazily,
     * {@code runCompute} runs when the depth-first search first reaches the node, not during tree construction.
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

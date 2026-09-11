/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A {@link UnionAll} produced by source expansion, as opposed to user-written subqueries
 * or {@link ViewUnionAll}. Children are independently distributable source plans for one
 * resolved {@code FROM}.
 * <p>
 * View resolution may build a <em>provisional</em> instance that keeps the original ordered
 * branch keys so an index-only composition can be restored to the equivalent view plan.
 * Nested source fan-ins stay grouped on that path. The normalized planning input is a
 * <em>final</em> fan-in: nested source fan-ins are flattened into this node and the
 * reconstruction keys are discarded.
 */
public class SourceFanInUnionAll extends UnionAll {

    /**
     * Cap on the number of source producers one resolved {@code FROM} may expand to. Distinct from
     * {@link Fork#MAX_BRANCHES}, which bounds the branches of a user-written {@code FORK}: those are
     * independent query pipelines, while these are the sources of a single {@code FROM}, closer to the
     * concrete indices behind one {@code EsRelation}. Enforced twice: {@code DatasetRewriter} rejects an
     * over-cap expansion before pre-analysis so field-caps never walks the extra leaves, and
     * {@link Fork#checkBranchCount} catches any tree that reaches post-analysis over the cap, including one
     * assembled by flattening.
     * <p>
     * The bound is per resolved {@code FROM}, not per plan, and two things sit outside the pre-analysis half
     * of it. Cross-project shadows are appended after the rewrite-time check, so a shadow that matches a
     * remote namesake becomes a real producer that only {@link Fork#checkBranchCount} counts; a {@code FROM}
     * naming more than half the cap in exact dataset names can therefore be rejected post-analysis for a
     * count the user did not write. And a user {@code FORK} copies the pipeline into every branch, so each
     * branch carries its own fan-in: the plan-wide producer count reaches {@link Fork#MAX_BRANCHES} times
     * this number, and the per-source costs below are paid that many times. Peak concurrency is unaffected,
     * since one throttle is shared across the whole session.
     * <p>
     * The number bounds plan size and resolution work, not execution concurrency: the
     * {@code branch_parallel_degree} pragma throttles how many producers run at once regardless of how many
     * exist. What scales with the producer count is one plan optimized and mapped per source, one schema
     * resolution and one split-discovery round per source, one distribution decision per source, and a child
     * compute session per source. The execution cost that comes with each of those is described on
     * {@code ComputeService#executeSourceFanIn}.
     * <p>
     * That per-source cost is what separates this from the index case, where the number is unbounded. A
     * {@code FROM} over many indices is one {@code EsRelation} carrying many concrete indices: one plan
     * node, one batched field-caps round, and shard dispatch gathered into a single request per node. Index
     * count therefore costs constant plan work and one request per node, while producer count costs linear
     * plan work and a request per producer per node. Raising this number does not change that shape, it only
     * moves further along it.
     * <p>
     * A relation that carried many resources itself would remove the per-source branch: one plan, one
     * resolution, and one dispatch per node would cover every source, with fan-out happening over splits
     * inside the relation the way a single dataset's files already do. The producer count would then be
     * bounded by execution parallelism rather than by plan size, and this cap would have nothing left to
     * bound.
     */
    public static final int MAX_PRODUCERS = 8;

    /**
     * Ordered branch keys from view resolution, aligned with {@link #children()}. {@code null}
     * means this is the final flat fan-in. Keys themselves may be {@code null}; {@link ViewUnionAll}
     * allows a null map key for an unnamed branch.
     */
    @Nullable
    private final List<String> branchKeys;

    /**
     * Returns {@code true} if {@code count} producers would exceed {@link #MAX_PRODUCERS}. Centralizes the
     * comparison so callers that fail earlier with a more user-facing message stay in step with the cap.
     */
    public static boolean exceedsMaxProducers(int count) {
        return count > MAX_PRODUCERS;
    }

    /**
     * Builds a final source-fan-in union whose children are independently distributable producers.
     * Nested source fan-ins are flattened.
     */
    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        this(source, children, output, null);
    }

    /**
     * Builds a source-fan-in union. A null {@code branchKeys} is the final flat form. A non-null
     * list is analysis-only reconstruction data and must have one entry per child, including a
     * possible null key.
     */
    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output, @Nullable List<String> branchKeys) {
        super(source, branchKeys == null ? flattenSourceFanInChildren(children) : children, output);
        if (branchKeys == null) {
            this.branchKeys = null;
            return;
        }
        if (branchKeys.size() != children.size()) {
            throw new IllegalArgumentException("provisional source expansion requires one branch key per child");
        }
        this.branchKeys = Collections.unmodifiableList(new ArrayList<>(branchKeys));
    }

    /**
     * Analysis-only source expansion that keeps the resolver's ordered branch keys and does not
     * flatten nested source groups. Normalization either lowers this to a final fan-in or restores
     * the equivalent {@link ViewUnionAll}.
     */
    public static SourceFanInUnionAll provisional(Source source, LinkedHashMap<String, LogicalPlan> branches, List<Attribute> output) {
        return new SourceFanInUnionAll(
            source,
            new ArrayList<>(branches.values()),
            output,
            Collections.unmodifiableList(new ArrayList<>(branches.keySet()))
        );
    }

    /**
     * True when this node still carries reconstruction keys from view resolution.
     */
    public boolean isProvisional() {
        return branchKeys != null;
    }

    /**
     * Ordered branch keys aligned with {@link #children()}, or {@code null} for a final fan-in.
     */
    @Nullable
    public List<String> branchKeys() {
        return branchKeys;
    }

    /**
     * Normalizes nested {@link SourceFanInUnionAll} children away so no caller can build a final
     * fan-in of fan-ins: the producers of one resolved {@code FROM} are always this node's direct
     * children. Provisional construction skips this so nested groups can be restored.
     */
    static List<LogicalPlan> flattenSourceFanInChildren(List<LogicalPlan> children) {
        boolean needsFlatten = false;
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll) {
                needsFlatten = true;
                break;
            }
        }
        if (needsFlatten == false) {
            return children;
        }
        List<LogicalPlan> flattened = new ArrayList<>(children.size());
        for (LogicalPlan child : children) {
            if (child instanceof SourceFanInUnionAll nested) {
                flattened.addAll(nested.children());
            } else {
                flattened.add(child);
            }
        }
        return flattened;
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        if (branchKeys != null) {
            if (newChildren.size() != branchKeys.size()) {
                throw new IllegalArgumentException(
                    "provisional source expansion expects a 1:1 positional replacement; use pruneEmptyBranches to drop branches"
                );
            }
            return new SourceFanInUnionAll(source(), newChildren, output(), branchKeys);
        }
        return new SourceFanInUnionAll(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        if (branchKeys == null) {
            return NodeInfo.create(this, SourceFanInUnionAll::new, children(), output());
        }
        return NodeInfo.create(this, SourceFanInUnionAll::new, children(), output(), branchKeys);
    }

    @Override
    public SourceFanInUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return (SourceFanInUnionAll) replaceChildren(subPlans);
    }

    @Override
    public SourceFanInUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        if (branchKeys != null) {
            if (subPlans.size() != branchKeys.size()) {
                throw new IllegalArgumentException(
                    "provisional source expansion expects a 1:1 positional replacement; use pruneEmptyBranches to drop branches"
                );
            }
            return new SourceFanInUnionAll(source(), subPlans, output, branchKeys);
        }
        return new SourceFanInUnionAll(source(), subPlans, output);
    }

    @Override
    public SourceFanInUnionAll refreshOutput() {
        return new SourceFanInUnionAll(source(), children(), refreshedOutput(), branchKeys);
    }

    @Override
    public ExecuteLocation executesOn() {
        return ExecuteLocation.ANY;
    }

    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        List<String> keptKeys = branchKeys == null ? null : new ArrayList<>(children().size());
        List<LogicalPlan> kids = children();
        for (int i = 0; i < kids.size(); i++) {
            LogicalPlan child = kids.get(i);
            if (isEmpty.test(child) == false) {
                kept.add(child);
                if (keptKeys != null) {
                    keptKeys.add(branchKeys.get(i));
                }
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        return new SourceFanInUnionAll(source(), kept, output(), keptKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SourceFanInUnionAll.class, children(), branchKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SourceFanInUnionAll other = (SourceFanInUnionAll) o;
        return Objects.equals(children(), other.children()) && Objects.equals(branchKeys, other.branchKeys);
    }
}

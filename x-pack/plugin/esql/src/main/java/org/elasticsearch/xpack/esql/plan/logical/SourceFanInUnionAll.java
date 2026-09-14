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
     * {@link MergePlan#MAX_BRANCHES}, which bounds the branches of a user-written {@code FORK}.
     * {@code DatasetRewriter} rejects an over-cap expansion before field-caps, and
     * {@link MergePlan#checkBranchCount} rejects any tree that still exceeds the cap after flattening.
     * The number bounds plan size and resolution work, not execution concurrency: the
     * {@code branch_parallel_degree} pragma throttles how many producers run at once.
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

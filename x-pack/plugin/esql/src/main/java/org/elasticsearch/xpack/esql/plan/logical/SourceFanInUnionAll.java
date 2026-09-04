/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A {@link UnionAll} produced by dataset source expansion, as opposed to user-written subqueries
 * or {@link ViewUnionAll}. Children are independently distributable source plans for one
 * resolved {@code FROM}. Nested source fan-ins are flattened into this node.
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
    public static final int MAX_PRODUCERS = 100;

    /**
     * Returns {@code true} if {@code count} producers would exceed {@link #MAX_PRODUCERS}. Centralizes the
     * comparison so callers that fail earlier with a more user-facing message stay in step with the cap.
     */
    public static boolean exceedsMaxProducers(int count) {
        return count > MAX_PRODUCERS;
    }

    /**
     * Builds a source-fan-in union whose children are independently distributable producers.
     */
    public SourceFanInUnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, flattenSourceFanInChildren(children), output);
    }

    /**
     * Lifts nested {@link SourceFanInUnionAll} children into this node so a view whose body is
     * already a multi-source {@code FROM} can be composed with another source.
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
        return new SourceFanInUnionAll(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, SourceFanInUnionAll::new, children(), output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new SourceFanInUnionAll(source(), subPlans, output());
    }

    @Override
    public SourceFanInUnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new SourceFanInUnionAll(source(), subPlans, output);
    }

    @Override
    public SourceFanInUnionAll refreshOutput() {
        return new SourceFanInUnionAll(source(), children(), refreshedOutput());
    }

    @Override
    public ExecuteLocation executesOn() {
        return ExecuteLocation.ANY;
    }

    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        for (LogicalPlan child : children()) {
            if (isEmpty.test(child) == false) {
                kept.add(child);
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        return new SourceFanInUnionAll(source(), kept, output());
    }

    @Override
    public int hashCode() {
        return Objects.hash(SourceFanInUnionAll.class, children());
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
        return Objects.equals(children(), other.children());
    }
}

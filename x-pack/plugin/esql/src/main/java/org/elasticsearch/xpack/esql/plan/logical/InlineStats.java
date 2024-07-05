/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class InlineStats extends UnaryPlan implements Phased {

    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;
    private List<Attribute> output;

    public InlineStats(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    @Override
    protected NodeInfo<InlineStats> info() {
        return NodeInfo.create(this, InlineStats::new, child(), groupings, aggregates);
    }

    @Override
    public InlineStats replaceChild(LogicalPlan newChild) {
        return new InlineStats(source(), newChild, groupings, aggregates);
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    @Override
    public List<Attribute> output() {
        if (this.output == null) {
            this.output = mergeOutputAttributes(Expressions.asAttributes(aggregates), child().output());
        }
        return output;
    }

    @Override
    public LogicalPlan firstPhase() {
        return new Aggregate(source(), child(), Aggregate.AggregateType.STANDARD, groupings, aggregates);
    }

    @Override
    public LogicalPlan nextPhase(List<Attribute> layout, List<Page> firstPhaseResult) {
        // NOCOMMIT memory tracking
        if (firstPhaseResult.size() > 1) {
            throw new UnsupportedOperationException();
        }
        Page page = firstPhaseResult.get(0);
        Block[] blocks = IntStream.range(0, page.getBlockCount()).mapToObj(b -> {
            Block block = page.getBlock(b);
            Block.Builder builder = block.elementType().newBlockBuilder(block.getPositionCount(), PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
            builder.copyFrom(block, 0, block.getPositionCount());
            return builder.build();
        }).toArray(Block[]::new);
        LocalRelation local = new LocalRelation(source(), layout, LocalSupplier.of(blocks));

        List<Attribute> groupingAttributes = new ArrayList<>(groupings.size());
        for (Expression g : groupings) {
            if (g instanceof Attribute a) {
                groupingAttributes.add(a);
            } else {
                throw new UnsupportedOperationException("INLINESTATS doesn't support expressions in grouping position yet");
            }
        }

        List<Attribute> leftFields = new ArrayList<>(groupingAttributes.size());
        List<Attribute> rightFields = new ArrayList<>(groupingAttributes.size());
        List<Attribute> rhsOutput = Join.makeReference(local.output());
        for (Attribute lhs : groupingAttributes) {
            for (Attribute rhs : rhsOutput) {
                if (lhs.name().equals(rhs.name())) {
                    leftFields.add(lhs);
                    rightFields.add(rhs);
                    break;
                }
            }
        }
        JoinConfig config = new JoinConfig(JoinType.LEFT, groupingAttributes, leftFields, rightFields);

        return new Join(source(), child(), local, config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InlineStats other = (InlineStats) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child());
    }
}

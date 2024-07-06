/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class InlineStats extends UnaryPlan implements NamedWriteable, Phased {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        InlineStats.class,
        "InlineStats",
        InlineStats::new
    );

    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;
    private List<Attribute> output;

    public InlineStats(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    public InlineStats(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            ((PlanStreamInput) in).readLogicalPlanNode(),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        ((PlanStreamOutput) out).writeLogicalPlanNode(child());
        out.writeNamedWriteableCollection(groupings);
        out.writeNamedWriteableCollection(aggregates);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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

    @Override
    public LogicalPlan firstPhase() {
        return new Aggregate(source(), child(), Aggregate.AggregateType.STANDARD, groupings, aggregates);
    }

    @Override
    public LogicalPlan nextPhase(List<Attribute> schema, List<Page> firstPhaseResult) {
        LocalRelation local = firstPhaseResultsToLocalRelation(schema, firstPhaseResult);
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

    private LocalRelation firstPhaseResultsToLocalRelation(List<Attribute> schema, List<Page> firstPhaseResult) {
        // Limit ourselves to 1mb of results similar to LOOKUP for now.
        long bytesUsed = firstPhaseResult.stream().mapToLong(Page::ramBytesUsedByBlocks).sum();
        if (bytesUsed > ByteSizeValue.ofMb(1).getBytes()) {
            throw new IllegalArgumentException("first phase result too large [" + ByteSizeValue.ofBytes(bytesUsed) + "] > 1mb");
        }
        int positionCount = firstPhaseResult.stream().mapToInt(Page::getPositionCount).sum();
        Block.Builder[] builders = new Block.Builder[schema.size()];
        Block[] blocks;
        try {
            for (int b = 0; b < builders.length; b++) {
                builders[b] = PlannerUtils.toElementType(schema.get(b).dataType())
                    .newBlockBuilder(positionCount, PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
            }
            for (Page p : firstPhaseResult) {
                for (int b = 0; b < builders.length; b++) {
                    builders[b].copyFrom(p.getBlock(b), 0, p.getPositionCount());
                }
            }
            blocks = Block.Builder.buildAll(builders);
        } finally {
            Releasables.closeExpectNoException(builders);
        }
        return new LocalRelation(source(), schema, LocalSupplier.of(blocks));
    }

}

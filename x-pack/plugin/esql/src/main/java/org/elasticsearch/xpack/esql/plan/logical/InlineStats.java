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
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
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

/**
 * Enriches the stream of data with the results of running a {@link Aggregate STATS}.
 * <p>
 *     This is a {@link Phased} operation that doesn't have a "native" implementation.
 *     Instead, it's implemented as first running a {@link Aggregate STATS} and then
 *     a {@link Join}.
 * </p>
 */
public class InlineStats extends UnaryPlan implements NamedWriteable, Phased, Stats {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "InlineStats",
        InlineStats::new
    );

    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;
    private List<Attribute> lazyOutput;

    public InlineStats(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child);
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    public InlineStats(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
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

    @Override
    public InlineStats with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new InlineStats(source(), child, newGroupings, newAggregates);
    }

    @Override
    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    @Override
    public List<Attribute> output() {
        if (this.lazyOutput == null) {
            List<NamedExpression> addedFields = new ArrayList<>();
            AttributeSet set = child().outputSet();

            for (NamedExpression agg : aggregates) {
                Attribute att = agg.toAttribute();
                if (set.contains(att) == false) {
                    addedFields.add(agg);
                    set.add(att);
                }
            }

            this.lazyOutput = mergeOutputAttributes(addedFields, child().output());
        }
        return lazyOutput;
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
        if (equalsAndSemanticEquals(firstPhase().output(), schema) == false) {
            throw new IllegalStateException("Unexpected first phase outputs: " + firstPhase().output() + " vs " + schema);
        }
        if (groupings.isEmpty()) {
            return ungroupedNextPhase(schema, firstPhaseResult);
        }
        return groupedNextPhase(schema, firstPhaseResult);
    }

    private LogicalPlan ungroupedNextPhase(List<Attribute> schema, List<Page> firstPhaseResult) {
        if (firstPhaseResult.size() != 1) {
            throw new IllegalArgumentException("expected single row");
        }
        Page p = firstPhaseResult.get(0);
        if (p.getPositionCount() != 1) {
            throw new IllegalArgumentException("expected single row");
        }
        List<Alias> values = new ArrayList<>(schema.size());
        for (int i = 0; i < schema.size(); i++) {
            Attribute s = schema.get(i);
            Object value = BlockUtils.toJavaObject(p.getBlock(i), 0);
            values.add(new Alias(source(), s.name(), new Literal(source(), value, s.dataType()), aggregates.get(i).id()));
        }
        return new Eval(source(), child(), values);
    }

    private static boolean equalsAndSemanticEquals(List<Attribute> left, List<Attribute> right) {
        if (left.equals(right) == false) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).semanticEquals(right.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    private LogicalPlan groupedNextPhase(List<Attribute> schema, List<Page> firstPhaseResult) {
        LocalRelation local = firstPhaseResultsToLocalRelation(schema, firstPhaseResult);
        List<Attribute> groupingAttributes = new ArrayList<>(groupings.size());
        for (Expression g : groupings) {
            if (g instanceof Attribute a) {
                groupingAttributes.add(a);
            } else {
                throw new IllegalStateException("optimized plans should only have attributes in groups, but got [" + g + "]");
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

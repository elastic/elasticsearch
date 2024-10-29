/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Aggregate extends UnaryPlan implements Stats {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Aggregate",
        Aggregate::new
    );

    public enum AggregateType {
        STANDARD,
        // include metrics aggregates such as rates
        METRICS;

        static void writeType(StreamOutput out, AggregateType type) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeString(type.name());
            } else if (type != STANDARD) {
                throw new IllegalStateException("cluster is not ready to support aggregate type [" + type + "]");
            }
        }

        static AggregateType readType(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                return AggregateType.valueOf(in.readString());
            } else {
                return STANDARD;
            }
        }
    }

    private final AggregateType aggregateType;
    private final List<Expression> groupings;
    private final List<? extends NamedExpression> aggregates;

    private List<Attribute> lazyOutput;

    public Aggregate(
        Source source,
        LogicalPlan child,
        AggregateType aggregateType,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates
    ) {
        super(source, child);
        this.aggregateType = aggregateType;
        this.groupings = groupings;
        this.aggregates = aggregates;
    }

    public Aggregate(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            AggregateType.readType(in),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        AggregateType.writeType(out, aggregateType());
        out.writeNamedWriteableCollection(groupings);
        out.writeNamedWriteableCollection(aggregates());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, Aggregate::new, child(), aggregateType, groupings, aggregates);
    }

    @Override
    public Aggregate replaceChild(LogicalPlan newChild) {
        return new Aggregate(source(), newChild, aggregateType, groupings, aggregates);
    }

    @Override
    public Aggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new Aggregate(source(), child, aggregateType(), newGroupings, newAggregates);
    }

    public AggregateType aggregateType() {
        return aggregateType;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    public List<? extends NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public String commandName() {
        return switch (aggregateType) {
            case STANDARD -> "STATS";
            case METRICS -> "METRICS";
        };
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && Resolvables.resolved(aggregates);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = output(aggregates);
        }
        return lazyOutput;
    }

    public static List<Attribute> output(List<? extends NamedExpression> aggregates) {
        return mergeOutputAttributes(Expressions.asAttributes(aggregates), emptyList());
    }

    @Override
    protected AttributeSet computeReferences() {
        return computeReferences(aggregates, groupings);
    }

    public static AttributeSet computeReferences(List<? extends NamedExpression> aggregates, List<? extends Expression> groupings) {
        AttributeSet result = Expressions.references(groupings).combine(Expressions.references(aggregates));
        for (Expression grouping : groupings) {
            if (grouping instanceof Alias) {
                result.remove(((Alias) grouping).toAttribute());
            }
        }
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregateType, groupings, aggregates, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Aggregate other = (Aggregate) obj;
        return aggregateType == other.aggregateType
            && Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child());
    }
}

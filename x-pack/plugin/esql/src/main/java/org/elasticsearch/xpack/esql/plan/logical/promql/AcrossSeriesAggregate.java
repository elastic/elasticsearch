/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AcrossSeriesAggregate extends PromqlFunctionCall {

    public enum Grouping {
        BY,
        WITHOUT,
        NONE
    }

    private final Grouping grouping;
    private final List<NamedExpression> groupings;
    private final NameId stepId;
    private final NameId valueId;

    public AcrossSeriesAggregate(
        Source source,
        LogicalPlan child,
        String functionName,
        List<Expression> parameters,
        Grouping grouping,
        List<NamedExpression> groupings
    ) {
        this(source, child, functionName, parameters, grouping, groupings, new NameId(), new NameId());
    }

    public AcrossSeriesAggregate(
        Source source,
        LogicalPlan child,
        String functionName,
        List<Expression> parameters,
        Grouping grouping,
        List<NamedExpression> groupings,
        NameId stepId,
        NameId valueId
    ) {
        super(source, child, functionName, parameters);
        this.grouping = grouping;
        this.groupings = groupings;
        this.stepId = stepId;
        this.valueId = valueId;
    }

    public Grouping grouping() {
        return grouping;
    }

    public List<NamedExpression> groupings() {
        return groupings;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(groupings) && super.expressionsResolved();
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(
            this,
            AcrossSeriesAggregate::new,
            child(),
            functionName(),
            parameters(),
            grouping(),
            groupings(),
            stepId(),
            valueId()
        );
    }

    @Override
    public AcrossSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new AcrossSeriesAggregate(source(), newChild, functionName(), parameters(), grouping(), groupings(), stepId(), valueId());
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_ACROSS_SERIES_AGGREGATION";
    // }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            AcrossSeriesAggregate that = (AcrossSeriesAggregate) o;
            return grouping == that.grouping && Objects.equals(groupings, that.groupings);
        }
        return false;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = new ArrayList<>(groupings.size() + 2);
            output.add(new ReferenceAttribute(source(), null, sourceText(), DataType.DOUBLE, Nullability.FALSE, valueId, false));
            output.add(new ReferenceAttribute(source(), null, "step", DataType.DATETIME, Nullability.FALSE, stepId, false));
            for (NamedExpression exp : groupings) {
                output.add(exp.toAttribute());
            }
        }
        return output;
    }

    public NameId valueId() {
        return valueId;
    }

    public NameId stepId() {
        return stepId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), grouping, groupings);
    }
}

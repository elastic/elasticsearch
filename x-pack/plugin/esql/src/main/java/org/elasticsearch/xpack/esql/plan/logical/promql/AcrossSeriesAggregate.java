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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
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

    public AcrossSeriesAggregate(
        Source source,
        LogicalPlan child,
        String functionName,
        List<Expression> parameters,
        Grouping grouping,
        List<NamedExpression> groupings
    ) {
        super(source, child, functionName, parameters);
        this.grouping = grouping;
        this.groupings = groupings;
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
        return NodeInfo.create(this, AcrossSeriesAggregate::new, child(), functionName(), parameters(), grouping(), groupings());
    }

    @Override
    public AcrossSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new AcrossSeriesAggregate(source(), newChild, functionName(), parameters(), grouping(), groupings());
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
        List<Attribute> output = new ArrayList<>(groupings.size());
        for (NamedExpression exp : groupings) {
            output.add(exp.toAttribute());
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), grouping, groupings);
    }
}

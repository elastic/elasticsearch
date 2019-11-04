/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeSet;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class Pivot extends UnaryPlan {

    private final Expression column;
    private final List<NamedExpression> values;
    private final List<NamedExpression> aggregates;
    // derived properties
    private AttributeSet groupingSet;
    private AttributeSet valueOutput;
    private List<Attribute> output;

    public Pivot(Source source, LogicalPlan child, Expression column, List<NamedExpression> values, List<NamedExpression> aggregates) {
        super(source, child);
        this.column = column;
        this.values = values;
        this.aggregates = aggregates;
    }

    private static Expression withQualifierNull(Expression e) {
        if (e instanceof Attribute) {
            Attribute fa = (Attribute) e;
            return fa.withQualifier(null);
        }
        return e;
    }

    @Override
    protected NodeInfo<Pivot> info() {
        return NodeInfo.create(this, Pivot::new, child(), column, values, aggregates);
    }

    @Override
    protected Pivot replaceChild(LogicalPlan newChild) {
        Expression newColumn = column;
        List<NamedExpression> newAggregates = aggregates;

        if (newChild instanceof EsRelation) {
            // when changing from a SubQueryAlias to EsRelation
            // the qualifier of the column and aggregates needs
            // to be changed to null like the attributes of EsRelation
            // otherwise they don't equal and aren't removed
            // when calculating the groupingSet
            newColumn = column.transformUp(Pivot::withQualifierNull);
            newAggregates = aggregates.stream().map((NamedExpression aggregate) ->
                (NamedExpression) aggregate.transformUp(Pivot::withQualifierNull)
            ).collect(Collectors.toList());
        }

        return new Pivot(source(), newChild, newColumn, values, newAggregates);
    }

    public Expression column() {
        return column;
    }

    public List<NamedExpression> values() {
        return values;
    }

    public List<NamedExpression> aggregates() {
        return aggregates;
    }

    public AttributeSet groupingSet() {
        if (groupingSet == null) {
            AttributeSet columnSet = Expressions.references(singletonList(column));
            // grouping can happen only on "primitive" fields, thus exclude multi-fields or nested docs
            // the verifier enforces this rule so it does not catch folks by surprise
            groupingSet = new AttributeSet(Expressions.onlyPrimitiveFieldAttributes(child().output()))
                    // make sure to have the column as the last entry (helps with translation)
                    .subtract(columnSet)
                    .subtract(Expressions.references(aggregates))
                    .combine(columnSet);
        }
        return groupingSet;
    }

    public AttributeSet valuesOutput() {
        // TODO: the generated id is a hack since it can clash with other potentially generated ids
        if (valueOutput == null) {
            List<Attribute> out = new ArrayList<>(aggregates.size() * values.size());
            if (aggregates.size() == 1) {
                NamedExpression agg = aggregates.get(0);
                for (NamedExpression value : values) {
                    ExpressionId id = value.id();
                    out.add(value.toAttribute().withDataType(agg.dataType()).withId(id));
                }
            }
            // for multiple args, concat the function and the value
            else {
                for (NamedExpression agg : aggregates) {
                    String name = agg instanceof Function ? ((Function) agg).functionName() : agg.name();
                    for (NamedExpression value : values) {
                        ExpressionId id = value.id();
                        out.add(value.toAttribute().withName(value.name() + "_" + name).withDataType(agg.dataType()).withId(id));
                    }
                }
            }
            valueOutput = new AttributeSet(out);
        }
        return valueOutput;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = new ArrayList<>(groupingSet()
                    .subtract(Expressions.references(singletonList(column)))
                    .combine(valuesOutput()));
        }

        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return column.resolved() && Resolvables.resolved(values) && Resolvables.resolved(aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, values, aggregates, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Pivot other = (Pivot) obj;
        return Objects.equals(column, other.column)
                && Objects.equals(values, other.values)
                && Objects.equals(aggregates, other.aggregates)
                && Objects.equals(child(), other.child());
    }
}

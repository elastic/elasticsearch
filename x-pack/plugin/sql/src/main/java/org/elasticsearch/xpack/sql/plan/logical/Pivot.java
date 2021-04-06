/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class Pivot extends UnaryPlan {

    private final Expression column;
    private final List<NamedExpression> values;
    private final List<NamedExpression> aggregates;
    private final List<Attribute> grouping;
    // derived properties
    private AttributeSet groupingSet;
    private AttributeSet valueOutput;
    private List<Attribute> output;
    private AttributeMap<Expression> aliases;

    public Pivot(Source source, LogicalPlan child, Expression column, List<NamedExpression> values, List<NamedExpression> aggregates) {
        this(source, child, column, values, aggregates, null);
    }

    public Pivot(Source source, LogicalPlan child, Expression column, List<NamedExpression> values, List<NamedExpression> aggregates,
            List<Attribute> grouping) {
        super(source, child);
        this.column = column;
        this.values = values;
        this.aggregates = aggregates;

        // resolve the grouping set ASAP so it doesn't get re-resolved after analysis (since the aliasing information has been removed)
        if (grouping == null && expressionsResolved()) {
            AttributeSet columnSet = Expressions.references(singletonList(column));
            // grouping can happen only on "primitive" fields, thus exclude multi-fields or nested docs
            // the verifier enforces this rule so it does not catch folks by surprise
            grouping = new ArrayList<>(new AttributeSet(Expressions.onlyPrimitiveFieldAttributes(child().output()))
                    // make sure to have the column as the last entry (helps with translation) so substract it
                    .subtract(columnSet)
                    .subtract(Expressions.references(aggregates))
                    .combine(columnSet));
        }

        this.grouping = grouping;
        this.groupingSet = grouping != null ? new AttributeSet(grouping) : null;
    }

    @Override
    protected NodeInfo<Pivot> info() {
        return NodeInfo.create(this, Pivot::new, child(), column, values, aggregates, grouping);
    }

    @Override
    protected Pivot replaceChild(LogicalPlan newChild) {
        return new Pivot(source(), newChild, column, values, aggregates, grouping);
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

    public List<Attribute> groupings() {
        return grouping;
    }

    public AttributeSet groupingSet() {
        if (groupingSet == null) {
            throw new SqlIllegalArgumentException("Cannot determine grouping in unresolved PIVOT");
        }
        return groupingSet;
    }

    private AttributeSet valuesOutput() {
        if (valueOutput == null) {
            List<Attribute> out = new ArrayList<>(aggregates.size() * values.size());
            if (aggregates.size() == 1) {
                NamedExpression agg = aggregates.get(0);
                for (NamedExpression value : values) {
                    out.add(value.toAttribute().withDataType(agg.dataType()));
                }
            }
            // for multiple args, concat the function and the value
            else {
                for (NamedExpression agg : aggregates) {
                    String name = agg.name();
                    if (agg instanceof Alias) {
                        Alias a = (Alias) agg;
                        if (a.child() instanceof Function) {
                            name = ((Function) a.child()).functionName();
                        }
                    }
                    //FIXME: the value attributes are reused and thus will clash - new ids need to be created
                    for (NamedExpression value : values) {
                        out.add(value.toAttribute().withName(value.name() + "_" + name).withDataType(agg.dataType()));
                    }
                }
            }
            valueOutput = new AttributeSet(out);
        }
        return valueOutput;
    }

    public AttributeMap<Literal> valuesToLiterals() {
        AttributeSet outValues = valuesOutput();
        AttributeMap.Builder<Literal> valuesMap = AttributeMap.builder();

        int index = 0;
        // for each attribute, associate its value
        // take into account while iterating that attributes are a multiplication of actual values
        for (Attribute attribute : outValues) {
            NamedExpression namedExpression = values.get(index % values.size());
            index++;
            // everything should have resolved to an alias
            if (namedExpression instanceof Alias) {
                valuesMap.put(attribute, Literal.of(((Alias) namedExpression).child()));
            }
            // fallback - verifier should prevent this
            else {
                throw new SqlIllegalArgumentException("Unexpected alias", namedExpression);
            }
        }

        return valuesMap.build();
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

    // Since pivot creates its own columns (and thus aliases)
    // remember the backing expressions inside a dedicated aliases map
    public AttributeMap<Expression> aliases() {
        // make sure to initialize all expressions
        output();
        return aliases;
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

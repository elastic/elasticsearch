/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;
import java.util.Objects;

/**
 * Count the number of documents matched ({@code COUNT})
 * <strong>OR</strong> count the number of distinct values
 * for a field that matched ({@code COUNT(DISTINCT}.
 */
public class Count extends AggregateFunction {

    private final boolean distinct;

    public Count(Source source, Expression field, boolean distinct) {
        super(source, field);
        this.distinct = distinct;
    }

    @Override
    protected NodeInfo<Count> info() {
        return NodeInfo.create(this, Count::new, field(), distinct);
    }

    @Override
    public Count replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Count(source(), newChildren.get(0), distinct);
    }

    public boolean distinct() {
        return distinct;
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public String functionId() {
        String functionId = id().toString();
        // if count works against a given expression, use its id (to identify the group)
        // in case of COUNT DISTINCT don't use the expression id to avoid possible duplicate IDs when COUNT and COUNT DISTINCT is used
        // in the same query
        if (!distinct() && field() instanceof NamedExpression) {
            functionId = ((NamedExpression) field()).id().toString();
        }
        return functionId;
    }

    @Override
    public AggregateFunctionAttribute toAttribute() {
        // COUNT(*) gets its value from the parent aggregation on which _count is called
        if (field() instanceof Literal) {
            return new AggregateFunctionAttribute(source(), name(), dataType(), id(), functionId(), id(), "_count");
        }
        // COUNT(column) gets its value from a sibling aggregation (an exists filter agg) by calling its id and then _count on it
        if (!distinct()) {
            return new AggregateFunctionAttribute(source(), name(), dataType(), id(), functionId(), id(), functionId() + "._count");
        }
        return super.toAttribute();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        Count other = (Count) obj;
        return Objects.equals(other.distinct(), distinct());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), distinct());
    }
}

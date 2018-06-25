/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import java.util.List;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Count the number of documents matched ({@code COUNT})
 * <strong>OR</strong> count the number of distinct values
 * for a field that matched ({@code COUNT(DISTINCT}.
 */
public class Count extends AggregateFunction {

    private final boolean distinct;

    public Count(Location location, Expression field, boolean distinct) {
        super(location, field);
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
        return new Count(location(), newChildren.get(0), distinct);
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
        if (field() instanceof NamedExpression) {
            functionId = ((NamedExpression) field()).id().toString();
        }
        return functionId;
    }

    @Override
    public AggregateFunctionAttribute toAttribute() {
        return new AggregateFunctionAttribute(location(), name(), dataType(), id(), functionId(), "_count");
    }
}

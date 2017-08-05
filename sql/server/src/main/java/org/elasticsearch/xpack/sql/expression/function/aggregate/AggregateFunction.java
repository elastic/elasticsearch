/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class AggregateFunction extends Function {

    private final Expression field;
    private final List<Expression> arguments;

    AggregateFunction(Location location, Expression field) {
        this(location, field, emptyList());
    }

    AggregateFunction(Location location, Expression field, List<Expression> arguments) {
        super(location, CollectionUtils.combine(singletonList(field), arguments));
        this.field = field;
        this.arguments = arguments;
    }

    public Expression field() {
        return field;
    }

    public List<Expression> arguments() {
        return arguments;
    }

    public String functionId() {
        return id().toString();
    }

    @Override
    public AggregateFunctionAttribute toAttribute() {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        return new AggregateFunctionAttribute(location(), name(), dataType(), id(), functionId(), null);
    }
}

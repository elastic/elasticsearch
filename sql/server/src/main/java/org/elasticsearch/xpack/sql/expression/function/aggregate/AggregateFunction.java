/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.singletonList;

public abstract class AggregateFunction extends Function {

    private final Expression argument;

    AggregateFunction(Location location, Expression child) {
        super(location, singletonList(child));
        this.argument = child;
    }

    public Expression argument() {
        return argument;
    }

    @Override
    public AggregateFunctionAttribute toAttribute() {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        return new AggregateFunctionAttribute(location(), name(), dataType(), id(), id().toString(), null);
    }
}

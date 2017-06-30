/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

public class Count extends AggregateFunction {

    private final boolean distinct;

    public Count(Location location, Expression argument, boolean distinct) {
        super(location, argument);
        this.distinct = distinct;
    }

    public boolean distinct() {
        return distinct;
    }

    @Override
    public DataType dataType() {
        return DataTypes.LONG;
    }


    @Override
    public String functionId() {
        String functionId = id().toString();
        // if count works against a given expression, use its id (to identify the group)
        if (argument() instanceof NamedExpression) {
            functionId = ((NamedExpression) argument()).id().toString();
        }
        return functionId;
    }

    @Override
    public AggregateFunctionAttribute toAttribute() {
        return new AggregateFunctionAttribute(location(), name(), dataType(), id(), functionId(), "_count");
    }
}

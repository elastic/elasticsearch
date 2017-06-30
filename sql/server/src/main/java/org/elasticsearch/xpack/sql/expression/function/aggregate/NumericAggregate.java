/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

abstract class NumericAggregate extends AggregateFunction {

    NumericAggregate(Location location, Expression argument) {
        super(location, argument);
    }

    @Override
    protected TypeResolution resolveType() {
        return argument().dataType().isNumeric() ? 
                    TypeResolution.TYPE_RESOLVED :
                    new TypeResolution("Function '%s' cannot be applied on a non-numeric expression ('%s' of type '%s')", functionName(), Expressions.name(argument()), argument().dataType().esName());
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }
}

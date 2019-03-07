/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isNumeric;

abstract class NumericAggregate extends AggregateFunction {

    NumericAggregate(Location location, Expression field, List<Expression> parameters) {
        super(location, field, parameters);
    }

    NumericAggregate(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected TypeResolution resolveType() {
        return isNumeric(field(), functionName(), ParamOrdinal.DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }
}

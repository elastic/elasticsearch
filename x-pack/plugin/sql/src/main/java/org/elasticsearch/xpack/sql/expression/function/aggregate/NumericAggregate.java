/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.sql.expression.TypeResolutions.isNumeric;

abstract class NumericAggregate extends AggregateFunction {

    NumericAggregate(Source source, Expression field, List<Expression> parameters) {
        super(source, field, parameters);
    }

    NumericAggregate(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveType() {
        return isNumeric(field(), sourceText(), ParamOrdinal.DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }
}

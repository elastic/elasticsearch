/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class NumericAggregate extends AggregateFunction {

    NumericAggregate(Source source, Expression field, List<Expression> parameters) {
        super(source, field, parameters);
    }

    NumericAggregate(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveType() {
        return isNumeric(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }
}

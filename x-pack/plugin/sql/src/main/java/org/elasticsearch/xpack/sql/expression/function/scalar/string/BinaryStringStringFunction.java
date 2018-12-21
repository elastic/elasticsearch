/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * A binary string function with two string parameters and a numeric result
 */
public abstract class BinaryStringStringFunction extends BinaryStringFunction<String, Number> {

    public BinaryStringStringFunction(Location location, Expression left, Expression right) {
        super(location, left, right);
    }

    @Override
    protected TypeResolution resolveSecondParameterInputType(Expression e) {
        return Expressions.typeMustBeString(e, functionName(), Expressions.ParamOrdinal.SECOND);
    }

    @Override
    public DataType dataType() {
        return DataType.INTEGER;
    }
}

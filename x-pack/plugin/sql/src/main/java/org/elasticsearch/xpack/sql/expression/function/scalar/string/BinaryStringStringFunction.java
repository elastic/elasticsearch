/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * A binary string function with two string parameters and a numeric result
 */
public abstract class BinaryStringStringFunction extends BinaryStringFunction<String, Number> {

    public BinaryStringStringFunction(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected TypeResolution resolveSecondParameterInputType(Expression e) {
        return isStringAndExact(e, sourceText(), Expressions.ParamOrdinal.SECOND);
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }
}

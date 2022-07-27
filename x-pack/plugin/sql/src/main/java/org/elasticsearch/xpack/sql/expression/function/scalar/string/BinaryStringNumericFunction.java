/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

/**
 * A binary string function with a numeric second parameter and a string result
 */
public abstract class BinaryStringNumericFunction extends BinaryStringFunction<Number, String> {

    public BinaryStringNumericFunction(Source source, Expression left, Expression right) {
        super(source, left, right);
    }

    @Override
    protected abstract BinaryStringNumericOperation operation();

    @Override
    protected TypeResolution resolveSecondParameterInputType(Expression e) {
        return isNumeric(e, sourceText(), SECOND);
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryStringNumericPipe(source(), this, Expressions.pipe(left()), Expressions.pipe(right()), operation());
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }
}

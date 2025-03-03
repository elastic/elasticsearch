/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash.HashFunction;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public abstract class AbstractHashFunction extends UnaryScalarFunction {

    protected AbstractHashFunction(Source source, Expression field) {
        super(source, field);
    }

    protected AbstractHashFunction(StreamInput in) throws IOException {
        super(in);
    }

    protected abstract HashFunction getHashFunction();

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(field, sourceText(), DEFAULT);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new HashConstantEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "hash"),
            new Function<>() {
                @Override
                public HashFunction apply(DriverContext context) {
                    return getHashFunction().copy();
                }

                @Override
                public String toString() {
                    return getHashFunction().toString();
                }
            },
            toEvaluator.apply(field)
        );
    }
}

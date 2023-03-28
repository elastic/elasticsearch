/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.function.Supplier;

public class Cast {
    /**
     * Build the evaluator supplier to cast {@code in} from {@code current} to {@code required}.
     */
    public static Supplier<EvalOperator.ExpressionEvaluator> cast(
        DataType current,
        DataType required,
        Supplier<EvalOperator.ExpressionEvaluator> in
    ) {
        if (current == required) {
            return in;
        }
        if (required == DataTypes.DOUBLE) {
            if (current == DataTypes.LONG) {
                return () -> new CastLongToDoubleEvaluator(in.get());
            }
            if (current == DataTypes.INTEGER) {
                return () -> new CastIntToDoubleEvaluator(in.get());
            }
            throw cantCast(current, required);
        }
        if (required == DataTypes.LONG) {
            if (current == DataTypes.INTEGER) {
                return () -> new CastIntToLongEvaluator(in.get());
            }
            throw cantCast(current, required);
        }
        throw cantCast(current, required);
    }

    private static UnsupportedOperationException cantCast(DataType current, DataType required) {
        return new UnsupportedOperationException("can't process [" + current.typeName() + " -> " + required.typeName() + "]");
    }

    @Evaluator(extraName = "IntToLong")
    static long castIntToLong(int v) {
        return v;
    }

    @Evaluator(extraName = "IntToDouble")
    static double castIntToDouble(int v) {
        return v;
    }

    @Evaluator(extraName = "LongToDouble")
    static double castLongToDouble(long v) {
        return v;
    }
}

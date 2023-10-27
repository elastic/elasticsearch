/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongToDouble;

public class Cast {
    /**
     * Build the evaluator supplier to cast {@code in} from {@code current} to {@code required}.
     */
    public static ExpressionEvaluator.Factory cast(DataType current, DataType required, ExpressionEvaluator.Factory in) {
        if (current == required) {
            return in;
        }
        if (current == DataTypes.NULL || required == DataTypes.NULL) {
            return EvalOperator.CONSTANT_NULL_FACTORY;
        }
        if (required == DataTypes.DOUBLE) {
            if (current == DataTypes.LONG) {
                return new CastLongToDoubleEvaluator.Factory(in);
            }
            if (current == DataTypes.INTEGER) {
                return new CastIntToDoubleEvaluator.Factory(in);
            }
            if (current == DataTypes.UNSIGNED_LONG) {
                return new CastUnsignedLongToDoubleEvaluator.Factory(in);
            }
            throw cantCast(current, required);
        }
        if (required == DataTypes.UNSIGNED_LONG) {
            if (current == DataTypes.LONG) {
                return new CastLongToUnsignedLongEvaluator.Factory(in);
            }
            if (current == DataTypes.INTEGER) {
                return new CastIntToUnsignedLongEvaluator.Factory(in);
            }
        }
        if (required == DataTypes.LONG) {
            if (current == DataTypes.INTEGER) {
                return new CastIntToLongEvaluator.Factory(in);
            }
            throw cantCast(current, required);
        }
        throw cantCast(current, required);
    }

    private static EsqlIllegalArgumentException cantCast(DataType current, DataType required) {
        return new EsqlIllegalArgumentException("can't process [" + current.typeName() + " -> " + required.typeName() + "]");
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

    @Evaluator(extraName = "UnsignedLongToDouble")
    static double castUnsignedLongToDouble(long v) {
        return unsignedLongToDouble(v);
    }

    @Evaluator(extraName = "IntToUnsignedLong")
    static long castIntToUnsignedLong(int v) {
        return castLongToUnsignedLong(v);
    }

    @Evaluator(extraName = "LongToUnsignedLong")
    static long castLongToUnsignedLong(long v) {
        if (v < 0) {
            throw new QlIllegalArgumentException("[" + v + "] out of [unsigned_long] range");
        }
        return v;
    }
}

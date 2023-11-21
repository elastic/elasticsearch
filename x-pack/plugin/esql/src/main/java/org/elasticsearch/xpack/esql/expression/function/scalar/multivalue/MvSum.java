/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongAddExact;

/**
 * Reduce a multivalued field to a single valued field containing the sum of all values.
 */
public class MvSum extends AbstractMultivalueFunction {
    public MvSum(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), t -> t.isNumeric() && isRepresentable(t), sourceText(), null, "numeric");
    }

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case DOUBLE -> new MvSumDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvSumIntEvaluator.Factory(source(), fieldEval);
            case LONG -> field().dataType() == DataTypes.UNSIGNED_LONG
                ? new MvSumUnsignedLongEvaluator.Factory(source(), fieldEval)
                : new MvSumLongEvaluator.Factory(source(), fieldEval);
            case NULL -> dvrCtx -> EvalOperator.CONSTANT_NULL;

            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvSum(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvSum::new, field());
    }

    @MvEvaluator(extraName = "Double", finish = "finish")
    public static void process(CompensatedSum sum, double v) {
        sum.add(v);
    }

    public static double finish(CompensatedSum sum) {
        double value = sum.value();
        sum.reset(0, 0);
        return value;
    }

    @MvEvaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
    static int process(int current, int v) {
        return Math.addExact(current, v);
    }

    @MvEvaluator(extraName = "Long", warnExceptions = { ArithmeticException.class })
    static long process(long current, long v) {
        return Math.addExact(current, v);
    }

    @MvEvaluator(extraName = "UnsignedLong", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLong(long current, long v) {
        return unsignedLongAddExact(current, v);
    }
}

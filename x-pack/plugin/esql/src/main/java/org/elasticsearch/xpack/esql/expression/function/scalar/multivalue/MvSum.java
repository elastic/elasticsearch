/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

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
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case DOUBLE -> () -> new MvSumDoubleEvaluator(fieldEval.get());
            case INT -> () -> new MvSumIntEvaluator(fieldEval.get());
            case LONG -> field().dataType() == DataTypes.UNSIGNED_LONG
                ? () -> new MvSumUnsignedLongEvaluator(fieldEval.get())
                : () -> new MvSumLongEvaluator(fieldEval.get());
            case NULL -> () -> EvalOperator.CONSTANT_NULL;
            default -> throw new UnsupportedOperationException("unsupported type [" + field().dataType() + "]");
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

    @MvEvaluator(extraName = "Int")
    static int process(int current, int v) {
        return current + v;
    }

    @MvEvaluator(extraName = "Long")
    static long process(long current, long v) {
        return current + v;
    }

    @MvEvaluator(extraName = "UnsignedLong")
    static long processUnsignedLong(long current, long v) {
        return Add.processUnsignedLongs(current, v);
    }
}

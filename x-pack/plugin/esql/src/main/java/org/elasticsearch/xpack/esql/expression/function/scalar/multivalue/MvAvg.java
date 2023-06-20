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
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

/**
 * Reduce a multivalued field to a single valued field containing the average value.
 */
public class MvAvg extends AbstractMultivalueFunction {
    public MvAvg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), t -> t.isNumeric() && isRepresentable(t), sourceText(), null, "numeric");
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> evaluator(Supplier<EvalOperator.ExpressionEvaluator> fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case DOUBLE -> () -> new MvAvgDoubleEvaluator(fieldEval.get());
            case INT -> () -> new MvAvgIntEvaluator(fieldEval.get());
            case LONG -> () -> new MvAvgLongEvaluator(fieldEval.get());
            case NULL -> () -> EvalOperator.CONSTANT_NULL;
            default -> throw new UnsupportedOperationException("unsupported type [" + field().dataType() + "]");
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvAvg(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvAvg::new, field());
    }

    @MvEvaluator(extraName = "Double", finish = "finish")
    public static void process(CompensatedSum sum, double v) {
        sum.add(v);
    }

    public static double finish(CompensatedSum sum, int valueCount) {
        double value = sum.value();
        sum.reset(0, 0);
        return value / valueCount;
    }

    @MvEvaluator(extraName = "Int", finish = "finish", single = "single")
    static int process(int current, int v) {
        return current + v;
    }

    static double finish(int sum, int valueCount) {
        return ((double) sum) / valueCount;
    }

    static double single(int value) {
        return value;
    }

    @MvEvaluator(extraName = "Long", finish = "finish", single = "single")
    static long process(long current, long v) {
        return current + v;
    }

    static double finish(long sum, int valueCount) {
        return ((double) sum) / valueCount;
    }

    static double single(long value) {
        return value;
    }

}

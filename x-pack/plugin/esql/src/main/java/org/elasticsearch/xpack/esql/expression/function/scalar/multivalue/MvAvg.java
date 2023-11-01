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
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongToDouble;

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
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (LocalExecutionPlanner.toElementType(field().dataType())) {
            case DOUBLE -> new MvAvgDoubleEvaluator.Factory(fieldEval);
            case INT -> new MvAvgIntEvaluator.Factory(fieldEval);
            case LONG -> field().dataType() == DataTypes.UNSIGNED_LONG
                ? new MvAvgUnsignedLongEvaluator.Factory(fieldEval)
                : new MvAvgLongEvaluator.Factory(fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
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
    static void process(CompensatedSum sum, int v) {
        sum.add(v);
    }

    static double single(int value) {
        return value;
    }

    @MvEvaluator(extraName = "Long", finish = "finish", single = "single")
    static void process(CompensatedSum sum, long v) {
        sum.add(v);
    }

    static double single(long value) {
        return value;
    }

    @MvEvaluator(extraName = "UnsignedLong", finish = "finish", single = "singleUnsignedLong")
    static void processUnsignedLong(CompensatedSum sum, long v) {
        sum.add(unsignedLongToDouble(v));
    }

    static double singleUnsignedLong(long value) {
        return unsignedLongToDouble(value);
    }
}

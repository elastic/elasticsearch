/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

/**
 * Converts from <a href="https://en.wikipedia.org/wiki/Radian">radians</a>
 * to <a href="https://en.wikipedia.org/wiki/Degree_(angle)">degrees</a>.
 */
public class ToDegrees extends AbstractConvertFunction implements EvaluatorMapper {
    private static final Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> EVALUATORS =
        Map.of(
            DOUBLE,
            ToDegreesEvaluator::new,
            INTEGER,
            (field, source) -> new ToDegreesEvaluator(new ToDoubleFromIntEvaluator(field, source), source),
            LONG,
            (field, source) -> new ToDegreesEvaluator(new ToDoubleFromLongEvaluator(field, source), source),
            UNSIGNED_LONG,
            (field, source) -> new ToDegreesEvaluator(new ToDoubleFromUnsignedLongEvaluator(field, source), source)
        );

    public ToDegrees(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BiFunction<EvalOperator.ExpressionEvaluator, Source, EvalOperator.ExpressionEvaluator>> evaluators() {
        return EVALUATORS;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToDegrees(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToDegrees::new, field());
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @ConvertEvaluator
    static double process(double deg) {
        return Math.toDegrees(deg);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

/**
 * Converts from <a href="https://en.wikipedia.org/wiki/Degree_(angle)">degrees</a>
 * to <a href="https://en.wikipedia.org/wiki/Radian">radians</a>.
 */
public class ToRadians extends AbstractConvertFunction implements EvaluatorMapper {
    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(DOUBLE, ToRadiansEvaluator.Factory::new),
        Map.entry(INTEGER, (field, source) -> new ToRadiansEvaluator.Factory(new ToDoubleFromIntEvaluator.Factory(field, source), source)),
        Map.entry(LONG, (field, source) -> new ToRadiansEvaluator.Factory(new ToDoubleFromLongEvaluator.Factory(field, source), source)),
        Map.entry(
            UNSIGNED_LONG,
            (field, source) -> new ToRadiansEvaluator.Factory(new ToDoubleFromUnsignedLongEvaluator.Factory(field, source), source)
        )
    );

    @FunctionInfo(returnType = "double", description = "Converts a number in degrees to radians.")
    public ToRadians(Source source, @Param(name = "number", type = { "double", "integer", "long", "unsigned_long" }) Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToRadians(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToRadians::new, field());
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @ConvertEvaluator
    static double process(double deg) {
        return Math.toRadians(deg);
    }
}

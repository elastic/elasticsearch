/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.MvEvaluator;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAddExact;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToInt;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToDouble;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isRepresentable;

/**
 * TODO
 */
public class MvRiemannZeta extends EsqlScalarFunction implements EvaluatorMapper {
    private final Expression field, p;

    @FunctionInfo(returnType = { "double" }, description = "TODO", examples = @Example(file = "math", tag = "mv_riemann_zeta"))
    public MvRiemannZeta(
        Source source,
        @Param(name = "number", type = { "double" }, description = "TODO") Expression field,
        @Param(name = "p", type = { "double" }, description = "TODO") Expression p
    ) {
        super(source, Arrays.asList(field, p));
        this.field = field;
        this.p = p;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(field, t -> t.isNumeric() && isRepresentable(t), sourceText(), FIRST, "numeric");

        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = TypeResolutions.isType(p, dt -> dt == DOUBLE, sourceText(), SECOND, "double");
        if (resolution.unresolved()) {
            return resolution;
        }

        return resolution;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && p.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        // TODO validate p > 0

        return switch (PlannerUtils.toElementType(field.dataType())) {
            case DOUBLE -> new MvRiemannZetaDoubleEvaluator.Factory(source(), toEvaluator.apply(field), toEvaluator.apply(p));
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;

            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvRiemannZeta(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvRiemannZeta::new, field, p);
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Evaluator(extraName = "Double", warnExceptions = { InvalidArgumentException.class })
    static void process(DoubleBlock.Builder builder, int position, DoubleBlock block, double p) {
        CompensatedSum sum = new CompensatedSum();
        int start = block.getFirstValueIndex(position);
        int end = block.getValueCount(position) + start;

        for (int i = start; i < end; i++) {
            double current_score = block.getDouble(i) / Math.pow(i + 1, p);
            sum.add(current_score);
        }
        builder.appendDouble(sum.value());
    }
}

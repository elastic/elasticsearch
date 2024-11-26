/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Reduce a multivalued field to a single valued field containing the weighted sum of all element applying the P series function.
 */
public class MvPSeriesWeightedSum extends EsqlScalarFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MvPSeriesWeightedSum",
        MvPSeriesWeightedSum::new
    );

    private final Expression field, p;

    @FunctionInfo(
        returnType = { "double" },

        description = "Converts a multivalued expression into a single-valued column by multiplying every "
            + "element on the input list by its corresponding term in P-Series and computing the sum.",
        examples = @Example(file = "mv_pseries_weighted_sum", tag = "example")
    )
    public MvPSeriesWeightedSum(
        Source source,
        @Param(name = "number", type = { "double" }, description = "Multivalue expression.") Expression field,
        @Param(
            name = "p",
            type = { "double" },
            description = "It is a constant number that represents the 'p' parameter in the P-Series. "
                + "It impacts every element's contribution to the weighted sum."
        ) Expression p
    ) {
        super(source, Arrays.asList(field, p));
        this.field = field;
        this.p = p;
    }

    private MvPSeriesWeightedSum(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = TypeResolutions.isType(field, dt -> dt == DOUBLE, sourceText(), FIRST, "double");
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = TypeResolutions.isType(p, dt -> dt == DOUBLE, sourceText(), SECOND, "double");
        if (resolution.unresolved()) {
            return resolution;
        }

        if (p.dataType() == NULL) {
            // If the type is `null` this parameter doesn't have to be foldable. It's effectively foldable anyway.
            // TODO figure out if the tests are wrong here, or if null is really different from foldable null
            return resolution;
        }

        return isFoldable(p, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return field.foldable() && p.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case DOUBLE -> new MvPSeriesWeightedSumDoubleEvaluator.Factory(
                source(),
                toEvaluator.apply(field),
                ctx -> new CompensatedSum(),
                (Double) p.fold()
            );
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvPSeriesWeightedSum(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvPSeriesWeightedSum::new, field, p);
    }

    @Override
    public DataType dataType() {
        if (p.dataType() == NULL) {
            return NULL;
        }
        return field.dataType();
    }

    @Evaluator(extraName = "Double", warnExceptions = ArithmeticException.class)
    static void process(
        DoubleBlock.Builder builder,
        int position,
        DoubleBlock block,
        @Fixed(includeInToString = false, build = true) CompensatedSum sum,
        @Fixed double p
    ) {
        sum.reset(0, 0);
        int start = block.getFirstValueIndex(position);
        int end = block.getValueCount(position) + start;

        for (int i = start; i < end; i++) {
            double current_score = block.getDouble(i) / Math.pow(i - start + 1, p);
            sum.add(current_score);
        }
        if (Double.isFinite(sum.value())) {
            builder.appendDouble(sum.value());
        } else {
            throw new ArithmeticException("double overflow");
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(p);
    }

    Expression field() {
        return field;
    }

    Expression p() {
        return p;
    }
}

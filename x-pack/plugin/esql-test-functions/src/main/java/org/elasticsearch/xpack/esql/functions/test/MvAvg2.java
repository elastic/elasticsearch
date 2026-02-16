/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.RuntimeMvEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.RuntimeEvaluatorSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;

/**
 * Test function for runtime MvEvaluator generation with single value optimization.
 * <p>
 * This function computes a simple sum and uses the {@code single} attribute
 * to optimize for single-valued fields.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW x = [1, 2, 3] | EVAL result = mv_avg2(x)
 * </pre>
 */
public class MvAvg2 extends AbstractMultivalueFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvAvg2", MvAvg2::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Computes sum with single value optimization (test function using runtime generation)."
    )
    public MvAvg2(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Multivalue expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private MvAvg2(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process methods with @RuntimeMvEvaluator ====================

    @RuntimeMvEvaluator(extraName = "Int", single = "singleInt")
    public static int processInt(int current, int next) {
        return current + next;
    }

    @RuntimeMvEvaluator(extraName = "Long", single = "singleLong")
    public static long processLong(long current, long next) {
        return current + next;
    }

    @RuntimeMvEvaluator(extraName = "Double", single = "singleDouble")
    public static double processDouble(double current, double next) {
        return current + next;
    }

    /**
     * Single value optimization for int - just return the value as-is.
     */
    public static int singleInt(int value) {
        return value;
    }

    /**
     * Single value optimization for long - just return the value as-is.
     */
    public static long singleLong(long value) {
        return value;
    }

    /**
     * Single value optimization for double - just return the value as-is.
     */
    public static double singleDouble(double value) {
        return value;
    }

    // ==================== Type resolution ====================

    @Override
    protected TypeResolution resolveFieldType() {
        return isType(field(), t -> t.isNumeric() && isRepresentable(t), sourceText(), null, "numeric");
    }

    // ==================== toEvaluator ====================

    @Override
    protected ExpressionEvaluator.Factory evaluator(ExpressionEvaluator.Factory fieldEval) {
        return switch (PlannerUtils.toElementType(field().dataType())) {
            case DOUBLE -> RuntimeEvaluatorSupport.createMvFactory(MvAvg2.class, DataType.DOUBLE, source(), fieldEval);
            case INT -> RuntimeEvaluatorSupport.createMvFactory(MvAvg2.class, DataType.INTEGER, source(), fieldEval);
            case LONG -> RuntimeEvaluatorSupport.createMvFactory(MvAvg2.class, DataType.LONG, source(), fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported data type: " + field().dataType());
        };
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvAvg2(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvAvg2::new, field());
    }
}

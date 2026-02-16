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
 * Test function for runtime MvEvaluator generation.
 * <p>
 * This function is equivalent to {@code MV_SUM()} but uses runtime bytecode
 * generation via {@link RuntimeMvEvaluator} instead of compile-time code
 * generation via {@code @MvEvaluator}.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW x = [1, 2, 3] | EVAL sum = mv_sum2(x)
 * </pre>
 */
public class MvSum2 extends AbstractMultivalueFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvSum2", MvSum2::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Converts a multivalued field into a single valued field containing the sum of all values (test function using runtime generation)."
    )
    public MvSum2(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Multivalue expression."
        ) Expression field
    ) {
        super(source, field);
    }

    private MvSum2(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process methods with @RuntimeMvEvaluator ====================

    @RuntimeMvEvaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
    public static int processInt(int current, int next) {
        return Math.addExact(current, next);
    }

    @RuntimeMvEvaluator(extraName = "Long", warnExceptions = { ArithmeticException.class })
    public static long processLong(long current, long next) {
        return Math.addExact(current, next);
    }

    @RuntimeMvEvaluator(extraName = "Double")
    public static double processDouble(double current, double next) {
        return current + next;
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
            case DOUBLE -> RuntimeEvaluatorSupport.createMvFactory(MvSum2.class, DataType.DOUBLE, source(), fieldEval);
            case INT -> RuntimeEvaluatorSupport.createMvFactory(MvSum2.class, DataType.INTEGER, source(), fieldEval);
            case LONG -> RuntimeEvaluatorSupport.createMvFactory(MvSum2.class, DataType.LONG, source(), fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported data type: " + field().dataType());
        };
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvSum2(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvSum2::new, field());
    }
}

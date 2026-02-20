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
 * Test function for runtime MvEvaluator generation with ascending optimization.
 * <p>
 * This function is equivalent to {@code MV_MAX()} but uses runtime bytecode
 * generation via {@link RuntimeMvEvaluator} with the {@code ascending} attribute
 * to optimize for sorted multivalued fields.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW x = [1, 2, 3] | EVAL max = mv_max2(x)
 * </pre>
 */
public class MvMax2 extends AbstractMultivalueFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MvMax2", MvMax2::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Returns the maximum value from a multivalued field "
            + "(test function using runtime generation with ascending optimization)."
    )
    public MvMax2(
        Source source,
        @Param(name = "number", type = { "double", "integer", "long" }, description = "Multivalue expression.") Expression field
    ) {
        super(source, field);
    }

    private MvMax2(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process methods with @RuntimeMvEvaluator ====================

    @RuntimeMvEvaluator(extraName = "Int", ascending = "ascendingIndex")
    public static int processInt(int current, int next) {
        return Math.max(current, next);
    }

    @RuntimeMvEvaluator(extraName = "Long", ascending = "ascendingIndex")
    public static long processLong(long current, long next) {
        return Math.max(current, next);
    }

    @RuntimeMvEvaluator(extraName = "Double", ascending = "ascendingIndex")
    public static double processDouble(double current, double next) {
        return Math.max(current, next);
    }

    /**
     * For ascending sorted values, max is always the last element.
     * @param valueCount the number of values
     * @return the index of the maximum value (valueCount - 1)
     */
    public static int ascendingIndex(int valueCount) {
        return valueCount - 1;
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
            case DOUBLE -> RuntimeEvaluatorSupport.createMvFactory(MvMax2.class, DataType.DOUBLE, source(), fieldEval);
            case INT -> RuntimeEvaluatorSupport.createMvFactory(MvMax2.class, DataType.INTEGER, source(), fieldEval);
            case LONG -> RuntimeEvaluatorSupport.createMvFactory(MvMax2.class, DataType.LONG, source(), fieldEval);
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw new IllegalArgumentException("Unsupported data type: " + field().dataType());
        };
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvMax2(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvMax2::new, field());
    }
}

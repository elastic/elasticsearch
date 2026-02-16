/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.RuntimeEvaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.RuntimeEvaluatorSupport;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Test binary function for runtime evaluator generation.
 * <p>
 * This function adds two numbers and uses runtime bytecode generation
 * via {@link RuntimeEvaluator} instead of compile-time code generation.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW a = 3, b = 4 | EVAL sum = add2(a, b)
 * </pre>
 */
public class Add2 extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Add2", Add2::new);

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Adds two numbers (test function for binary runtime generation)."
    )
    public Add2(
        Source source,
        @Param(name = "left", type = { "double", "integer", "long" }) Expression left,
        @Param(name = "right", type = { "double", "integer", "long" }) Expression right
    ) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    private Add2(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process methods with @RuntimeEvaluator ====================

    @RuntimeEvaluator(extraName = "Double", warnExceptions = { ArithmeticException.class })
    public static double processDouble(double left, double right) {
        return org.elasticsearch.xpack.esql.core.util.NumericUtils.asFiniteNumber(left + right);
    }

    @RuntimeEvaluator(extraName = "Long", warnExceptions = { ArithmeticException.class })
    public static long processLong(long left, long right) {
        return Math.addExact(left, right);
    }

    @RuntimeEvaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
    public static int processInt(int left, int right) {
        return Math.addExact(left, right);
    }

    // ==================== Accessors ====================

    public Expression left() {
        return left;
    }

    public Expression right() {
        return right;
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var leftFactory = toEvaluator.apply(left);
        var rightFactory = toEvaluator.apply(right);
        DataType type = dataType();

        // Use RuntimeEvaluatorSupport to create the factory with multiple field factories
        return RuntimeEvaluatorSupport.createFactory(Add2.class, type, source(), List.of(leftFactory, rightFactory));
    }

    // ==================== Type handling ====================

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        DataType leftType = left.dataType();
        DataType rightType = right.dataType();

        // Both must be numeric
        if (!isNumeric(leftType)) {
            return new TypeResolution("Expected numeric type for left but got [" + leftType.typeName() + "]");
        }
        if (!isNumeric(rightType)) {
            return new TypeResolution("Expected numeric type for right but got [" + rightType.typeName() + "]");
        }

        // Types must match (for simplicity in this test function)
        if (leftType != rightType) {
            return new TypeResolution(
                "Left and right types must match but got [" + leftType.typeName() + "] and [" + rightType.typeName() + "]"
            );
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    private boolean isNumeric(DataType type) {
        return type == INTEGER || type == LONG || type == DOUBLE;
    }

    @Override
    public DataType dataType() {
        return left.dataType();
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Add2(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Add2::new, left, right);
    }
}

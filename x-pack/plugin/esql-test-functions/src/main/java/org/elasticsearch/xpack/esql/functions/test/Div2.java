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
 * Test division function with warnExceptions support.
 * <p>
 * This function divides two numbers and demonstrates warnExceptions handling:
 * when division by zero occurs, instead of failing the query, it registers
 * a warning and returns null for that position.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW a = 10, b = 2 | EVAL result = div2(a, b)  -- returns 5
 * ROW a = 10, b = 0 | EVAL result = div2(a, b)  -- returns null with warning
 * </pre>
 */
public class Div2 extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Div2", Div2::new);

    private final Expression left;
    private final Expression right;

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Divides two numbers. Returns null with a warning on division by zero (test function for warnExceptions)."
    )
    public Div2(
        Source source,
        @Param(name = "left", type = { "double", "integer", "long" }) Expression left,
        @Param(name = "right", type = { "double", "integer", "long" }) Expression right
    ) {
        super(source, List.of(left, right));
        this.left = left;
        this.right = right;
    }

    private Div2(StreamInput in) throws IOException {
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
    // Note: warnExceptions = {ArithmeticException.class} catches division by zero

    @RuntimeEvaluator(extraName = "Double", warnExceptions = { ArithmeticException.class })
    public static double processDouble(double left, double right) {
        // Double division doesn't throw ArithmeticException, but we include it for consistency
        // Division by zero returns Infinity or NaN for doubles
        return left / right;
    }

    @RuntimeEvaluator(extraName = "Long", warnExceptions = { ArithmeticException.class })
    public static long processLong(long left, long right) {
        return left / right;  // Throws ArithmeticException on divide by zero
    }

    @RuntimeEvaluator(extraName = "Int", warnExceptions = { ArithmeticException.class })
    public static int processInt(int left, int right) {
        return left / right;  // Throws ArithmeticException on divide by zero
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
        return RuntimeEvaluatorSupport.createFactory(Div2.class, type, source(), List.of(leftFactory, rightFactory));
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
        return new Div2(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Div2::new, left, right);
    }
}

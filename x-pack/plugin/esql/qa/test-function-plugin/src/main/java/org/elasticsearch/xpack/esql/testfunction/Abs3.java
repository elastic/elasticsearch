/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.testfunction;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.RuntimeEvaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Test function for runtime evaluator generation (qa plugin version).
 * <p>
 * This function is equivalent to {@code ABS()} but uses runtime bytecode
 * generation via {@link RuntimeEvaluator} instead of compile-time code
 * generation via {@code @Evaluator}.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW x = -5 | EVAL y = abs3(x)
 * </pre>
 */
public class Abs3 extends UnaryScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Abs3", Abs3::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Returns the absolute value (qa test function using runtime generation)."
    )
    public Abs3(Source source, @Param(name = "number", type = { "double", "integer", "long", "unsigned_long" }) Expression n) {
        super(source, n);
    }

    private Abs3(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process methods with @RuntimeEvaluator ====================

    @RuntimeEvaluator(extraName = "Double")
    public static double processDouble(double fieldVal) {
        return Math.abs(fieldVal);
    }

    @RuntimeEvaluator(extraName = "Long")
    public static long processLong(long fieldVal) {
        return Math.absExact(fieldVal);
    }

    @RuntimeEvaluator(extraName = "Int")
    public static int processInt(int fieldVal) {
        return Math.absExact(fieldVal);
    }

    @RuntimeEvaluator(extraName = "UnsignedLong")
    public static long processUnsignedLong(long fieldVal) {
        // Unsigned long is already positive, return as-is
        return fieldVal;
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        DataType type = dataType();

        // Special case: unsigned long is already positive, just return the field
        if (type == UNSIGNED_LONG) {
            return field;
        }

        // Use RuntimeEvaluatorSupport to create the factory
        return org.elasticsearch.xpack.esql.expression.function.scalar.RuntimeEvaluatorSupport.createFactory(
            Abs3.class,
            type,
            source(),
            field
        );
    }

    // ==================== Type handling ====================

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        DataType type = field().dataType();
        if (type == INTEGER || type == LONG || type == UNSIGNED_LONG || type == DOUBLE) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution("Expected numeric type but got [" + type.typeName() + "]");
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Abs3(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Abs3::new, field());
    }
}

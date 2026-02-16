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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

/**
 * Test variadic function for runtime evaluator generation.
 * <p>
 * This function returns the greatest value among its arguments and uses runtime bytecode
 * generation via {@link RuntimeEvaluator} for variadic (array parameter) support.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW a = 1, b = 5, c = 3 | EVAL max = greatest2(a, b, c)
 * </pre>
 */
public class Greatest2 extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Greatest2",
        Greatest2::new
    );

    @FunctionInfo(
        returnType = { "double", "integer", "long" },
        description = "Returns the greatest value among its arguments (test function for variadic runtime generation)."
    )
    public Greatest2(
        Source source,
        @Param(name = "first", type = { "double", "integer", "long" }) Expression first,
        @Param(name = "rest", type = { "double", "integer", "long" }, optional = true) List<Expression> rest
    ) {
        super(source, concat(first, rest));
    }

    private static List<Expression> concat(Expression first, List<Expression> rest) {
        if (rest == null || rest.isEmpty()) {
            return List.of(first);
        }
        return java.util.stream.Stream.concat(java.util.stream.Stream.of(first), rest.stream()).collect(Collectors.toList());
    }

    private Greatest2(StreamInput in) throws IOException {
        super(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process methods with @RuntimeEvaluator ====================

    @RuntimeEvaluator(extraName = "Double")
    public static double processDouble(double[] values) {
        double max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    @RuntimeEvaluator(extraName = "Long")
    public static long processLong(long[] values) {
        long max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    @RuntimeEvaluator(extraName = "Int")
    public static int processInt(int[] values) {
        int max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<ExpressionEvaluator.Factory> factories = children().stream()
            .map(toEvaluator::apply)
            .collect(Collectors.toList());
        DataType type = dataType();

        // Use RuntimeEvaluatorSupport to create the variadic factory
        return RuntimeEvaluatorSupport.createVariadicFactory(Greatest2.class, type, source(), factories);
    }

    // ==================== Type handling ====================

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (children().isEmpty()) {
            return new TypeResolution("greatest2 requires at least one argument");
        }

        DataType firstType = children().get(0).dataType();
        if (!isNumeric(firstType)) {
            return new TypeResolution("Expected numeric type but got [" + firstType.typeName() + "]");
        }

        // All arguments must have the same type
        for (int i = 1; i < children().size(); i++) {
            DataType argType = children().get(i).dataType();
            if (argType != firstType) {
                return new TypeResolution(
                    "All arguments must have the same type but got [" + firstType.typeName() + "] and [" + argType.typeName() + "]"
                );
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    private boolean isNumeric(DataType type) {
        return type == INTEGER || type == LONG || type == DOUBLE;
    }

    @Override
    public DataType dataType() {
        return children().isEmpty() ? INTEGER : children().get(0).dataType();
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.isEmpty()) {
            throw new IllegalArgumentException("greatest2 requires at least one argument");
        }
        return new Greatest2(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Greatest2::new, children().get(0), children().subList(1, children().size()));
    }
}

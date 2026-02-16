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
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.RuntimeEvaluatorSupport;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Test function for zero-parameter runtime evaluator generation.
 * <p>
 * This function returns the mathematical constant π (pi) using runtime bytecode
 * generation via {@link RuntimeEvaluator}. Unlike the built-in PI() function
 * which is foldable (constant-folded at planning time), this function uses
 * runtime evaluation to test zero-parameter function support.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW x = pi2()
 * </pre>
 */
public class Pi2 extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Pi2", Pi2::new);

    @FunctionInfo(
        returnType = "double",
        description = "Returns the mathematical constant π (pi). Test function for zero-parameter runtime generation."
    )
    public Pi2(Source source) {
        super(source, List.of());  // Empty argument list
    }

    private Pi2(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process method with @RuntimeEvaluator ====================

    @RuntimeEvaluator(extraName = "Double")
    public static double processDouble() {
        return Math.PI;
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator toEvaluator) {
        // Use RuntimeEvaluatorSupport to create the factory with empty child factories list
        return RuntimeEvaluatorSupport.createFactory(
            Pi2.class,
            DataType.DOUBLE,
            source(),
            List.of()  // No child factories for zero-parameter function
        );
    }

    // ==================== Type handling ====================

    @Override
    protected TypeResolution resolveType() {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Pi2(source());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }
}

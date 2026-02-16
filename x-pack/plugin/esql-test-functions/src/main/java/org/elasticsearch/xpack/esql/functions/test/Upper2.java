/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.functions.test;

import org.apache.lucene.util.BytesRef;
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
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Test function for runtime evaluator generation with BytesRef (string) support.
 * <p>
 * This function converts a string to uppercase using runtime bytecode
 * generation via {@link RuntimeEvaluator}.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW s = "hello" | EVAL u = upper2(s)
 * </pre>
 */
public class Upper2 extends UnaryScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Upper2", Upper2::new);

    @FunctionInfo(returnType = { "keyword" }, description = "Converts string to uppercase (test function using runtime generation).")
    public Upper2(Source source, @Param(name = "str", type = { "keyword", "text" }) Expression field) {
        super(source, field);
    }

    private Upper2(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process method with @RuntimeEvaluator ====================

    @RuntimeEvaluator(extraName = "")
    public static BytesRef process(BytesRef input) {
        if (input == null) {
            return null;
        }
        return new BytesRef(input.utf8ToString().toUpperCase(Locale.ROOT));
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());

        // Use RuntimeEvaluatorSupport to create the factory
        return org.elasticsearch.xpack.esql.expression.function.scalar.RuntimeEvaluatorSupport.createFactory(
            Upper2.class,
            KEYWORD,  // Always return keyword
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
        if (type == KEYWORD || type == TEXT) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return new TypeResolution("Expected string type but got [" + type.typeName() + "]");
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Upper2(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Upper2::new, field());
    }
}

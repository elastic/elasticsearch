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

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Test variadic function for runtime evaluator generation with BytesRef (string) support.
 * <p>
 * This function concatenates all input strings into one result using runtime bytecode
 * generation via {@link RuntimeEvaluator} for variadic (array parameter) support.
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW a = "Hello", b = " ", c = "World" | EVAL result = concat2(a, b, c)
 * </pre>
 */
public class Concat2 extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Concat2",
        Concat2::new
    );

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Concatenates all input strings into one result (test function for variadic runtime generation with BytesRef)."
    )
    public Concat2(
        Source source,
        @Param(name = "first", type = { "keyword", "text" }) Expression first,
        @Param(name = "rest", type = { "keyword", "text" }, optional = true) List<Expression> rest
    ) {
        super(source, concat(first, rest));
    }

    private static List<Expression> concat(Expression first, List<Expression> rest) {
        if (rest == null || rest.isEmpty()) {
            return List.of(first);
        }
        return java.util.stream.Stream.concat(java.util.stream.Stream.of(first), rest.stream()).collect(Collectors.toList());
    }

    private Concat2(StreamInput in) throws IOException {
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

    // ==================== Process method with @RuntimeEvaluator ====================

    @RuntimeEvaluator(extraName = "")
    public static BytesRef process(BytesRef[] values) {
        int totalLength = 0;
        for (BytesRef v : values) {
            totalLength += v.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (BytesRef v : values) {
            System.arraycopy(v.bytes, v.offset, result, offset, v.length);
            offset += v.length;
        }
        return new BytesRef(result);
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<ExpressionEvaluator.Factory> factories = children().stream()
            .map(toEvaluator::apply)
            .collect(Collectors.toList());

        return RuntimeEvaluatorSupport.createVariadicFactory(Concat2.class, KEYWORD, source(), factories);
    }

    // ==================== Type handling ====================

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        if (children().isEmpty()) {
            return new TypeResolution("concat2 requires at least one argument");
        }

        for (int i = 0; i < children().size(); i++) {
            DataType argType = children().get(i).dataType();
            if (argType != KEYWORD && argType != TEXT && argType != DataType.NULL) {
                return new TypeResolution(
                    "Expected string type (keyword or text) but got [" + argType.typeName() + "] for argument " + (i + 1)
                );
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    // ==================== Node operations ====================

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.isEmpty()) {
            throw new IllegalArgumentException("concat2 requires at least one argument");
        }
        return new Concat2(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Concat2::new, children().get(0), children().subList(1, children().size()));
    }
}

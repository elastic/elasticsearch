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
import org.elasticsearch.compute.ann.RuntimeFixed;
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

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Test function for runtime evaluator generation with @RuntimeFixed parameter support.
 * <p>
 * This function returns the leftmost N characters of a string, where N is a fixed
 * parameter computed once at factory creation time (not per-row).
 * <p>
 * Example ES|QL usage:
 * <pre>
 * ROW s = "hello world" | EVAL result = left2(s, 5)
 * </pre>
 * <p>
 * This tests the @RuntimeFixed annotation support in the runtime evaluator generator.
 */
public class Left2 extends EsqlScalarFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Left2", Left2::new);

    private final Expression str;
    private final Expression length;

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Returns the leftmost N characters of a string (test function for @RuntimeFixed support)."
    )
    public Left2(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }) Expression str,
        @Param(name = "length", type = { "integer" }) Expression length
    ) {
        super(source, List.of(str, length));
        this.str = str;
        this.length = length;
    }

    private Left2(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
        out.writeNamedWriteable(length);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    // ==================== Process method with @RuntimeEvaluator and @RuntimeFixed ====================

    /**
     * Returns the leftmost N characters of the input string.
     *
     * @param str the input string
     * @param length the number of characters to return (fixed parameter)
     * @return the leftmost N characters, or the entire string if shorter than N
     */
    @RuntimeEvaluator(extraName = "")
    public static BytesRef process(BytesRef str, @RuntimeFixed int length) {
        if (str == null) {
            return null;
        }
        if (length <= 0) {
            return new BytesRef("");
        }

        // Convert to string to handle UTF-8 properly
        String s = str.utf8ToString();
        if (s.length() <= length) {
            return str;
        }
        return new BytesRef(s.substring(0, length));
    }

    // ==================== toEvaluator ====================

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // The str parameter is evaluated per-row
        var strFactory = toEvaluator.apply(str);

        // The length parameter is fixed - computed once
        // It must be foldable (constant) at this point
        if (!length.foldable()) {
            throw new IllegalArgumentException("left2 requires a constant length parameter");
        }
        int lengthValue = ((Number) length.fold(toEvaluator.foldCtx())).intValue();

        // Use RuntimeEvaluatorSupport with fixed values
        return RuntimeEvaluatorSupport.createFactoryWithFixed(
            Left2.class,
            KEYWORD,  // Always return keyword
            source(),
            List.of(strFactory),  // Evaluated parameters
            List.of(lengthValue)  // Fixed parameters
        );
    }

    // ==================== Type handling ====================

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        DataType strType = str.dataType();
        if (strType != KEYWORD && strType != TEXT) {
            return new TypeResolution("First argument must be a string type but got [" + strType.typeName() + "]");
        }

        DataType lengthType = length.dataType();
        if (lengthType != DataType.INTEGER) {
            return new TypeResolution("Second argument must be an integer but got [" + lengthType.typeName() + "]");
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
        return new Left2(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Left2::new, str, length);
    }
}

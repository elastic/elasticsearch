/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Removes leading and trailing whitespaces from a string.
 */
public final class Trim extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Trim", Trim::new);

    @FunctionInfo(
        returnType = { "keyword", "text" },
        description = "Removes leading and trailing whitespaces from a string.",
        examples = @Example(file = "string", tag = "trim")
    )
    public Trim(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression str
    ) {
        super(source, str);
    }

    private Trim(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }

        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        return new TrimEvaluator.Factory(source(), field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Trim(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Trim::new, field());
    }

    @Evaluator
    static BytesRef process(BytesRef val) {
        int offset = val.offset;
        UnicodeUtil.UTF8CodePoint codePoint = new UnicodeUtil.UTF8CodePoint();
        while (offset < val.offset + val.length) {
            codePoint = UnicodeUtil.codePointAt(val.bytes, offset, codePoint);
            if (Character.isWhitespace(codePoint.codePoint) == false) {
                break;
            }
            offset += codePoint.numBytes;
        }

        int end = offset;
        int i = offset;
        while (i < val.offset + val.length) {
            codePoint = UnicodeUtil.codePointAt(val.bytes, i, codePoint);
            if (Character.isWhitespace(codePoint.codePoint) == false) {
                end = i + codePoint.numBytes;
            }
            i += codePoint.numBytes;
        }

        return new BytesRef(val.bytes, offset, end - offset);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Escape non ASCII characters
 */
public final class ToAscii extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToAscii", ToAscii::new);

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Escape non ASCII characters.",
        examples = @Example(file = "string", tag = "to_ascii"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") }
    )
    public ToAscii(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression str
    ) {
        super(source, str);
    }

    private ToAscii(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var field = toEvaluator.apply(field());
        return new ToAsciiEvaluator.Factory(
            source(),
            field,
            context -> new BreakingBytesRefBuilder(context.breaker(), "to_ascii"),
            context -> new UnicodeUtil.UTF8CodePoint()
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToAscii(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToAscii::new, field());
    }

    @ConvertEvaluator
    static BytesRef process(
        BytesRef val,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) UnicodeUtil.UTF8CodePoint codePoint
    ) {
        // Pre-reserve at least as much as the input.
        scratch.grow(val.length);
        scratch.clear();

        // The second pass fills in the escaped values
        int offset = val.offset;
        while (offset < val.offset + val.length) {
            codePoint = UnicodeUtil.codePointAt(val.bytes, offset, codePoint);
            var code = codePoint.codePoint;

            BytesRef input = new BytesRef(val.bytes, offset, codePoint.numBytes);

            // Bump offset so continue can be used starting from this point
            offset += codePoint.numBytes;

            // Check for special ASCII control characters
            String escapeStr = switch (code) {
                case '\n' -> "\\\\n";
                case '\r' -> "\\\\r";
                case '\t' -> "\\\\t";
                case '\b' -> "\\\\b";
                case '\f' -> "\\\\f";
                case '\\' -> "\\\\\\\\";
                case '\'' -> "\\\\'";
                case '\"' -> "\\\\\"";
                default -> null;
            };

            // Printable ASCII characters (32-126) don't need escaping
            if (escapeStr == null && code >= 32 && code <= 126) {
                scratch.append(input);
                continue;
            }

            // For any other, we use escaped templates depending on the range
            if (escapeStr == null) {
                String formatStr;

                if (code < 128) {
                    formatStr = "\\\\x%02x";
                } else if (code <= 0xFF) {
                    // Use xHH for code points 128-255
                    formatStr = "\\\\x%02x";
                } else if (code <= 0xFFFF) {
                    // Use uHHHH for code points 256-65535
                    formatStr = "\\\\u%04x";
                } else {
                    // Use UHHHHHHHH for code points above 65535
                    formatStr = "\\\\U%08x";
                }

                escapeStr = Strings.format(formatStr, code);
            }

            scratch.append(new BytesRef(escapeStr));
        }

        return scratch.bytesRefView();
    }
}

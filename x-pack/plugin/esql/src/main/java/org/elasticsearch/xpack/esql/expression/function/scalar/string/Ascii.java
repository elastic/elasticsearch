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
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Escape non ASCII characters
 */
public final class Ascii extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Ascii", Ascii::new);

    @FunctionInfo(
        returnType = { "keyword" },
        description = "Escape non ASCII characters.",
        examples = @Example(file = "string", tag = "ascii")
    )

    public Ascii(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression str
    ) {
        super(source, str);
    }

    private Ascii(StreamInput in) throws IOException {
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
        return new AsciiEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "ascii"),
            field
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Ascii(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Ascii::new, field());
    }

    @Evaluator
    static BytesRef process(
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch,
        BytesRef val
    ) {
        UnicodeUtil.UTF8CodePoint codePoint = new UnicodeUtil.UTF8CodePoint();

        int finalSize = 0;

        /* A first iteration determines the total grow size. This is used to grow the scratch array
            just once which guarantees O(n) as worst case time complexity for the appending operation.
         */
        int offset = val.offset;
        while (offset < val.offset + val.length) {
            codePoint = UnicodeUtil.codePointAt(val.bytes, offset, codePoint);

            BytesRef input = new BytesRef(val.bytes, offset, codePoint.numBytes);
            var maybeEscaped = escapeCodePoint(codePoint);

            finalSize += maybeEscaped.orElse(input).length;

            offset += codePoint.numBytes;
        }

        scratch.grow(finalSize);
        scratch.clear();

        //The second pass fills in the escaped values
        offset = val.offset;
        while (offset < val.offset + val.length) {
            codePoint = UnicodeUtil.codePointAt(val.bytes, offset, codePoint);

            BytesRef input = new BytesRef(val.bytes, offset, codePoint.numBytes);
            var maybeEscaped = escapeCodePoint(codePoint);

            scratch.append(maybeEscaped.orElse(input));

            offset += codePoint.numBytes;
        }

        return scratch.bytesRefView();
    }


    private static Optional<BytesRef> escapeCodePoint(UnicodeUtil.UTF8CodePoint codePoint) {
        var code = codePoint.codePoint;

        // Printable ASCII characters (32-126) don't need escaping
        if (code >= 32 && code <= 126) {
            return Optional.empty();
        }

        // Handle special ASCII control characters
        String resultStr = switch (code) {
            case '\n' -> "\\\\n";
            case '\r' -> "\\\\r";
            case '\t' -> "\\\\t";
            case '\b' -> "\\\\b";
            case '\f' -> "\\\\f";
            case '\\' -> "\\\\\\";
            case '\'' -> "\\\\'";
            case '\"' -> "\\\\\"";
            default -> null;
        };

        if (resultStr != null) {
            return Optional.of(new BytesRef(resultStr));
        }

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

        resultStr = String.format(formatStr, code);

        return Optional.of(new BytesRef(resultStr));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class FromBase64 extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FromBase64",
        FromBase64::new
    );

    @FunctionInfo(
        returnType = "keyword",
        description = "Decode a base64 string.",
        detailedDescription = """
            Returns `null` and adds a warning header to the response if the decoded bytes are not
            well-formed UTF-8.
            """,
        examples = @Example(file = "string", tag = "from_base64")
    )
    public FromBase64(
        Source source,
        @Param(name = "string", type = { "keyword", "text" }, description = "A base64 string.") Expression string
    ) {
        super(source, string);
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    private FromBase64(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FromBase64(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FromBase64::new, field());
    }

    @Evaluator(warnExceptions = { IllegalArgumentException.class })
    static BytesRef process(BytesRef field, @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRefBuilder oScratch) {
        byte[] bytes = new byte[field.length];
        System.arraycopy(field.bytes, field.offset, bytes, 0, field.length);
        oScratch.grow(field.length);
        oScratch.clear();
        int decodedSize = Base64.getDecoder().decode(bytes, oScratch.bytes());
        if (isWellFormedUtf8(oScratch.bytes(), 0, decodedSize) == false) {
            throw new IllegalArgumentException("decoded value is not valid UTF-8, which is not supported yet");
        }
        return new BytesRef(oScratch.bytes(), 0, decodedSize);
    }

    /**
     * Allocation-free scan reporting whether {@code [off, off+len)} is well-formed UTF-8.
     * Inlined from the Utf8Sanitizer helper that does not exist on this release branch.
     */
    static boolean isWellFormedUtf8(byte[] bytes, int off, int len) {
        int i = off;
        int end = off + len;
        while (i < end) {
            int b0 = bytes[i] & 0xFF;
            if (b0 < 0x80) {
                i++;
                continue;
            }
            int consumed = utf8SequenceLength(bytes, i, end);
            if (consumed < 0) {
                return false;
            }
            i += consumed;
        }
        return true;
    }

    private static int utf8SequenceLength(byte[] bytes, int i, int end) {
        int b0 = bytes[i] & 0xFF;
        if (b0 < 0x80) {
            return 1;
        }
        if (b0 < 0xC2) {
            return -1;
        }
        if (b0 < 0xE0) {
            if (i + 1 >= end || isUtf8Cont(bytes[i + 1]) == false) {
                return -1;
            }
            return 2;
        }
        if (b0 < 0xF0) {
            int lo = (b0 == 0xE0) ? 0xA0 : 0x80;
            int hi = (b0 == 0xED) ? 0x9F : 0xBF;
            if (i + 1 >= end || inUtf8Range(bytes[i + 1], lo, hi) == false) {
                return -1;
            }
            if (i + 2 >= end || isUtf8Cont(bytes[i + 2]) == false) {
                return -2;
            }
            return 3;
        }
        if (b0 < 0xF5) {
            int lo = (b0 == 0xF0) ? 0x90 : 0x80;
            int hi = (b0 == 0xF4) ? 0x8F : 0xBF;
            if (i + 1 >= end || inUtf8Range(bytes[i + 1], lo, hi) == false) {
                return -1;
            }
            if (i + 2 >= end || isUtf8Cont(bytes[i + 2]) == false) {
                return -2;
            }
            if (i + 3 >= end || isUtf8Cont(bytes[i + 3]) == false) {
                return -3;
            }
            return 4;
        }
        return -1;
    }

    private static boolean isUtf8Cont(byte b) {
        return (b & 0xC0) == 0x80;
    }

    private static boolean inUtf8Range(byte b, int lo, int hi) {
        int v = b & 0xFF;
        return v >= lo && v <= hi;
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return switch (PlannerUtils.toElementType(field.dataType())) {
            case BYTES_REF -> new FromBase64Evaluator.Factory(source(), toEvaluator.apply(field), context -> new BytesRefBuilder());
            case NULL -> EvalOperator.CONSTANT_NULL_FACTORY;
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }
}

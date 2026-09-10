/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Internal scalar that encodes a string so that unsigned byte order of the result is
 * {@code facette/natsort} natural order (the comparator PromQL {@code sort_by_label} uses).
 * <p>
 * Digit runs are rewritten with a length prefix in {@code ['0','9']} so that a shorter number
 * precedes a longer one ({@code pod2} before {@code pod10}) with no upper bound on run length.
 * Non-digit runs are copied verbatim. A value with no digit byte is returned unchanged.
 * Numerically equal digit runs that differ only in leading zeros ({@code 007} vs {@code 7})
 * encode to the same key; empty input encodes to empty.
 */
public final class NaturalSortKey extends UnaryScalarFunction implements VersionedNamedWriteable, AnyNullIsNull {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NaturalSortKey",
        NaturalSortKey::new
    );

    public static final TransportVersion PROMQL_NATURAL_SORT_KEY = TransportVersion.fromName("promql_natural_sort_key");

    public NaturalSortKey(Source source, Expression field) {
        super(source, field);
    }

    private NaturalSortKey(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return PROMQL_NATURAL_SORT_KEY;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(field, sourceText(), DEFAULT);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new NaturalSortKey(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, NaturalSortKey::new, field);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new NaturalSortKeyEvaluator.Factory(
            source(),
            context -> new BreakingBytesRefBuilder(context.breaker(), "natural_sort_key"),
            toEvaluator.apply(field)
        );
    }

    @Evaluator
    static BytesRef process(@Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch, BytesRef v) {
        return encode(v, scratch);
    }

    /**
     * Encodes {@code v} so that unsigned byte comparison of the result matches natsort order.
     * Digit runs keep at least one digit after stripping leading zeros; the length prefix is
     * {@code (L-1)/9} copies of {@code '9'} followed by {@code '0' + ((L-1) % 9)}, then the
     * stripped digits. The returned {@link BytesRef} is either {@code v} (no digits) or a view
     * of {@code scratch} valid only until the next encode into the same builder.
     */
    static BytesRef encode(BytesRef v, BreakingBytesRefBuilder scratch) {
        byte[] bytes = v.bytes;
        int start = v.offset;
        int end = start + v.length;
        boolean hasDigit = false;
        for (int i = start; i < end; i++) {
            if (isDigit(bytes[i])) {
                hasDigit = true;
                break;
            }
        }
        if (hasDigit == false) {
            return v;
        }
        scratch.clear();
        scratch.grow(2 * v.length + 1);
        int i = start;
        while (i < end) {
            if (isDigit(bytes[i])) {
                int runEnd = i + 1;
                while (runEnd < end && isDigit(bytes[runEnd])) {
                    runEnd++;
                }
                int dStart = i;
                while (dStart < runEnd - 1 && bytes[dStart] == '0') {
                    dStart++;
                }
                int length = runEnd - dStart;
                int nines = (length - 1) / 9;
                for (int n = 0; n < nines; n++) {
                    scratch.append((byte) '9');
                }
                scratch.append((byte) ('0' + ((length - 1) % 9)));
                scratch.append(bytes, dStart, length);
                i = runEnd;
            } else {
                int textStart = i;
                i++;
                while (i < end && isDigit(bytes[i]) == false) {
                    i++;
                }
                scratch.append(bytes, textStart, i - textStart);
            }
        }
        return scratch.bytesRefView();
    }

    private static boolean isDigit(byte b) {
        return b >= '0' && b <= '9';
    }
}

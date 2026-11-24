/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.LiteralsEvaluator;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Literal or constant.
 */
public class Literal extends LeafExpression implements Accountable, EvaluatorMapper {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Literal.class);

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Literal",
        Literal::readFrom
    );

    public static final Literal TRUE = new Literal(Source.EMPTY, Boolean.TRUE, DataType.BOOLEAN);
    public static final Literal FALSE = new Literal(Source.EMPTY, Boolean.FALSE, DataType.BOOLEAN);
    public static final Literal NULL = new Literal(Source.EMPTY, null, DataType.NULL);

    /**
     * The value as it was written, including lists with a single value.
     * This is the one that is compared, serialized and printed.
     */
    private final Object value;
    /**
     * {@link #value} with single-value lists unwrapped. This is the one that is given to the code that inspects the literal.
     */
    private final Object unwrappedValue;
    private final DataType dataType;

    public Literal(Source source, Object value, DataType dataType) {
        super(source);
        assert noPlainStrings(value, dataType);
        this.dataType = dataType;
        this.value = value;
        this.unwrappedValue = unwrapSingleValue(value);
    }

    /**
     * A list with a single value is that value. In ES|QL {@code ["a"]} and {@code "a"} are the same: once they are in a block
     * there's no difference between a multivalue with one element and a single value. But params and array literals are
     * parsed as lists, so this is where they become the single value that any function expects.
     */
    private static Object unwrapSingleValue(Object value) {
        if (value instanceof List<?> list && list.size() == 1) {
            return list.getFirst();
        }
        return value;
    }

    private boolean noPlainStrings(Object value, DataType dataType) {
        if (dataType == KEYWORD || dataType == TEXT || dataType == VERSION) {
            if (value == null) {
                return true;
            }
            return switch (value) {
                case String s -> false;
                case Collection<?> c -> c.stream().allMatch(x -> noPlainStrings(x, dataType));
                default -> true;
            };
        }
        return true;
    }

    private static Literal readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Object value = in.readGenericValue();
        DataType dataType = DataType.readFrom(in);
        return new Literal(source, value, dataType);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeGenericValue(value);
        dataType.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Literal> info() {
        return NodeInfo.create(this, Literal::new, value, dataType);
    }

    /**
     * The value of this literal: a single value, a list for multivalues, or {@code null}.
     * A list with a single value is returned as that value, as {@link #fold} does.
     * <p>
     *     Use {@link #rawValue()} only when the code must tell {@code ["a"]} from {@code "a"}, like parse-time
     *     validations of what the user wrote.
     * </p>
     */
    public Object value() {
        return unwrappedValue;
    }

    /**
     * The value as it was written. Unlike {@link #value()}, a list with a single value is returned as a list.
     * Only meant for code that has to validate or print the original shape of the value. Anything that uses the value
     * must use {@link #value()} or {@link #fold}, which treat {@code ["a"]} and {@code "a"} the same.
     */
    public Object rawValue() {
        return value;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public Nullability nullable() {
        return value == null ? Nullability.TRUE : Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public Object fold(FoldContext ctx) {
        return unwrappedValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataType, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Literal other = (Literal) obj;
        return Objects.equals(value, other.value) && Objects.equals(dataType, other.dataType);
    }

    @Override
    public String toString() {
        return toString(NodeStringFormat.LIMITED);
    }

    @Override
    public String toString(NodeStringFormat format) {
        // The identity mapper holds the canonical raw rendering (KEYWORD/TEXT/VERSION handling,
        // geo-safe fallback); truncation is a presentation concern applied here.
        return truncate(NodeStringMapper.IDENTITY.literal(value, dataType), format);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // The mapper renders the value: raw under IDENTITY, an interned token under anonymization.
        // 500-char LIMITED truncation is applied uniformly (tokens are short, so it is a no-op for
        // them) — no identity branch. Caller-visible shape: <value>[<type>].
        sb.append(truncate(mapper.literal(value, dataType), format)).append('[').append(dataType).append(']');
    }

    private static String truncate(String str, NodeStringFormat format) {
        if (str.length() > 500 && format == NodeStringFormat.LIMITED) {
            return str.substring(0, 500) + "...";
        }
        return str;
    }

    @Override
    public long ramBytesUsed() {
        long ramBytesUsed = BASE_RAM_BYTES_USED;
        if (value instanceof BytesRef b) {
            ramBytesUsed += b.length;
        } else {
            ramBytesUsed += RamUsageEstimator.sizeOfObject(value);
        }
        return ramBytesUsed;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new LiteralsEvaluator.Factory(this);
    }

    /**
     * Utility method for creating a literal out of a foldable expression.
     * Throws an exception if the expression is not foldable.
     */
    public static Literal of(FoldContext ctx, Expression foldable) {
        if (foldable.foldable() == false) {
            throw new QlIllegalArgumentException("Foldable expression required for Literal creation; received unfoldable " + foldable);
        }

        if (foldable instanceof Literal) {
            return (Literal) foldable;
        }

        return new Literal(foldable.source(), foldable.fold(ctx), foldable.dataType());
    }

    public static Literal of(Expression source, Object value) {
        return new Literal(source.source(), value, source.dataType());
    }

    public static Literal keyword(Source source, String literal) {
        return new Literal(source, BytesRefs.toBytesRef(literal), KEYWORD);
    }

    public static Literal text(Source source, String literal) {
        return new Literal(source, BytesRefs.toBytesRef(literal), TEXT);
    }

    public static Literal timeDuration(Source source, Duration literal) {
        return new Literal(source, literal, DataType.TIME_DURATION);
    }

    public static Literal dateTime(Source source, Instant literal) {
        return new Literal(source, literal.toEpochMilli(), DataType.DATETIME);
    }

    public static Literal integer(Source source, Integer literal) {
        return new Literal(source, literal, INTEGER);
    }

    public static Literal fromDouble(Source source, Double literal) {
        return new Literal(source, literal, DOUBLE);
    }

    public static Literal fromLong(Source source, Long literal) {
        return new Literal(source, literal, LONG);
    }

    public static Expression fromBoolean(Source source, Boolean literal) {
        return new Literal(source, literal, DataType.BOOLEAN);
    }

    private static BytesRef longAsWKB(DataType dataType, long encoded) {
        return dataType == GEO_POINT ? GEO.longAsWkb(encoded) : CARTESIAN.longAsWkb(encoded);
    }

    private static long wkbAsLong(DataType dataType, BytesRef wkb) {
        return dataType == GEO_POINT ? GEO.wkbAsLong(wkb) : CARTESIAN.wkbAsLong(wkb);
    }
}

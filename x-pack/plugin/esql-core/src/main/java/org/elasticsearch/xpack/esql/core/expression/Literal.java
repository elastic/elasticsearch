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
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
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
public class Literal extends LeafExpression implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Literal.class);

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Literal",
        Literal::readFrom
    );

    public static final Literal TRUE = new Literal(Source.EMPTY, Boolean.TRUE, DataType.BOOLEAN);
    public static final Literal FALSE = new Literal(Source.EMPTY, Boolean.FALSE, DataType.BOOLEAN);
    public static final Literal NULL = new Literal(Source.EMPTY, null, DataType.NULL);

    private final Object value;
    private final DataType dataType;

    public Literal(Source source, Object value, DataType dataType) {
        super(source);
        assert noPlainStrings(value, dataType);
        this.dataType = dataType;
        this.value = value;
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
        Source source = Source.readFrom((StreamInput & PlanStreamInput) in);
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

    public Object value() {
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
        return value;
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
        String str;
        if (dataType == KEYWORD || dataType == TEXT) {
            str = BytesRefs.toString(value);
        } else if (dataType == VERSION && value instanceof BytesRef br) {
            str = new Version(br).toString();
            // TODO review how we manage IPs: https://github.com/elastic/elasticsearch/issues/129605
            // } else if (dataType == IP && value instanceof BytesRef ip) {
            // str = DocValueFormat.IP.format(ip);
        } else {
            str = String.valueOf(value);
        }

        if (str == null) {
            str = "null";
        }
        if (str.length() > 500) {
            return str.substring(0, 500) + "...";
        }
        return str;
    }

    @Override
    public String nodeString() {
        return toString() + "[" + dataType + "]";
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
        return new Literal(source, literal, DataType.DATETIME);
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

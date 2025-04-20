/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

/**
 * Literal or constant.
 */
public class Literal extends LeafExpression {
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
        this.dataType = dataType;
        this.value = value;
    }

    private static Literal readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((StreamInput & PlanStreamInput) in);
        Object value = in.readGenericValue();
        DataType dataType = DataType.readFrom(in);
        return new Literal(source, mapToLiteralValue(in, dataType, value), dataType);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeGenericValue(mapFromLiteralValue(out, dataType, value));
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
        String str = String.valueOf(value);
        if (str.length() > 500) {
            return str.substring(0, 500) + "...";
        }
        return str;
    }

    @Override
    public String nodeString() {
        return toString() + "[" + dataType + "]";
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

    /**
     * Not all literal values are currently supported in StreamInput/StreamOutput as generic values.
     * This mapper allows for addition of new and interesting values without (yet) adding to StreamInput/Output.
     * This makes the most sense during the pre-GA version of ESQL. When we get near GA we might want to push this down.
     * <p>
     * For the spatial point type support we need to care about the fact that 8.12.0 uses encoded longs for serializing
     * while 8.13 uses WKB.
     */
    private static Object mapFromLiteralValue(StreamOutput out, DataType dataType, Object value) {
        if (dataType == GEO_POINT || dataType == CARTESIAN_POINT) {
            // In 8.12.0 we serialized point literals as encoded longs, but now use WKB
            if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
                if (value instanceof List<?> list) {
                    return list.stream().map(v -> mapFromLiteralValue(out, dataType, v)).toList();
                }
                return wkbAsLong(dataType, (BytesRef) value);
            }
        }
        return value;
    }

    /**
     * Not all literal values are currently supported in StreamInput/StreamOutput as generic values.
     * This mapper allows for addition of new and interesting values without (yet) changing StreamInput/Output.
     */
    private static Object mapToLiteralValue(StreamInput in, DataType dataType, Object value) {
        if (dataType == GEO_POINT || dataType == CARTESIAN_POINT) {
            // In 8.12.0 we serialized point literals as encoded longs, but now use WKB
            if (in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
                if (value instanceof List<?> list) {
                    return list.stream().map(v -> mapToLiteralValue(in, dataType, v)).toList();
                }
                return longAsWKB(dataType, (Long) value);
            }
        }
        return value;
    }

    private static BytesRef longAsWKB(DataType dataType, long encoded) {
        return dataType == GEO_POINT ? GEO.longAsWkb(encoded) : CARTESIAN.longAsWkb(encoded);
    }

    private static long wkbAsLong(DataType dataType, BytesRef wkb) {
        return dataType == GEO_POINT ? GEO.wkbAsLong(wkb) : CARTESIAN.wkbAsLong(wkb);
    }
}

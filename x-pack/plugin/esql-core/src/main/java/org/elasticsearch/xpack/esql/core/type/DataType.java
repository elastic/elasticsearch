/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public enum DataType {
    UNSUPPORTED(builder().typeName("UNSUPPORTED")),
    NULL(builder().esType("null")),
    BOOLEAN(builder().esType("boolean").size(1)),

    /**
     * These are numeric fields labeled as metric counters in time-series indices. Although stored
     * internally as numeric fields, they represent cumulative metrics and must not be treated as regular
     * numeric fields. Therefore, we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate aggregation, and casting to their
     * parent numeric type.
     */
    COUNTER_LONG(builder().esType("counter_long").size(Long.BYTES).docValues().counter()),
    COUNTER_INTEGER(builder().esType("counter_integer").size(Integer.BYTES).docValues().counter()),
    COUNTER_DOUBLE(builder().esType("counter_double").size(Double.BYTES).docValues().counter()),

    LONG(builder().esType("long").size(Long.BYTES).integer().docValues().counter(COUNTER_LONG)),
    INTEGER(builder().esType("integer").size(Integer.BYTES).integer().docValues().counter(COUNTER_INTEGER)),
    SHORT(builder().esType("short").size(Short.BYTES).integer().docValues().widenSmallNumeric(INTEGER)),
    BYTE(builder().esType("byte").size(Byte.BYTES).integer().docValues().widenSmallNumeric(INTEGER)),
    UNSIGNED_LONG(builder().esType("unsigned_long").size(Long.BYTES).integer().docValues()),
    DOUBLE(builder().esType("double").size(Double.BYTES).rational().docValues().counter(COUNTER_DOUBLE)),
    FLOAT(builder().esType("float").size(Float.BYTES).rational().docValues().widenSmallNumeric(DOUBLE)),
    HALF_FLOAT(builder().esType("half_float").size(Float.BYTES).rational().docValues().widenSmallNumeric(DOUBLE)),
    SCALED_FLOAT(builder().esType("scaled_float").size(Long.BYTES).rational().docValues().widenSmallNumeric(DOUBLE)),

    KEYWORD(builder().esType("keyword").unknownSize().docValues()),
    TEXT(builder().esType("text").unknownSize()),
    DATETIME(builder().esType("date").typeName("DATETIME").size(Long.BYTES).docValues()),
    IP(builder().esType("ip").size(45).docValues()),
    VERSION(builder().esType("version").unknownSize().docValues()),
    OBJECT(builder().esType("object")),
    NESTED(builder().esType("nested")),
    SOURCE(builder().esType(SourceFieldMapper.NAME).unknownSize()),
    DATE_PERIOD(builder().typeName("DATE_PERIOD").size(3 * Integer.BYTES)),
    TIME_DURATION(builder().typeName("TIME_DURATION").size(Integer.BYTES + Long.BYTES)),
    GEO_POINT(builder().esType("geo_point").size(Double.BYTES * 2).docValues()),
    CARTESIAN_POINT(builder().esType("cartesian_point").size(Double.BYTES * 2).docValues()),
    CARTESIAN_SHAPE(builder().esType("cartesian_shape").unknownSize().docValues()),
    GEO_SHAPE(builder().esType("geo_shape").unknownSize().docValues()),

    DOC_DATA_TYPE(builder().esType("_doc").size(Integer.BYTES * 3)),
    TSID_DATA_TYPE(builder().esType("_tsid").unknownSize().docValues()),
    PARTIAL_AGG(builder().esType("partial_agg").unknownSize());

    private final String typeName;

    private final String name;

    private final String esType;

    private final int size;

    /**
     * True if the type represents an integer number
     */
    private final boolean isInteger;

    /**
     * True if the type represents a rational number
     */
    private final boolean isRational;

    /**
     * True if the type supports doc values by default
     */
    private final boolean docValues;

    /**
     * {@code true} if this is a TSDB counter, {@code false} otherwise.
     */
    private final boolean isCounter;

    /**
     * If this is a "small" numeric type this contains the type ESQL will
     * widen it into, otherwise this is {@code null}.
     */
    private final DataType widenSmallNumeric;

    /**
     * If this is a representable numeric this will be the counter "version"
     * of this numeric, otherwise this is {@code null}.
     */
    private final DataType counter;

    DataType(Builder builder) {
        String typeString = builder.typeName != null ? builder.typeName : builder.esType;
        this.typeName = typeString.toLowerCase(Locale.ROOT);
        this.name = typeString.toUpperCase(Locale.ROOT);
        this.esType = builder.esType;
        this.size = builder.size;
        this.isInteger = builder.isInteger;
        this.isRational = builder.isRational;
        this.docValues = builder.docValues;
        this.isCounter = builder.isCounter;
        this.widenSmallNumeric = builder.widenSmallNumeric;
        this.counter = builder.counter;
    }

    private static final Collection<DataType> TYPES = Arrays.stream(values())
        .filter(d -> d != DOC_DATA_TYPE && d != TSID_DATA_TYPE)
        .sorted(Comparator.comparing(DataType::typeName))
        .toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static final Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        // TODO: Why don't we use the names ES uses as the esType field for these?
        // ES calls this 'point', but ESQL calls it 'cartesian_point'
        map.put("point", DataType.CARTESIAN_POINT);
        map.put("shape", DataType.CARTESIAN_SHAPE);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private static final Map<String, DataType> NAME_OR_ALIAS_TO_TYPE;
    static {
        Map<String, DataType> map = DataType.types().stream().collect(toMap(DataType::typeName, Function.identity()));
        map.put("bool", BOOLEAN);
        map.put("int", INTEGER);
        map.put("string", KEYWORD);
        NAME_OR_ALIAS_TO_TYPE = Collections.unmodifiableMap(map);
    }

    public static Collection<DataType> types() {
        return TYPES;
    }

    public static DataType fromTypeName(String name) {
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.ROOT));
    }

    public static DataType fromEs(String name) {
        DataType type = ES_TO_TYPE.get(name);
        return type != null ? type : UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof BigInteger) {
            return UNSIGNED_LONG;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof Byte) {
            return BYTE;
        }
        if (value instanceof Short) {
            return SHORT;
        }
        if (value instanceof ZonedDateTime) {
            return DATETIME;
        }
        if (value instanceof String || value instanceof Character || value instanceof BytesRef) {
            return KEYWORD;
        }

        return null;
    }

    public static boolean isUnsupported(DataType from) {
        return from == UNSUPPORTED;
    }

    public static boolean isString(DataType t) {
        return t == KEYWORD || t == TEXT;
    }

    public static boolean isPrimitive(DataType t) {
        return t != OBJECT && t != NESTED && t != UNSUPPORTED;
    }

    public static boolean isNull(DataType t) {
        return t == NULL;
    }

    public static boolean isNullOrNumeric(DataType t) {
        return t.isNumeric() || isNull(t);
    }

    public static boolean isSigned(DataType t) {
        return t.isNumeric() && t.equals(UNSIGNED_LONG) == false;
    }

    public static boolean isDateTime(DataType type) {
        return type == DATETIME;
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL)
                || (isString(left) && isString(right))
                || (left.isNumeric() && right.isNumeric())
                || (isDateTime(left) && isDateTime(right));
        }
    }

    public String nameUpper() {
        return name;
    }

    public String typeName() {
        return typeName;
    }

    public String esType() {
        return esType;
    }

    public String outputType() {
        return esType == null ? "unsupported" : esType;
    }

    public boolean isInteger() {
        return isInteger;
    }

    public boolean isRational() {
        return isRational;
    }

    public boolean isNumeric() {
        return isInteger || isRational;
    }

    public int size() {
        return size;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    /**
     * {@code true} if this is a TSDB counter, {@code false} otherwise.
     */
    public boolean isCounter() {
        return isCounter;
    }

    /**
     * If this is a "small" numeric type this contains the type ESQL will
     * widen it into, otherwise this returns {@code this}.
     */
    public DataType widenSmallNumeric() {
        return widenSmallNumeric == null ? this : widenSmallNumeric;
    }

    /**
     * If this is a representable numeric this will be the counter "version"
     * of this numeric, otherwise this is {@code null}.
     */
    public DataType counter() {
        return counter;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(typeName);
    }

    public static DataType readFrom(StreamInput in) throws IOException {
        // TODO: Use our normal enum serialization pattern
        String name = in.readString();
        if (name.equalsIgnoreCase(DataType.DOC_DATA_TYPE.nameUpper())) {
            return DataType.DOC_DATA_TYPE;
        }
        DataType dataType = DataType.fromTypeName(name);
        if (dataType == null) {
            throw new IOException("Unknown DataType for type name: " + name);
        }
        return dataType;
    }

    public static Set<String> namesAndAliases() {
        return NAME_OR_ALIAS_TO_TYPE.keySet();
    }

    public static DataType fromNameOrAlias(String typeName) {
        DataType type = NAME_OR_ALIAS_TO_TYPE.get(typeName.toLowerCase(Locale.ROOT));
        return type != null ? type : UNSUPPORTED;
    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * Named parameters with default values. It's just easier to do this with
     * a builder in java....
     */
    private static class Builder {
        private String esType;

        private String typeName;

        private int size;

        /**
         * True if the type represents an integer number
         */
        private boolean isInteger;

        /**
         * True if the type represents a rational number
         */
        private boolean isRational;

        /**
         * True if the type supports doc values by default
         */
        private boolean docValues;

        /**
         * {@code true} if this is a TSDB counter, {@code false} otherwise.
         */
        private boolean isCounter;

        /**
         * If this is a "small" numeric type this contains the type ESQL will
         * widen it into, otherwise this is {@code null}.
         */
        private DataType widenSmallNumeric;

        /**
         * If this is a representable numeric this will be the counter "version"
         * of this numeric, otherwise this is {@code null}.
         */
        private DataType counter;

        Builder() {}

        Builder esType(String esType) {
            this.esType = esType;
            return this;
        }

        Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        Builder size(int size) {
            this.size = size;
            return this;
        }

        Builder unknownSize() {
            this.size = Integer.MAX_VALUE;
            return this;
        }

        Builder integer() {
            this.isInteger = true;
            return this;
        }

        Builder rational() {
            this.isRational = true;
            return this;
        }

        Builder docValues() {
            this.docValues = true;
            return this;
        }

        Builder counter() {
            this.isCounter = true;
            return this;
        }

        Builder widenSmallNumeric(DataType widenSmallNumeric) {
            this.widenSmallNumeric = widenSmallNumeric;
            return this;
        }

        Builder counter(DataType counter) {
            assert counter.isCounter;
            this.counter = counter;
            return this;
        }
    }
}

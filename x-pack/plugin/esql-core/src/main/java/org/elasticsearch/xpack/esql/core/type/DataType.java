/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public enum DataType {
    UNSUPPORTED("UNSUPPORTED", null, 0, false, false, false),
    NULL("null", 0, false, false, false),
    BOOLEAN("boolean", 1, false, false, false),
    BYTE("byte", Byte.BYTES, true, false, true),
    SHORT("short", Short.BYTES, true, false, true),
    INTEGER("integer", Integer.BYTES, true, false, true),
    LONG("long", Long.BYTES, true, false, true),
    UNSIGNED_LONG("unsigned_long", Long.BYTES, true, false, true),
    DOUBLE("double", Double.BYTES, false, true, true),
    FLOAT("float", Float.BYTES, false, true, true),
    HALF_FLOAT("half_float", Float.BYTES, false, true, true),
    SCALED_FLOAT("scaled_float", Long.BYTES, false, true, true),
    KEYWORD("keyword", Integer.MAX_VALUE, false, false, true),
    TEXT("text", Integer.MAX_VALUE, false, false, false),
    DATETIME("DATETIME", "date", Long.BYTES, false, false, true),
    IP("ip", 45, false, false, true),
    VERSION("version", Integer.MAX_VALUE, false, false, true),
    OBJECT("object", 0, false, false, false),
    NESTED("nested", 0, false, false, false),
    SOURCE(SourceFieldMapper.NAME, SourceFieldMapper.NAME, Integer.MAX_VALUE, false, false, false),
    DATE_PERIOD("DATE_PERIOD", null, 3 * Integer.BYTES, false, false, false),
    TIME_DURATION("TIME_DURATION", null, Integer.BYTES + Long.BYTES, false, false, false),
    GEO_POINT("geo_point", Double.BYTES * 2, false, false, true),
    CARTESIAN_POINT("cartesian_point", Double.BYTES * 2, false, false, true),
    CARTESIAN_SHAPE("cartesian_shape", Integer.MAX_VALUE, false, false, true),
    GEO_SHAPE("geo_shape", Integer.MAX_VALUE, false, false, true),

    /**
     * These are numeric fields labeled as metric counters in time-series indices. Although stored
     * internally as numeric fields, they represent cumulative metrics and must not be treated as regular
     * numeric fields. Therefore, we define them differently and separately from their parent numeric field.
     * These fields are strictly for use in retrieval from indices, rate aggregation, and casting to their
     * parent numeric type.
     */
    COUNTER_LONG("counter_long", Long.BYTES, false, false, true),
    COUNTER_INTEGER("counter_integer", Integer.BYTES, false, false, true),
    COUNTER_DOUBLE("counter_double", Double.BYTES, false, false, true),
    DOC_DATA_TYPE("_doc", Integer.BYTES * 3, false, false, false),
    TSID_DATA_TYPE("_tsid", Integer.MAX_VALUE, false, false, true);

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

    DataType(String esName, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        this(null, esName, size, isInteger, isRational, hasDocValues);
    }

    DataType(String typeName, String esType, int size, boolean isInteger, boolean isRational, boolean hasDocValues) {
        String typeString = typeName != null ? typeName : esType;
        this.typeName = typeString.toLowerCase(Locale.ROOT);
        this.name = typeString.toUpperCase(Locale.ROOT);
        this.esType = esType;
        this.size = size;
        this.isInteger = isInteger;
        this.isRational = isRational;
        this.docValues = hasDocValues;
    }

    private static final Collection<DataType> TYPES = Stream.of(
        UNSUPPORTED,
        NULL,
        BOOLEAN,
        BYTE,
        SHORT,
        INTEGER,
        LONG,
        UNSIGNED_LONG,
        DOUBLE,
        FLOAT,
        HALF_FLOAT,
        SCALED_FLOAT,
        KEYWORD,
        TEXT,
        DATETIME,
        IP,
        VERSION,
        OBJECT,
        NESTED,
        SOURCE,
        DATE_PERIOD,
        TIME_DURATION,
        GEO_POINT,
        CARTESIAN_POINT,
        CARTESIAN_SHAPE,
        GEO_SHAPE,
        COUNTER_LONG,
        COUNTER_INTEGER,
        COUNTER_DOUBLE
    ).sorted(Comparator.comparing(DataType::typeName)).toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        map.put("date_nanos", DATETIME);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
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
        if (value instanceof String || value instanceof Character) {
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
}

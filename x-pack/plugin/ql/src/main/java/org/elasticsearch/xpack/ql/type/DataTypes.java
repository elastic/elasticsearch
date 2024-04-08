/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.index.mapper.SourceFieldMapper;

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

public final class DataTypes {

    // tag::noformat
    public static final DataType UNSUPPORTED      = new DataType("UNSUPPORTED", null, 0,                 false, false, false);

    public static final DataType NULL             = new DataType("null",              0,                 false, false, false);

    public static final DataType BOOLEAN          = new DataType("boolean",           1,                 false, false, false);
    // integer numeric
    public static final DataType BYTE             = new DataType("byte",              Byte.BYTES,        true, false, true);
    public static final DataType SHORT            = new DataType("short",             Short.BYTES,       true, false, true);
    public static final DataType INTEGER          = new DataType("integer",           Integer.BYTES,     true, false, true);
    public static final DataType LONG             = new DataType("long",              Long.BYTES,        true, false, true);
    public static final DataType UNSIGNED_LONG    = new DataType("unsigned_long",     Long.BYTES,        true, false, true);
    // decimal numeric
    public static final DataType DOUBLE           = new DataType("double",            Double.BYTES,      false, true, true);
    public static final DataType FLOAT            = new DataType("float",             Float.BYTES,       false, true, true);
    public static final DataType HALF_FLOAT       = new DataType("half_float",        Float.BYTES,       false, true, true);
    public static final DataType SCALED_FLOAT     = new DataType("scaled_float",      Long.BYTES,        false, true, true);
    // string
    public static final DataType KEYWORD          = new DataType("keyword",           Integer.MAX_VALUE, false, false, true);
    public static final DataType TEXT             = new DataType("text",              Integer.MAX_VALUE, false, false, false);
    // date
    public static final DataType DATETIME         = new DataType("DATETIME", "date",        Long.BYTES,  false, false, true);
    // ip
    public static final DataType IP               = new DataType("ip",                45,                false, false, true);
    // version
    public static final DataType VERSION          = new DataType("version",           Integer.MAX_VALUE, false, false, true);
    // binary
    public static final DataType BINARY           = new DataType("binary",            Integer.MAX_VALUE, false, false, true);
    // complex types
    public static final DataType OBJECT           = new DataType("object",            0,                 false, false, false);
    public static final DataType NESTED           = new DataType("nested",            0,                 false, false, false);
    //end::noformat
    public static final DataType SOURCE = new DataType(
        SourceFieldMapper.NAME,
        SourceFieldMapper.NAME,
        Integer.MAX_VALUE,
        false,
        false,
        false
    );

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
        BINARY,
        OBJECT,
        NESTED
    ).sorted(Comparator.comparing(DataType::typeName)).toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        map.put("date_nanos", DATETIME);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private DataTypes() {}

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
}

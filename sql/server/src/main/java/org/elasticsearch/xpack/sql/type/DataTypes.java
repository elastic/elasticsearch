/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.sql.JDBCType;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class DataTypes {

    public static final DataType NULL = new NullType();
    public static final DataType BOOLEAN = new BooleanType(true);
    public static final DataType BYTE = new ByteType(true);
    public static final DataType SHORT = new ShortType(true);
    public static final DataType INTEGER = new IntegerType(true);
    public static final DataType LONG = new LongType(true);
    public static final DataType DOUBLE = new DoubleType(true);
    public static final DataType FLOAT = new FloatType(true);
    public static final DataType HALF_FLOAT = new HalfFloatType(true);
    public static final DataType IP_TYPE = new IpType(true);
    public static final DataType KEYWORD = KeywordType.DEFAULT;
    public static final DataType TEXT = new TextType();

    public static final DataType GEO_POINT = new GeoPointType();
    public static final DataType DATE = DateType.DEFAULT;

    public static final DataType BINARY = new BinaryType(true);
    public static final DataType UNKNOWN = new UnknownDataType();

    private static final Map<String, DataType> ES_PRIMITIVES_DEFAULT = new LinkedHashMap<>();
    private static final Map<String, DataType> ES_PRIMITIVES_NO_DOC_VALUES = new LinkedHashMap<>();
    private static final Map<JDBCType, DataType> JDBC_TO_TYPES = new LinkedHashMap<>();

    static {
        initDefault(NULL);
        initDefault(BOOLEAN);
        initDefault(BYTE);
        initDefault(SHORT);
        initDefault(INTEGER);
        initDefault(LONG);
        initDefault(DOUBLE);
        initDefault(FLOAT);
        initDefault(HALF_FLOAT);
        initDefault(IP_TYPE);
        // text and keyword are handled separately
        initDefault(BINARY);
        initDefault(UNKNOWN);

        //init(GEO_POINT);

        for (DataType type : ES_PRIMITIVES_DEFAULT.values()) {
            JDBC_TO_TYPES.put(type.sqlType(), type);
        }

        initNoDocValues(NULL);
        initNoDocValues(new BooleanType(false));
        initNoDocValues(new ByteType(false));
        initNoDocValues(new ShortType(false));
        initNoDocValues(new IntegerType(false));
        initNoDocValues(new LongType(false));
        initNoDocValues(new DoubleType(false));
        initNoDocValues(new FloatType(false));
        initNoDocValues(new HalfFloatType(false));
        initNoDocValues(new IpType(false));
        initNoDocValues(new BinaryType(false));
        initNoDocValues(UNKNOWN);
    }

    private static void initDefault(DataType type) {
        ES_PRIMITIVES_DEFAULT.put(type.esName(), type);
    }

    private static void initNoDocValues(DataType type) {
        ES_PRIMITIVES_NO_DOC_VALUES.put(type.esName(), type);
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
        if (value instanceof String) {
            return KEYWORD;
        }
        throw new SqlIllegalArgumentException("No idea what's the DataType for %s", value.getClass());
    }

    public static DataType from(JDBCType type) {
        return JDBC_TO_TYPES.get(type);
    }

    public static DataType fromEsName(String typeString, boolean docValuesEnabled) {
        return docValuesEnabled ? ES_PRIMITIVES_DEFAULT.get(typeString) : ES_PRIMITIVES_NO_DOC_VALUES.get(typeString);
    }
}
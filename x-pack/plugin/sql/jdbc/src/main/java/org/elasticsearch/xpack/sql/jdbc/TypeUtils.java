/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Calendar;
import java.util.Collections;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

final class TypeUtils {

    private TypeUtils() {}

    private static final Map<Class<?>, EsType> CLASS_TO_TYPE;
    private static final Map<EsType, Class<?>> TYPE_TO_CLASS;
    private static final Map<String, EsType> ENUM_NAME_TO_TYPE;
    private static final Map<Integer, EsType> SQL_TO_TYPE;

    private static final Set<EsType> SIGNED_TYPE = EnumSet.of(EsType.BYTE,
            EsType.SHORT, EsType.INTEGER, EsType.LONG,
            EsType.FLOAT, EsType.HALF_FLOAT, EsType.SCALED_FLOAT, EsType.DOUBLE, EsType.DATETIME);


    static {
        Map<Class<?>, EsType> aMap = new LinkedHashMap<>();
        aMap.put(Boolean.class, EsType.BOOLEAN);
        aMap.put(Byte.class, EsType.BYTE);
        aMap.put(Short.class, EsType.SHORT);
        aMap.put(Integer.class, EsType.INTEGER);
        aMap.put(Long.class, EsType.LONG);
        aMap.put(Float.class, EsType.FLOAT);
        aMap.put(Double.class, EsType.DOUBLE);
        aMap.put(String.class, EsType.KEYWORD);
        aMap.put(byte[].class, EsType.BINARY);
        aMap.put(String.class, EsType.KEYWORD);
        aMap.put(Timestamp.class, EsType.DATETIME);

        // apart from the mappings in {@code DataType} three more Java classes can be mapped to a {@code JDBCType.TIMESTAMP}
        // according to B-4 table from the jdbc4.2 spec
        aMap.put(Calendar.class, EsType.DATETIME);
        aMap.put(GregorianCalendar.class, EsType.DATETIME);
        aMap.put(java.util.Date.class, EsType.DATETIME);
        aMap.put(java.sql.Date.class, EsType.DATETIME);
        aMap.put(java.sql.Time.class, EsType.TIME);
        aMap.put(LocalDateTime.class, EsType.DATETIME);
        CLASS_TO_TYPE = Collections.unmodifiableMap(aMap);

        Map<EsType, Class<?>> types = new LinkedHashMap<>();
        types.put(EsType.BOOLEAN, Boolean.class);
        types.put(EsType.BYTE, Byte.class);
        types.put(EsType.SHORT, Short.class);
        types.put(EsType.INTEGER, Integer.class);
        types.put(EsType.LONG, Long.class);
        types.put(EsType.DOUBLE, Double.class);
        types.put(EsType.FLOAT, Float.class);
        types.put(EsType.HALF_FLOAT, Double.class);
        types.put(EsType.SCALED_FLOAT, Double.class);
        types.put(EsType.KEYWORD, String.class);
        types.put(EsType.TEXT, String.class);
        types.put(EsType.BINARY, byte[].class);
        types.put(EsType.DATETIME, Timestamp.class);
        types.put(EsType.IP, String.class);
        types.put(EsType.INTERVAL_YEAR, Period.class);
        types.put(EsType.INTERVAL_MONTH, Period.class);
        types.put(EsType.INTERVAL_YEAR_TO_MONTH, Period.class);
        types.put(EsType.INTERVAL_DAY, Duration.class);
        types.put(EsType.INTERVAL_HOUR, Duration.class);
        types.put(EsType.INTERVAL_MINUTE, Duration.class);
        types.put(EsType.INTERVAL_SECOND, Duration.class);
        types.put(EsType.INTERVAL_DAY_TO_HOUR, Duration.class);
        types.put(EsType.INTERVAL_DAY_TO_MINUTE, Duration.class);
        types.put(EsType.INTERVAL_DAY_TO_SECOND, Duration.class);
        types.put(EsType.INTERVAL_HOUR_TO_MINUTE, Duration.class);
        types.put(EsType.INTERVAL_HOUR_TO_SECOND, Duration.class);
        types.put(EsType.INTERVAL_MINUTE_TO_SECOND, Duration.class);
        types.put(EsType.GEO_POINT, String.class);
        types.put(EsType.GEO_SHAPE, String.class);
        types.put(EsType.SHAPE, String.class);

        TYPE_TO_CLASS = unmodifiableMap(types);


        Map<String, EsType> strings = new LinkedHashMap<>();
        Map<Integer, EsType> numbers = new LinkedHashMap<>();

        for (EsType dataType : EsType.values()) {
            strings.put(dataType.getName().toLowerCase(Locale.ROOT), dataType);
            numbers.putIfAbsent(dataType.getVendorTypeNumber(), dataType);
        }

        ENUM_NAME_TO_TYPE = unmodifiableMap(strings);
        SQL_TO_TYPE = unmodifiableMap(numbers);
    }

    static boolean isSigned(EsType type) {
        return SIGNED_TYPE.contains(type);
    }

    static Class<?> classOf(EsType type) {
        return TYPE_TO_CLASS.get(type);
    }

    static SQLType asSqlType(int sqlType) throws SQLException {
        for (JDBCType jdbcType : JDBCType.class.getEnumConstants()) {
            if (sqlType == jdbcType.getVendorTypeNumber().intValue()) {
                return jdbcType;
            }
        }
        // fallback to DataType
        return of(sqlType);
    }

    static EsType of(SQLType sqlType) throws SQLException {
        if (sqlType instanceof EsType) {
            return (EsType) sqlType;
        }
        EsType dataType = SQL_TO_TYPE.get(Integer.valueOf(sqlType.getVendorTypeNumber()));
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported SQL type [" + sqlType + "]");
        }
        return dataType;
    }

    static EsType of(int sqlType) throws SQLException {
        EsType dataType = SQL_TO_TYPE.get(Integer.valueOf(sqlType));
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported SQL type [" + sqlType + "]");
        }
        return dataType;
    }

    static EsType of(String name) throws SQLException {
        EsType dataType = ENUM_NAME_TO_TYPE.get(name);
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported Data type [" + name + "]");
        }
        return dataType;
    }

    static boolean isString(EsType dataType) {
        return dataType == EsType.KEYWORD || dataType == EsType.TEXT;
    }

    static EsType of(Class<? extends Object> clazz) throws SQLException {
        EsType dataType = CLASS_TO_TYPE.get(clazz);

        if (dataType == null) {
            // fall-back to iteration for checking class hierarchies (in case of custom objects)
            for (Entry<Class<?>, EsType> e : CLASS_TO_TYPE.entrySet()) {
                if (e.getKey().isAssignableFrom(clazz)) {
                    return e.getValue();
                }
            }

            throw new SQLFeatureNotSupportedException("Objects of type [" + clazz.getName() + "] are not supported");
        }
        return dataType;
    }
}

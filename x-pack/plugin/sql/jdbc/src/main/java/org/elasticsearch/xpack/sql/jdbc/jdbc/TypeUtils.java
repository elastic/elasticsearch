/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc.jdbc;

import org.elasticsearch.xpack.sql.jdbc.type.DataType;

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

public class TypeUtils {

    private TypeUtils() {}

    private static final Map<Class<?>, DataType> CLASS_TO_TYPE;
    private static final Map<DataType, Class<?>> TYPE_TO_CLASS;
    private static final Map<String, DataType> ENUM_NAME_TO_TYPE;
    private static final Map<Integer, DataType> SQL_TO_TYPE;

    private static final Set<DataType> SIGNED_TYPE = EnumSet.of(DataType.BYTE,
            DataType.SHORT, DataType.INTEGER, DataType.LONG,
            DataType.FLOAT, DataType.HALF_FLOAT, DataType.SCALED_FLOAT, DataType.DOUBLE, DataType.DATE);


    static {
        Map<Class<?>, DataType> aMap = new LinkedHashMap<>();
        aMap.put(Boolean.class, DataType.BOOLEAN);
        aMap.put(Byte.class, DataType.BYTE);
        aMap.put(Short.class, DataType.SHORT);
        aMap.put(Integer.class, DataType.INTEGER);
        aMap.put(Long.class, DataType.LONG);
        aMap.put(Float.class, DataType.FLOAT);
        aMap.put(Double.class, DataType.DOUBLE);
        aMap.put(String.class, DataType.KEYWORD);
        aMap.put(byte[].class, DataType.BINARY);
        aMap.put(String.class, DataType.KEYWORD);
        aMap.put(Timestamp.class, DataType.DATE);

        // apart from the mappings in {@code DataType} three more Java classes can be mapped to a {@code JDBCType.TIMESTAMP}
        // according to B-4 table from the jdbc4.2 spec
        aMap.put(Calendar.class, DataType.DATE);
        aMap.put(GregorianCalendar.class, DataType.DATE);
        aMap.put(java.util.Date.class, DataType.DATE);
        aMap.put(java.sql.Date.class, DataType.DATE);
        aMap.put(java.sql.Time.class, DataType.DATE);
        aMap.put(LocalDateTime.class, DataType.DATE);
        CLASS_TO_TYPE = Collections.unmodifiableMap(aMap);

        Map<DataType, Class<?>> types = new LinkedHashMap<>();
        types.put(DataType.BOOLEAN, Boolean.class);
        types.put(DataType.BYTE, Byte.class);
        types.put(DataType.SHORT, Short.class);
        types.put(DataType.INTEGER, Integer.class);
        types.put(DataType.LONG, Long.class);
        types.put(DataType.DOUBLE, Double.class);
        types.put(DataType.FLOAT, Float.class);
        types.put(DataType.HALF_FLOAT, Double.class);
        types.put(DataType.SCALED_FLOAT, Double.class);
        types.put(DataType.KEYWORD, String.class);
        types.put(DataType.TEXT, String.class);
        types.put(DataType.BINARY, byte[].class);
        types.put(DataType.DATE, Timestamp.class);
        types.put(DataType.IP, String.class);
        types.put(DataType.INTERVAL_YEAR, Period.class);
        types.put(DataType.INTERVAL_MONTH, Period.class);
        types.put(DataType.INTERVAL_YEAR_TO_MONTH, Period.class);
        types.put(DataType.INTERVAL_DAY, Duration.class);
        types.put(DataType.INTERVAL_HOUR, Duration.class);
        types.put(DataType.INTERVAL_MINUTE, Duration.class);
        types.put(DataType.INTERVAL_SECOND, Duration.class);
        types.put(DataType.INTERVAL_DAY_TO_HOUR, Duration.class);
        types.put(DataType.INTERVAL_DAY_TO_MINUTE, Duration.class);
        types.put(DataType.INTERVAL_DAY_TO_SECOND, Duration.class);
        types.put(DataType.INTERVAL_HOUR_TO_MINUTE, Duration.class);
        types.put(DataType.INTERVAL_HOUR_TO_SECOND, Duration.class);
        types.put(DataType.INTERVAL_MINUTE_TO_SECOND, Duration.class);

        TYPE_TO_CLASS = unmodifiableMap(types);


        Map<String, DataType> strings = new LinkedHashMap<>();
        Map<Integer, DataType> numbers = new LinkedHashMap<>();

        for (DataType dataType : DataType.values()) {
            strings.put(dataType.getName().toLowerCase(Locale.ROOT), dataType);
            numbers.putIfAbsent(dataType.getVendorTypeNumber(), dataType);
        }

        ENUM_NAME_TO_TYPE = unmodifiableMap(strings);
        SQL_TO_TYPE = unmodifiableMap(numbers);
    }

    static boolean isSigned(DataType type) {
        return SIGNED_TYPE.contains(type);
    }

    static Class<?> classOf(DataType type) {
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

    static DataType of(SQLType sqlType) throws SQLException {
        if (sqlType instanceof DataType) {
            return (DataType) sqlType;
        }
        DataType dataType = SQL_TO_TYPE.get(Integer.valueOf(sqlType.getVendorTypeNumber()));
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported SQL type [" + sqlType + "]");
        }
        return dataType;
    }

    static DataType of(int sqlType) throws SQLException {
        DataType dataType = SQL_TO_TYPE.get(Integer.valueOf(sqlType));
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported SQL type [" + sqlType + "]");
        }
        return dataType;
    }

    public static DataType of(String name) throws SQLException {
        DataType dataType = ENUM_NAME_TO_TYPE.get(name);
        if (dataType == null) {
            throw new SQLFeatureNotSupportedException("Unsupported Data type [" + name + "]");
        }
        return dataType;
    }

    static boolean isString(DataType dataType) {
        return dataType == DataType.KEYWORD || dataType == DataType.TEXT;
    }

    static DataType of(Class<? extends Object> clazz) throws SQLException {
        DataType dataType = CLASS_TO_TYPE.get(clazz);

        if (dataType == null) {
            // fall-back to iteration for checking class hierarchies (in case of custom objects)
            for (Entry<Class<?>, DataType> e : CLASS_TO_TYPE.entrySet()) {
                if (e.getKey().isAssignableFrom(clazz)) {
                    return e.getValue();
                }
            }

            throw new SQLFeatureNotSupportedException("Objects of type [" + clazz.getName() + "] are not supported");
        }
        return dataType;
    }
}
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.type.ArrayDataType;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.isArray;
import static org.elasticsearch.xpack.sql.expression.literal.interval.Intervals.compatibleInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATETIME_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.FLOAT_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_POINT_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_HOUR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_DAY_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_HOUR_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MINUTE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MINUTE_TO_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_SECOND;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.INTERVAL_YEAR_TO_MONTH;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.KEYWORD_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.LONG_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.SHAPE_ARRAY;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.areCompatible;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.defaultPrecision;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.displaySize;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isDateBased;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isDateOrTimeBased;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isFromDocValuesOnly;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isGeo;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlDataType;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlDateTimeSub;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlMaximumScale;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlMinimumScale;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.metaSqlRadix;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.sqlType;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.types;

public class SqlDataTypesTests extends ESTestCase {

    public void testMetadataType() {
        assertEquals(Integer.valueOf(91), metaSqlDataType(DATE));
        assertEquals(Integer.valueOf(92), metaSqlDataType(TIME));
        assertEquals(Integer.valueOf(9), metaSqlDataType(DATETIME));
        assertEquals(Integer.valueOf(9), metaSqlDataType(DATETIME_ARRAY));
        DataType t = randomDataTypeNoDateTime();
        assertEquals(sqlType(t).getVendorTypeNumber(), metaSqlDataType(t));
    }

    public void testMetaDateTypeSub() {
        assertEquals(Integer.valueOf(1), metaSqlDateTimeSub(DATE));
        assertEquals(Integer.valueOf(2), metaSqlDateTimeSub(TIME));
        assertEquals(Integer.valueOf(3), metaSqlDateTimeSub(DATETIME));
        assertEquals(Integer.valueOf(3), metaSqlDateTimeSub(DATETIME_ARRAY));
        assertEquals(Integer.valueOf(0), metaSqlDateTimeSub(randomDataTypeNoDateTime()));
    }

    public void testMetaMinimumScale() {
        assertNull(metaSqlMinimumScale(DATE));
        assertEquals(Short.valueOf((short) 9), metaSqlMinimumScale(TIME));
        assertEquals(Short.valueOf((short) 9), metaSqlMinimumScale(DATETIME));
        assertEquals(Short.valueOf((short) 9), metaSqlMinimumScale(DATETIME_ARRAY));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(LONG));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(LONG_ARRAY));
        assertEquals(Short.valueOf((short) defaultPrecision(FLOAT)), metaSqlMaximumScale(FLOAT));
        assertEquals(Short.valueOf((short) defaultPrecision(FLOAT_ARRAY)), metaSqlMaximumScale(FLOAT_ARRAY));
        assertNull(metaSqlMinimumScale(KEYWORD));
        assertNull(metaSqlMinimumScale(KEYWORD_ARRAY));
    }

    public void testMetaMaximumScale() {
        assertNull(metaSqlMinimumScale(DATE));
        assertEquals(Short.valueOf((short) 9), metaSqlMinimumScale(TIME));
        assertEquals(Short.valueOf((short) 9), metaSqlMaximumScale(DATETIME));
        assertEquals(Short.valueOf((short) 9), metaSqlMaximumScale(DATETIME_ARRAY));
        assertEquals(Short.valueOf((short) 0), metaSqlMaximumScale(LONG));
        assertEquals(Short.valueOf((short) 0), metaSqlMaximumScale(LONG_ARRAY));
        assertEquals(Short.valueOf((short) defaultPrecision(FLOAT)), metaSqlMaximumScale(FLOAT));
        assertEquals(Short.valueOf((short) defaultPrecision(FLOAT_ARRAY)), metaSqlMaximumScale(FLOAT_ARRAY));
        assertNull(metaSqlMaximumScale(KEYWORD));
        assertNull(metaSqlMaximumScale(KEYWORD_ARRAY));
    }

    public void testMetaRadix() {
        assertNull(metaSqlRadix(DATETIME));
        assertNull(metaSqlRadix(DATETIME_ARRAY));
        assertNull(metaSqlRadix(KEYWORD));
        assertNull(metaSqlRadix(KEYWORD_ARRAY));
        assertEquals(Integer.valueOf(10), metaSqlRadix(LONG));
        assertEquals(Integer.valueOf(10), metaSqlRadix(LONG_ARRAY));
        assertEquals(Integer.valueOf(2), metaSqlRadix(FLOAT));
        assertEquals(Integer.valueOf(2), metaSqlRadix(FLOAT_ARRAY));
    }


    // type checks
    public void testIsInterval() {
        for (DataType dataType : asList(INTERVAL_YEAR,
                INTERVAL_MONTH,
                INTERVAL_DAY,
                INTERVAL_HOUR,
                INTERVAL_MINUTE,
                INTERVAL_SECOND,
                INTERVAL_YEAR_TO_MONTH,
                INTERVAL_DAY_TO_HOUR,
                INTERVAL_DAY_TO_MINUTE,
                INTERVAL_DAY_TO_SECOND,
                INTERVAL_HOUR_TO_MINUTE,
                INTERVAL_HOUR_TO_SECOND,
                INTERVAL_MINUTE_TO_SECOND)) {
            assertTrue(dataType + " is not an interval", isInterval(dataType));
        }
    }

    public void testIntervalCompatibilityYearMonth() {
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_YEAR, INTERVAL_MONTH));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_YEAR, INTERVAL_YEAR_TO_MONTH));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_MONTH, INTERVAL_YEAR));
        assertEquals(INTERVAL_YEAR_TO_MONTH, compatibleInterval(INTERVAL_MONTH, INTERVAL_YEAR_TO_MONTH));
    }

    public void testIntervalCompatibilityDayTime() {
        assertEquals(INTERVAL_DAY_TO_HOUR, compatibleInterval(INTERVAL_DAY, INTERVAL_HOUR));
        assertEquals(INTERVAL_DAY_TO_HOUR, compatibleInterval(INTERVAL_DAY_TO_HOUR, INTERVAL_HOUR));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_DAY, INTERVAL_MINUTE));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_DAY_TO_HOUR, INTERVAL_HOUR_TO_MINUTE));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_MINUTE, INTERVAL_DAY_TO_HOUR));
        assertEquals(INTERVAL_DAY_TO_MINUTE, compatibleInterval(INTERVAL_DAY, INTERVAL_DAY_TO_MINUTE));
        assertEquals(INTERVAL_DAY_TO_SECOND, compatibleInterval(INTERVAL_DAY, INTERVAL_SECOND));
        assertEquals(INTERVAL_DAY_TO_SECOND, compatibleInterval(INTERVAL_MINUTE, INTERVAL_DAY_TO_SECOND));

        assertEquals(INTERVAL_HOUR_TO_MINUTE, compatibleInterval(INTERVAL_MINUTE, INTERVAL_HOUR));
        assertEquals(INTERVAL_HOUR_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_HOUR));
        assertEquals(INTERVAL_HOUR_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_HOUR_TO_MINUTE));
        assertEquals(INTERVAL_HOUR_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_HOUR_TO_MINUTE));

        assertEquals(INTERVAL_MINUTE_TO_SECOND, compatibleInterval(INTERVAL_SECOND, INTERVAL_MINUTE));
    }

    public void testIncompatibleInterval() {
        assertNull(compatibleInterval(INTERVAL_YEAR, INTERVAL_SECOND));
        assertNull(compatibleInterval(INTERVAL_YEAR, INTERVAL_DAY_TO_HOUR));
        assertNull(compatibleInterval(INTERVAL_HOUR, INTERVAL_MONTH));
        assertNull(compatibleInterval(INTERVAL_MINUTE_TO_SECOND, INTERVAL_MONTH));
    }

    public void testIntervalCompabitilityWithDateTimes() {
        for (DataType intervalType : asList(INTERVAL_YEAR,
            INTERVAL_MONTH,
            INTERVAL_DAY,
            INTERVAL_HOUR,
            INTERVAL_MINUTE,
            INTERVAL_SECOND,
            INTERVAL_YEAR_TO_MONTH,
            INTERVAL_DAY_TO_HOUR,
            INTERVAL_DAY_TO_MINUTE,
            INTERVAL_DAY_TO_SECOND,
            INTERVAL_HOUR_TO_MINUTE,
            INTERVAL_HOUR_TO_SECOND,
            INTERVAL_MINUTE_TO_SECOND)) {
            for (DataType dateTimeType: asList(DATE, DATETIME)) {
                assertTrue(areCompatible(intervalType, dateTimeType));
                assertTrue(areCompatible(dateTimeType, intervalType));
            }
        }
    }

    public void testIsDateBased() {
        List<DataType> dateBasedList = asList(DATE, DATETIME, DATETIME_ARRAY);
        for (DataType d : dateBasedList) {
            assertTrue(isDateBased(d));
        }
        DataType nonDateBased = randomValueOtherThanMany(dateBasedList::contains, () -> randomFrom(types()));
        assertFalse(isDateBased(nonDateBased));
    }

    public void testIsDateOrTimeBased() {
        List<DataType> dateOrTimeBasedList = asList(DATE, TIME, DATETIME, DATETIME_ARRAY);
        for (DataType d : dateOrTimeBasedList) {
            assertTrue(isDateOrTimeBased(d));
        }
        DataType nonDateOrTimeBased = randomValueOtherThanMany(dateOrTimeBasedList::contains, () -> randomFrom(types()));
        assertFalse(isDateOrTimeBased(nonDateOrTimeBased));
    }

    public void testIsGeo() {
        List<DataType> geoBasedList = asList(GEO_POINT, GEO_SHAPE, SHAPE, GEO_POINT_ARRAY, GEO_SHAPE_ARRAY, SHAPE_ARRAY);
        for (DataType d : geoBasedList) {
            assertTrue(isGeo(d));
        }
        DataType nonGeoBased = randomValueOtherThanMany(geoBasedList::contains, () -> randomFrom(types()));
        assertFalse(isGeo(nonGeoBased));
    }

    public void testIsFromDocValuesOnly() {
        List<DataType> fromDocValOnly = new ArrayList<>();
        for (DataType d : asList(KEYWORD, DATE, DATETIME, SCALED_FLOAT, GEO_POINT, SHAPE)) {
            fromDocValOnly.add(d);
            if (d != DATE) {
                DataType a = ArrayDataType.of(d);
                fromDocValOnly.add(a);
            }
        }

        fromDocValOnly.forEach(x -> assertTrue(isFromDocValuesOnly(x)));

        DataType nonExclusive = randomValueOtherThanMany(fromDocValOnly::contains, () -> randomFrom(types()));
        assertFalse(isFromDocValuesOnly(nonExclusive));
    }

    public void testIsArray() {
        for (DataType d : types()) {
            assertTrue(sqlType(d) != JDBCType.ARRAY || isArray(d));
        }
    }

    public void testArrayDefaultPrecisionAndSize() {
        for (DataType d : types()) {
            if (isArray(d)) {
                DataType baseType = ((ArrayDataType) d).baseType();
                assertEquals(defaultPrecision(baseType), defaultPrecision(d));
                assertEquals(displaySize(baseType), displaySize(d));
            }
        }
    }

    public void testEsToDataType() {
        List<String> types = new ArrayList<>(Arrays.asList("null", "boolean", "bool",
                "byte", "tinyint",
                "short", "smallint",
                "integer",
                "long", "bigint",
                "double", "real",
                "half_float", "scaled_float", "float",
                "decimal", "numeric",
                "keyword", "text", "varchar",
                "date", "time", "datetime", "timestamp",
                "binary", "varbinary",
                "ip",
                "interval_year", "interval_month", "interval_year_to_month",
                "interval_day", "interval_hour", "interval_minute", "interval_second",
                "interval_day_to_hour", "interval_day_to_minute", "interval_day_to_second",
                "interval_hour_to_minute", "interval_hour_to_second",
                "interval_minute_to_second",
                "boolean_array",
                "byte_array", "short_array", "integer_array", "long_array",
                "double_array", "float_array", "half_float_array", "scaled_float_array",
                "keyword_array", "text_array",
                "datetime_array",
                "ip_array",
                "binary_array",
                "geo_shape_array", "geo_point_array",
                "shape_array"));

        types.addAll(SqlDataTypes.types().stream()
                .filter(DataTypes::isPrimitive)
                .map(DataType::typeName)
               .collect(toList()));
        String type = randomFrom(types.toArray(new String[0]));
        DataType dataType = SqlDataTypes.fromSqlOrEsType(type);
        assertNotNull("cound not find " + type, dataType);
    }

    public void testTypeAsArray() {
        for (String baseTypeName : Arrays.asList(
            "boolean", "bool", "bit",
            "byte", "smallint",
            "short", "tinyint",
            "integer",
            "long", "bigint",
            "double", "real",
            "float", "half_float", "scaled_float",
            "keyword", "text",
            "datetime", "timestamp",
            "ip",
            "binary", "varbinary",
            "geo_shape", "geo_point",
            "shape")) {
            DataType baseType = SqlDataTypes.fromSqlOrEsType(baseTypeName);
            DataType arrayType = SqlDataTypes.arrayType(baseType);
            assertNotNull(arrayType);
            assertTrue(arrayType instanceof ArrayDataType);
        }
    }

    public void testTypeNotAsArray() {
        for (String baseTypeName : Arrays.asList(
            "null",
            "date", "time",
            "interval_year", "interval_month", "interval_year_to_month",
            "interval_day", "interval_hour", "interval_minute", "interval_second",
            "interval_day_to_hour", "interval_day_to_minute", "interval_day_to_second",
            "interval_hour_to_minute", "interval_hour_to_second",
            "interval_minute_to_second")) {
            DataType baseType = SqlDataTypes.fromSqlOrEsType(baseTypeName);
            DataType arrayType = SqlDataTypes.arrayType(baseType);
            assertNull(arrayType);
        }
    }

        private DataType randomDataTypeNoDateTime() {
        return randomValueOtherThanMany(SqlDataTypes::isDateOrTimeBased, () -> randomFrom(SqlDataTypes.types()));
    }
}

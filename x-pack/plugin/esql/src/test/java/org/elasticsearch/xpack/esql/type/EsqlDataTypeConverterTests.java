/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.COUNTER_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOC_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.HALF_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.PARTIAL_AGG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SCALED_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SOURCE;
import static org.elasticsearch.xpack.esql.core.type.DataType.TSID_DATA_TYPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTimeOrNanosOrTemporal;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;

public class EsqlDataTypeConverterTests extends ESTestCase {

    public void testNanoTimeToString() {
        long expected = randomLong();
        long actual = EsqlDataTypeConverter.dateNanosToLong(EsqlDataTypeConverter.nanoTimeToString(expected));
        assertEquals(expected, actual);
    }

    public void testCommonTypeNull() {
        for (DataType dataType : DataType.values()) {
            assertEqualsCommonType(dataType, NULL, dataType);
        }
    }

    public void testCommonTypeStrings() {
        for (DataType dataType1 : DataType.stringTypes()) {
            for (DataType dataType2 : DataType.values()) {
                if (dataType2 == NULL) {
                    assertEqualsCommonType(dataType1, NULL, dataType1);
                } else if ((isString(dataType1) && isString(dataType2))) {
                    if (dataType1 == dataType2) {
                        assertEqualsCommonType(dataType1, dataType2, dataType1);
                    } else {
                        assertEqualsCommonType(dataType1, dataType2, KEYWORD);
                    }
                } else {
                    assertNullCommonType(dataType1, dataType2);
                }
            }
        }
    }

    public void testCommonTypeDateTimeIntervals() {
        List<DataType> DATE_TIME_INTERVALS = Arrays.stream(DataType.values()).filter(DataType::isDateTimeOrNanosOrTemporal).toList();
        for (DataType dataType1 : DATE_TIME_INTERVALS) {
            for (DataType dataType2 : DataType.values()) {
                if (dataType2 == NULL) {
                    assertEqualsCommonType(dataType1, NULL, dataType1);
                } else if (isDateTimeOrNanosOrTemporal(dataType2)) {
                    if ((dataType1 == DATE_NANOS && dataType2 == DATETIME) || (dataType1 == DATETIME && dataType2 == DATE_NANOS)) {
                        assertNullCommonType(dataType1, dataType2);
                    } else if (isDateTime(dataType1) || isDateTime(dataType2)) {
                        assertEqualsCommonType(dataType1, dataType2, DATETIME);
                    } else if (dataType1 == DATE_NANOS || dataType2 == DATE_NANOS) {
                        assertEqualsCommonType(dataType1, dataType2, DATE_NANOS);
                    } else if (dataType1 == dataType2) {
                        assertEqualsCommonType(dataType1, dataType2, dataType1);
                    } else {
                        assertNullCommonType(dataType1, dataType2);
                    }
                } else {
                    assertNullCommonType(dataType1, dataType2);
                }
            }
        }
    }

    public void testCommonTypeNumeric() {
        // whole numbers
        commonNumericType(BYTE, List.of(NULL, BYTE));
        commonNumericType(SHORT, List.of(NULL, BYTE, SHORT));
        commonNumericType(INTEGER, List.of(NULL, BYTE, SHORT, INTEGER));
        commonNumericType(LONG, List.of(NULL, BYTE, SHORT, INTEGER, LONG));
        commonNumericType(UNSIGNED_LONG, List.of(NULL, BYTE, SHORT, INTEGER, LONG, UNSIGNED_LONG));
        // floats
        commonNumericType(HALF_FLOAT, List.of(NULL, BYTE, SHORT, INTEGER, LONG, UNSIGNED_LONG, HALF_FLOAT, FLOAT));
        commonNumericType(FLOAT, List.of(NULL, BYTE, SHORT, INTEGER, LONG, UNSIGNED_LONG, FLOAT, HALF_FLOAT));
        commonNumericType(DOUBLE, List.of(NULL, BYTE, SHORT, INTEGER, LONG, UNSIGNED_LONG, HALF_FLOAT, FLOAT, DOUBLE, SCALED_FLOAT));
        commonNumericType(SCALED_FLOAT, List.of(NULL, BYTE, SHORT, INTEGER, LONG, UNSIGNED_LONG, HALF_FLOAT, FLOAT, SCALED_FLOAT, DOUBLE));
    }

    /**
     * The first argument and the second argument(s) have the first argument as a common type.
     */
    private static void commonNumericType(DataType numericType, List<DataType> lowerTypes) {
        List<DataType> NUMERICS = Arrays.stream(DataType.values()).filter(DataType::isNumeric).toList();
        List<DataType> DOUBLES = Arrays.stream(DataType.values()).filter(DataType::isRationalNumber).toList();
        for (DataType dataType : DataType.values()) {
            if (DOUBLES.containsAll(List.of(numericType, dataType)) && (dataType.estimatedSize().equals(numericType.estimatedSize()))) {
                assertEquals(numericType, commonType(dataType, numericType));
            } else if (lowerTypes.contains(dataType)) {
                assertEqualsCommonType(numericType, dataType, numericType);
            } else if (NUMERICS.contains(dataType)) {
                assertEqualsCommonType(numericType, dataType, dataType);
            } else {
                assertNullCommonType(numericType, dataType);
            }
        }
    }

    public void testCommonTypeMiscellaneous() {
        List<DataType> MISCELLANEOUS = List.of(
            COUNTER_INTEGER,
            COUNTER_LONG,
            COUNTER_DOUBLE,
            UNSUPPORTED,
            OBJECT,
            SOURCE,
            DOC_DATA_TYPE,
            TSID_DATA_TYPE,
            PARTIAL_AGG,
            IP,
            VERSION,
            GEO_POINT,
            GEO_SHAPE,
            CARTESIAN_POINT,
            CARTESIAN_SHAPE,
            BOOLEAN
        );
        for (DataType dataType1 : MISCELLANEOUS) {
            for (DataType dataType2 : DataType.values()) {
                if (dataType2 == NULL || dataType1 == dataType2) {
                    assertEqualsCommonType(dataType1, dataType2, dataType1);
                } else {
                    assertNullCommonType(dataType1, dataType2);
                }
            }
        }
    }

    private static void assertEqualsCommonType(DataType dataType1, DataType dataType2, DataType commonType) {
        assertEquals("Expected " + commonType + " for " + dataType1 + " and " + dataType2, commonType, commonType(dataType1, dataType2));
        assertEquals("Expected " + commonType + " for " + dataType1 + " and " + dataType2, commonType, commonType(dataType2, dataType1));
    }

    private static void assertNullCommonType(DataType dataType1, DataType dataType2) {
        assertNull("Expected null for " + dataType1 + " and " + dataType2, commonType(dataType1, dataType2));
        assertNull("Expected null for " + dataType1 + " and " + dataType2, commonType(dataType2, dataType1));
    }
}

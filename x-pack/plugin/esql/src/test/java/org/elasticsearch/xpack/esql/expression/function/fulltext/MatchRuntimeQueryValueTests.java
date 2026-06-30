/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.function.fulltext.Match.queryAsRuntimeSearchValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link Match#queryAsRuntimeSearchValue}, the conversion that turns the folded {@code match} query
 * value into the typed value compared against the field block on the runtime-search (non-Lucene) path.
 * <p>
 * This is the most conversion-heavy part of runtime {@code match}: the (field type x query type) matrix, the encoding
 * of {@code ip}/{@code version}/{@code unsigned_long}/dates, and the error vs. silent-coercion behavior. Exercising it
 * here keeps it covered without the cost of an end-to-end query.
 */
public class MatchRuntimeQueryValueTests extends ESTestCase {

    private static BytesRef str(String s) {
        return new BytesRef(s);
    }

    public void testKeywordPassesThroughUnchanged() {
        BytesRef value = str("hello");
        Object result = queryAsRuntimeSearchValue(KEYWORD, KEYWORD, value);
        assertThat(result, instanceOf(BytesRef.class));
        assertThat(result, equalTo(value));
    }

    public void testIpFromStringIsEncoded() {
        Object result = queryAsRuntimeSearchValue(IP, KEYWORD, str("127.0.0.1"));
        assertThat(result, instanceOf(BytesRef.class));
        assertThat(result, equalTo(EsqlDataTypeConverter.stringToIP("127.0.0.1")));
    }

    public void testIpFromIpIsNotReEncoded() {
        // A non-string ip query is already stored in encoded form and must be passed through untouched.
        BytesRef encoded = EsqlDataTypeConverter.stringToIP("::1");
        Object result = queryAsRuntimeSearchValue(IP, IP, encoded);
        assertThat(result, equalTo(encoded));
    }

    public void testVersionFromStringIsEncoded() {
        Object result = queryAsRuntimeSearchValue(VERSION, KEYWORD, str("1.2.3"));
        assertThat(result, instanceOf(BytesRef.class));
        assertThat(result, equalTo(EsqlDataTypeConverter.stringToVersion("1.2.3")));
    }

    public void testBoolean() {
        assertThat(queryAsRuntimeSearchValue(BOOLEAN, BOOLEAN, true), equalTo(true));
        assertThat(queryAsRuntimeSearchValue(BOOLEAN, KEYWORD, str("true")), equalTo(true));
        assertThat(queryAsRuntimeSearchValue(BOOLEAN, KEYWORD, str("false")), equalTo(false));
    }

    public void testDouble() {
        assertThat(queryAsRuntimeSearchValue(DOUBLE, DOUBLE, 1.5d), equalTo(1.5d));
        assertThat(queryAsRuntimeSearchValue(DOUBLE, KEYWORD, str("1.5")), equalTo(1.5d));
        // Cross-type numeric: an integer query widens to the double field.
        assertThat(queryAsRuntimeSearchValue(DOUBLE, INTEGER, 3), equalTo(3.0d));
    }

    public void testInteger() {
        assertThat(queryAsRuntimeSearchValue(INTEGER, INTEGER, 7), equalTo(7));
        assertThat(queryAsRuntimeSearchValue(INTEGER, KEYWORD, str("7")), equalTo(7));
    }

    public void testLong() {
        assertThat(queryAsRuntimeSearchValue(LONG, LONG, 42L), equalTo(42L));
        assertThat(queryAsRuntimeSearchValue(LONG, KEYWORD, str("42")), equalTo(42L));
        // Cross-type numeric: an integer query widens to the long field.
        assertThat(queryAsRuntimeSearchValue(LONG, INTEGER, 9), equalTo(9L));
    }

    public void testDatetime() {
        long millis = EsqlDataTypeConverter.dateTimeToLong("2024-01-01T00:00:00.000Z");
        assertThat(queryAsRuntimeSearchValue(DATETIME, KEYWORD, str("2024-01-01T00:00:00.000Z")), equalTo(millis));
        // A datetime literal is already folded to epoch millis.
        assertThat(queryAsRuntimeSearchValue(DATETIME, DATETIME, millis), equalTo(millis));
    }

    public void testDateNanos() {
        long nanos = EsqlDataTypeConverter.dateNanosToLong("2024-01-01T00:00:00.000Z");
        assertThat(queryAsRuntimeSearchValue(DATE_NANOS, KEYWORD, str("2024-01-01T00:00:00.000Z")), equalTo(nanos));
        assertThat(queryAsRuntimeSearchValue(DATE_NANOS, DATE_NANOS, nanos), equalTo(nanos));
    }

    public void testUnsignedLong() {
        // Encoded unsigned_long literal (e.g. produced by `5::unsigned_long`) is passed through as-is.
        long encodedFive = EsqlDataTypeConverter.longToUnsignedLong(5L, true);
        assertThat(queryAsRuntimeSearchValue(UNSIGNED_LONG, UNSIGNED_LONG, encodedFive), equalTo(encodedFive));
        // A plain numeric query is encoded into the unsigned_long representation.
        assertThat(queryAsRuntimeSearchValue(UNSIGNED_LONG, INTEGER, 5), equalTo(EsqlDataTypeConverter.longToUnsignedLong(5L, true)));
        // The largest unsigned_long, via a string query, round-trips to the encoded max.
        String max = "18446744073709551615";
        assertThat(queryAsRuntimeSearchValue(UNSIGNED_LONG, KEYWORD, str(max)), equalTo(EsqlDataTypeConverter.stringToUnsignedLong(max)));
    }

    /**
     * Documents the current behavior of cross-numeric-type queries: the numeric value is coerced to the field's
     * element type, which silently truncates/overflows. This differs from the string path, which validates ranges
     * (see {@link #testStringQueriesValidateRanges()}). Pinned so any change here is a deliberate one.
     */
    public void testNumericCoercionIsLossyAndSilent() {
        // 1.9 truncates toward zero when the field is a long.
        assertThat(queryAsRuntimeSearchValue(LONG, DOUBLE, 1.9d), equalTo(1L));
        // A long that does not fit overflows when the field is an int (no exception).
        assertThat(queryAsRuntimeSearchValue(INTEGER, LONG, 3_000_000_000L), equalTo((int) 3_000_000_000L));
    }

    /**
     * In contrast to {@link #testNumericCoercionIsLossyAndSilent()}, string queries go through the strict
     * {@link EsqlDataTypeConverter} converters and reject out-of-range / unparseable values.
     */
    public void testStringQueriesValidateRanges() {
        // Out-of-range numeric strings fail with InvalidArgumentException from the strict converters.
        expectThrows(InvalidArgumentException.class, () -> queryAsRuntimeSearchValue(INTEGER, KEYWORD, str("3000000000")));
        expectThrows(
            InvalidArgumentException.class,
            () -> queryAsRuntimeSearchValue(UNSIGNED_LONG, KEYWORD, str("99999999999999999999999"))
        );
        // Unparseable ip / datetime strings fail with IllegalArgumentException.
        expectThrows(IllegalArgumentException.class, () -> queryAsRuntimeSearchValue(IP, KEYWORD, str("not-an-ip")));
        expectThrows(IllegalArgumentException.class, () -> queryAsRuntimeSearchValue(DATETIME, KEYWORD, str("not-a-date")));
    }
}

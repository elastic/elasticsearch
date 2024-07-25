/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Range;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.ZoneId;
import java.util.Arrays;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.FIVE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.L;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.SIX;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

public class RangeTests extends ESTestCase {

    // 6 < a <= 5 -> FALSE
    public void testFoldExcludingRangeToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = EsqlTestUtils.rangeOf(fa, SIX, false, FIVE, true);
        assertEquals(Boolean.FALSE, r.foldable());
    }

    // 6 < a <= 5.5 -> FALSE
    public void testFoldExcludingRangeWithDifferentTypesToFalse() {
        FieldAttribute fa = getFieldAttribute();

        Range r = EsqlTestUtils.rangeOf(fa, SIX, false, L(5.5d), true);
        assertEquals(Boolean.FALSE, r.foldable());
    }

    public void testAreBoundariesInvalid() {
        // value, value type, lower, lower type, lower included, higher, higher type, higher included, boundaries invalid
        Object[][] tests = {
            // dates
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, randomBoolean(), d("2022-01-01"), DATETIME, randomBoolean(), false },
            { d("2021-01-01"), DATETIME, d("2022-01-01"), DATETIME, randomBoolean(), d("2021-01-01"), DATETIME, randomBoolean(), true },
            { d("2021-01-01"), DATETIME, d("2014-01-01"), DATETIME, randomBoolean(), d("2022-01-01"), DATETIME, randomBoolean(), false },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, randomBoolean(), d("2024-01-01"), DATETIME, randomBoolean(), false },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, randomBoolean(), d("1924-01-01"), DATETIME, randomBoolean(), true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, true, d("2021-01-01"), DATETIME, true, false },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, false, d("2021-01-01"), DATETIME, true, true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, true, d("2021-01-01"), DATETIME, false, true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, false, d("2021-01-01"), DATETIME, false, true },
            { d("2021-01-01"), DATETIME, d("2022-01-01"), DATETIME, randomBoolean(), d("2021-01-01"), DATETIME, randomBoolean(), true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, false, d("2021-01-01"), DATETIME, false, true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, true, d("2021-01-01"), DATETIME, true, false },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                d("2021-01-01"),
                DATETIME,
                randomBoolean(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                false },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                d("2021-01-01"),
                DATETIME,
                randomBoolean(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                false },
            {
                randomAlphaOfLength(10),
                DATETIME,
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                d("2021-01-01"),
                DATETIME,
                randomBoolean(),
                true },
            {
                randomAlphaOfLength(10),
                DATETIME,
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                d("2021-01-01"),
                DATETIME,
                randomBoolean(),
                true },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                d("2021-01-01"),
                DATETIME,
                randomBoolean(),
                true },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                d("2014-01-01"),
                DATETIME,
                randomBoolean(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                false },
            { randomAlphaOfLength(10), randomTextType(), d("2021-01-01"), DATETIME, true, d("2021-01-01"), DATETIME, true, false },
            { randomAlphaOfLength(10), randomTextType(), d("2021-01-01"), DATETIME, false, d("2021-01-01"), DATETIME, true, true },
            { randomAlphaOfLength(10), randomTextType(), d("2021-01-01"), DATETIME, true, d("2021-01-01"), DATETIME, false, true },
            { randomAlphaOfLength(10), randomTextType(), d("2021-01-01"), DATETIME, false, d("2021-01-01"), DATETIME, false, true },

            // strings
            {
                randomAlphaOfLength(10),
                randomTextType(),
                "a",
                randomTextType(),
                randomBoolean(),
                "b",
                randomTextType(),
                randomBoolean(),
                false },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                "b",
                randomTextType(),
                randomBoolean(),
                "a",
                randomTextType(),
                randomBoolean(),
                true },
            { randomAlphaOfLength(10), randomTextType(), "a", randomTextType(), false, "a", randomTextType(), false, true },

            // numbers
            { 10, randomNumericType(), 1, randomNumericType(), randomBoolean(), 10, randomNumericType(), randomBoolean(), false },
            { 10, randomNumericType(), 10, randomNumericType(), randomBoolean(), 1, randomNumericType(), randomBoolean(), true },
            { 10, randomNumericType(), 1, randomNumericType(), false, 1, randomNumericType(), randomBoolean(), true },
            { 10, randomNumericType(), 1, randomNumericType(), randomBoolean(), 1, randomNumericType(), false, true },
            { 10, randomNumericType(), 1.0, randomNumericType(), randomBoolean(), 10, randomNumericType(), randomBoolean(), false },
            { 10, randomNumericType(), 1, randomNumericType(), randomBoolean(), 10.D, randomNumericType(), randomBoolean(), false },
            { 10, randomNumericType(), 10.0, randomNumericType(), randomBoolean(), 1, randomNumericType(), randomBoolean(), true },

        };

        for (int i = 0; i < tests.length; i++) {
            Object[] test = tests[i];
            Range range = new Range(
                Source.EMPTY,
                l(test[0], (DataType) test[1]),
                l(test[2], (DataType) test[3]),
                (Boolean) test[4],
                l(test[5], (DataType) test[6]),
                (Boolean) test[7],
                ZoneId.systemDefault()
            );
            assertEquals(
                "failed on test " + i + ": " + Arrays.toString(test),
                test[8],
                Range.areBoundariesInvalid(range.lower().fold(), range.includeLower(), range.upper().fold(), range.includeUpper())
            );
        }
    }

    private static long d(String date) {
        return EsqlDataTypeConverter.dateTimeToLong(date);
    }

    private static DataType randomNumericType() {
        return randomFrom(INTEGER, LONG, UNSIGNED_LONG, DOUBLE);
    }

    private static DataType randomTextType() {
        return randomFrom(KEYWORD, TEXT);
    }

}

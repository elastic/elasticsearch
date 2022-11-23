/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DateUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public class RangeTests extends ESTestCase {

    public void testAreBoundariesInvalid() {
        // value, value type, lower, lower type, lower included, higher, higher type, higher included, boundaries invalid
        Object[][] tests = {
            // dates
            {
                d("2021-01-01"),
                DATETIME,
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                "2022-01-01",
                randomTextType(),
                randomBoolean(),
                false },
            {
                d("2021-01-01"),
                DATETIME,
                "2022-01-01",
                randomTextType(),
                randomBoolean(),
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                true },
            {
                d("2021-01-01"),
                DATETIME,
                "now-10y",
                randomTextType(),
                randomBoolean(),
                "2022-01-01",
                randomTextType(),
                randomBoolean(),
                false },
            {
                d("2021-01-01"),
                DATETIME,
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                "now+10y",
                randomTextType(),
                randomBoolean(),
                false },
            {
                d("2021-01-01"),
                DATETIME,
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                "now-100y",
                randomTextType(),
                randomBoolean(),
                false },
            { d("2021-01-01"), DATETIME, "2021-01-01", randomTextType(), true, "2021-01-01", randomTextType(), true, false },
            { d("2021-01-01"), DATETIME, "2021-01-01", randomTextType(), false, "2021-01-01", randomTextType(), true, true },
            { d("2021-01-01"), DATETIME, "2021-01-01", randomTextType(), true, "2021-01-01", randomTextType(), false, true },
            { d("2021-01-01"), DATETIME, "2021-01-01", randomTextType(), false, "2021-01-01", randomTextType(), false, true },
            {
                d("2021-01-01"),
                DATETIME,
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, false, "2021-01-01", randomTextType(), false, true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, false, d("2021-01-01"), DATETIME, false, true },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, true, "2021-01-01", randomTextType(), true, false },
            { d("2021-01-01"), DATETIME, d("2021-01-01"), DATETIME, true, d("2021-01-01"), DATETIME, true, false },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                d("2021-01-01"),
                DATETIME,
                randomBoolean(),
                "2022-01-01",
                randomTextType(),
                randomBoolean(),
                false },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                false },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                "2021-01-01",
                randomTextType(),
                randomBoolean(),
                true },
            {
                randomAlphaOfLength(10),
                randomTextType(),
                "2022-01-01",
                randomTextType(),
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
                "now-10y",
                randomTextType(),
                randomBoolean(),
                d("2022-01-01"),
                DATETIME,
                randomBoolean(),
                false },
            { randomAlphaOfLength(10), randomTextType(), d("2021-01-01"), DATETIME, true, "2021-01-01", randomTextType(), true, false },
            { randomAlphaOfLength(10), randomTextType(), d("2021-01-01"), DATETIME, false, "2021-01-01", randomTextType(), true, true },
            { randomAlphaOfLength(10), randomTextType(), "2021-01-01", randomTextType(), true, d("2021-01-01"), DATETIME, false, true },
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
            assertEquals("failed on test " + i + ": " + Arrays.toString(test), test[8], range.areBoundariesInvalid());
        }
    }

    private static ZonedDateTime d(String date) {
        return DateUtils.asDateTime(date);
    }

    private static DataType randomNumericType() {
        return randomFrom(INTEGER, SHORT, LONG, UNSIGNED_LONG, FLOAT, DOUBLE);
    }

    private static DataType randomTextType() {
        return randomFrom(KEYWORD, TEXT);
    }

}

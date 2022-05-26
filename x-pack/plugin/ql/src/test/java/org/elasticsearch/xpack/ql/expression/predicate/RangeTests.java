/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

public class RangeTests extends ESTestCase {

    private static final DataType K = DataTypes.KEYWORD;
    private static final DataType T = DataTypes.TEXT;
    private static final DataType DT = DataTypes.DATETIME;

    public void testAreBoundariesInvalid() {
        // value, value type, lower, lower type, lower included, higher, higher type, higher included, boundaries invalid
        Object[][] tests = {
            // dates
            { d("2021-01-01"), DT, "2021-01-01", K, randomBoolean(), "2022-01-01", K, randomBoolean(), false },
            { d("2021-01-01"), DT, "2022-01-01", K, randomBoolean(), "2021-01-01", K, randomBoolean(), true },
            { d("2021-01-01"), DT, "now-10y", K, randomBoolean(), "2022-01-01", K, randomBoolean(), false },
            { d("2021-01-01"), DT, "2021-01-01", K, randomBoolean(), "now+10y", K, randomBoolean(), false },
            { d("2021-01-01"), DT, "2021-01-01", K, randomBoolean(), "now-100y", K, randomBoolean(), false },
            { d("2021-01-01"), DT, "2021-01-01", K, true, "2021-01-01", K, true, false },
            { d("2021-01-01"), DT, "2021-01-01", K, false, "2021-01-01", K, true, true },
            { d("2021-01-01"), DT, "2021-01-01", K, true, "2021-01-01", K, false, true },
            { d("2021-01-01"), DT, "2021-01-01", K, false, "2021-01-01", K, false, true },
            { d("2021-01-01"), DT, d("2022-01-01"), DT, randomBoolean(), "2021-01-01", K, randomBoolean(), true },
            { d("2021-01-01"), DT, d("2021-01-01"), DT, false, "2021-01-01", K, false, true },
            { d("2021-01-01"), DT, d("2021-01-01"), DT, false, d("2021-01-01"), DT, false, true },
            { d("2021-01-01"), DT, d("2021-01-01"), DT, true, "2021-01-01", K, true, false },
            { d("2021-01-01"), DT, d("2021-01-01"), DT, true, d("2021-01-01"), DT, true, false },
            { randomAlphaOfLength(10), randomFrom(K, T), d("2021-01-01"), DT, randomBoolean(), "2022-01-01", K, randomBoolean(), false },
            { randomAlphaOfLength(10), randomFrom(K, T), "2021-01-01", K, randomBoolean(), d("2022-01-01"), DT, randomBoolean(), false },
            { randomAlphaOfLength(10), randomFrom(K, T), d("2022-01-01"), DT, randomBoolean(), "2021-01-01", K, randomBoolean(), true },
            { randomAlphaOfLength(10), randomFrom(K, T), "2022-01-01", K, randomBoolean(), d("2021-01-01"), DT, randomBoolean(), true },
            { randomAlphaOfLength(10), randomFrom(K, T), d("2022-01-01"), DT, randomBoolean(), d("2021-01-01"), DT, randomBoolean(), true },
            { randomAlphaOfLength(10), randomFrom(K, T), "now-10y", K, randomBoolean(), d("2022-01-01"), DT, randomBoolean(), false },
            { randomAlphaOfLength(10), randomFrom(K, T), d("2021-01-01"), DT, true, "2021-01-01", K, true, false },
            { randomAlphaOfLength(10), randomFrom(K, T), d("2021-01-01"), DT, false, "2021-01-01", K, true, true },
            { randomAlphaOfLength(10), randomFrom(K, T), "2021-01-01", K, true, d("2021-01-01"), DT, false, true },
            { randomAlphaOfLength(10), randomFrom(K, T), d("2021-01-01"), DT, false, d("2021-01-01"), DT, false, true },

            // strings
            {
                randomAlphaOfLength(10),
                randomFrom(K, T),
                "a",
                randomFrom(K, T),
                randomBoolean(),
                "b",
                randomFrom(K, T),
                randomBoolean(),
                false },
            {
                randomAlphaOfLength(10),
                randomFrom(K, T),
                "b",
                randomFrom(K, T),
                randomBoolean(),
                "a",
                randomFrom(K, T),
                randomBoolean(),
                true },
            { randomAlphaOfLength(10), randomFrom(K, T), "a", randomFrom(K, T), false, "a", randomFrom(K, T), false, true },

            // numbers
            { 10, randNType(), 1, randNType(), randomBoolean(), 10, randNType(), randomBoolean(), false },
            { 10, randNType(), 10, randNType(), randomBoolean(), 1, randNType(), randomBoolean(), true },
            { 10, randNType(), 1, randNType(), false, 1, randNType(), randomBoolean(), true },
            { 10, randNType(), 1, randNType(), randomBoolean(), 1, randNType(), false, true },
            { 10, randNType(), 1.0, randNType(), randomBoolean(), 10, randNType(), randomBoolean(), false },
            { 10, randNType(), 1, randNType(), randomBoolean(), 10.D, randNType(), randomBoolean(), false },
            { 10, randNType(), 10.0, randNType(), randomBoolean(), 1, randNType(), randomBoolean(), true },

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
            assertEquals("failed on test n " + i + ": " + Arrays.toString(test), test[8], range.areBoundariesInvalid());
        }
    }

    private static Literal l(Object value, DataType type) {
        return new Literal(Source.EMPTY, value, type);
    }

    private static ZonedDateTime d(String date) {
        return DateUtils.asDateTime(date);
    }

    private static DataType randNType() {
        return randomFrom(DataTypes.INTEGER, DataTypes.SHORT, DataTypes.LONG, DataTypes.UNSIGNED_LONG, DataTypes.FLOAT, DataTypes.DOUBLE);
    }
}

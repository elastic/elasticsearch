/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

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
                ZoneOffset.UTC
            );
            assertEquals(
                "failed on test " + i + ": " + Arrays.toString(test),
                test[8],
                range.areBoundariesInvalid(range.lower().fold(FoldContext.small()), range.upper().fold(FoldContext.small()))
            );
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

    /**
     * No value is ordered relative to {@code NaN}, so a range bounded by it is empty and must push down as a query matching
     * nothing, whichever bound it is.
     */
    public void testNaNBoundPushdown() {
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "field",
            new EsField("field", randomFrom(DOUBLE, INTEGER), Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal nan = new Literal(Source.EMPTY, Double.NaN, DOUBLE);
        Literal one = new Literal(Source.EMPTY, 1.0, DOUBLE);
        Query matchNone = new MatchAll(Source.EMPTY).negate(Source.EMPTY);
        for (Literal[] bounds : new Literal[][] {
            { nan, one },
            { one, nan },
            { nan, nan },
            { nan, Literal.NULL },
            { Literal.NULL, nan } }) {
            Range range = new Range(Source.EMPTY, field, bounds[0], randomBoolean(), bounds[1], randomBoolean(), null);
            assertEquals(matchNone, range.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER));
        }
    }

    /**
     * Lucene orders {@code -0.0} below {@code 0.0} on floating-point fields, while evaluation compares them as equal, so a zero
     * bound must push down as the zero that makes an inclusive bound take in both zeros and an exclusive bound leave both out.
     */
    public void testZeroBoundPushdown() {
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "field",
            new EsField("field", DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        for (boolean includeLower : new boolean[] { true, false }) {
            for (boolean includeUpper : new boolean[] { true, false }) {
                Literal lower = randomFrom(new Literal(Source.EMPTY, 0.0, DOUBLE), new Literal(Source.EMPTY, -0.0, DOUBLE));
                Literal upper = randomFrom(new Literal(Source.EMPTY, 0.0, DOUBLE), new Literal(Source.EMPTY, -0.0, DOUBLE));
                Range range = new Range(Source.EMPTY, field, lower, includeLower, upper, includeUpper, null);
                assertEquals(
                    new RangeQuery(
                        Source.EMPTY,
                        "field",
                        includeLower ? -0.0 : 0.0,
                        includeLower,
                        includeUpper ? 0.0 : -0.0,
                        includeUpper,
                        null
                    ),
                    range.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER)
                );
            }
        }
    }
}

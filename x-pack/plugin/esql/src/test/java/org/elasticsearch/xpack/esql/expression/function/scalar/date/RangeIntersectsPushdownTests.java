/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.compute.data.LongRangeBlockBuilder.LongRange;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.hamcrest.Matchers.equalTo;

public class RangeIntersectsPushdownTests extends ESTestCase {

    private static final long FROM = 1577836800000L; // 2020-01-01T00:00:00Z
    private static final long TO = 1609459200000L;   // 2021-01-01T00:00:00Z
    private static final long DATE = 1591574400000L; // 2020-06-08T00:00:00Z

    public void testNotTranslatableTwoDates() {
        // (date, date) lowers to Equals via surrogate; translatable() returns NO as a safety net.
        RangeIntersects fn = new RangeIntersects(Source.EMPTY, dateField("a"), dateLiteral(DATE));
        assertThat(fn.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testRecheckForDateRangeField() {
        // date_range fields use RECHECK so the evaluator can return null for multi-valued positions.
        RangeIntersects rangeLeft = new RangeIntersects(Source.EMPTY, rangeField("date_range"), rangeLiteral(FROM, TO));
        assertThat(rangeLeft.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.RECHECK));
        RangeIntersects dateRight = new RangeIntersects(Source.EMPTY, dateLiteral(DATE), rangeField("date_range"));
        assertThat(dateRight.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.RECHECK));
    }

    public void testNotTranslatableTwoLiterals() {
        RangeIntersects fn = new RangeIntersects(Source.EMPTY, dateLiteral(DATE), rangeLiteral(FROM, TO));
        assertThat(fn.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testDateFieldWithLiteralRange() {
        RangeIntersects fn = new RangeIntersects(Source.EMPTY, dateField("hire_date"), rangeLiteral(FROM, TO));
        var query = fn.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
        assertThat(
            query,
            equalTo(
                new RangeQuery(
                    Source.EMPTY,
                    "hire_date",
                    dateTimeToString(FROM),
                    true,
                    dateTimeToString(TO),
                    false,
                    DEFAULT_DATE_TIME_FORMATTER.pattern(),
                    null,
                    ShapeRelation.INTERSECTS
                )
            )
        );
    }

    public void testRangeFieldWithLiteralDate() {
        RangeIntersects fn = new RangeIntersects(Source.EMPTY, rangeField("date_range"), dateLiteral(DATE));
        var query = fn.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
        assertThat(
            query,
            equalTo(
                new RangeQuery(
                    Source.EMPTY,
                    "date_range",
                    dateTimeToString(DATE),
                    true,
                    dateTimeToString(DATE),
                    true,
                    DEFAULT_DATE_TIME_FORMATTER.pattern(),
                    null,
                    ShapeRelation.INTERSECTS
                )
            )
        );
    }

    public void testRangeFieldWithLiteralRange() {
        RangeIntersects fn = new RangeIntersects(Source.EMPTY, rangeField("date_range"), rangeLiteral(FROM, TO));
        var query = fn.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);
        assertThat(
            query,
            equalTo(
                new RangeQuery(
                    Source.EMPTY,
                    "date_range",
                    dateTimeToString(FROM),
                    true,
                    dateTimeToString(TO),
                    false,
                    DEFAULT_DATE_TIME_FORMATTER.pattern(),
                    null,
                    ShapeRelation.INTERSECTS
                )
            )
        );
    }

    public void testSymmetricArgumentOrder() {
        // Field on the right; output query is the same as field-on-left.
        RangeIntersects left = new RangeIntersects(Source.EMPTY, rangeField("date_range"), dateLiteral(DATE));
        RangeIntersects right = new RangeIntersects(Source.EMPTY, dateLiteral(DATE), rangeField("date_range"));
        assertThat(
            right.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER),
            equalTo(left.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER))
        );
    }

    private static FieldAttribute dateField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static FieldAttribute rangeField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.DATE_RANGE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static Literal dateLiteral(long millis) {
        return new Literal(Source.EMPTY, millis, DataType.DATETIME);
    }

    private static Literal rangeLiteral(long from, long to) {
        return new Literal(Source.EMPTY, new LongRange(from, to), DataType.DATE_RANGE);
    }
}

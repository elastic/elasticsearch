/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
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

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for InRange pushdown to Lucene.
 * Validates that the function correctly determines when it can be pushed down
 * and generates the appropriate Lucene query.
 */
public class InRangeStaticTests extends ESTestCase {

    public void testLuceneQuery_AllLiterals_NonTranslatable() {
        // Both arguments are literals - cannot push down (no field to query)
        var function = new InRange(
            Source.EMPTY,
            new Literal(Source.EMPTY, 1000L, DataType.DATETIME),
            new Literal(Source.EMPTY, new LongRangeBlockBuilder.LongRange(500L, 1500L), DataType.DATE_RANGE)
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testLuceneQuery_NonFoldableRange_NonTranslatable() {
        // Range is a field attribute (not constant) - cannot push down
        var function = new InRange(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "hire_date",
                new EsField("hire_date", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "date_range",
                new EsField("date_range", DataType.DATE_RANGE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    public void testLuceneQuery_FieldAndConstantRange_Translatable() {
        // Date field + constant range - CAN push down
        long rangeFrom = 500L;
        long rangeTo = 1500L;

        var function = new InRange(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "hire_date",
                new EsField("hire_date", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new Literal(Source.EMPTY, new LongRangeBlockBuilder.LongRange(rangeFrom, rangeTo), DataType.DATE_RANGE)
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));

        var query = function.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        // Should generate a RangeQuery with inclusive bounds
        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, "hire_date", rangeFrom, true, rangeTo, true, null)));
    }

    public void testLuceneQuery_DateNanosField_Translatable() {
        // Test with DATE_NANOS field type
        long rangeFrom = 1000000000L; // nanos
        long rangeTo = 2000000000L;

        var function = new InRange(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "timestamp_nanos",
                new EsField("timestamp_nanos", DataType.DATE_NANOS, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new Literal(Source.EMPTY, new LongRangeBlockBuilder.LongRange(rangeFrom, rangeTo), DataType.DATE_RANGE)
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));

        var query = function.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        assertThat(query, equalTo(new RangeQuery(Source.EMPTY, "timestamp_nanos", rangeFrom, true, rangeTo, true, null)));
    }

    public void testLuceneQuery_ConstantDateAndRangeField_Translatable() {
        // Constant date + range field - CAN push down (new case!)
        long dateValue = 1000L;

        var function = new InRange(
            Source.EMPTY,
            new Literal(Source.EMPTY, dateValue, DataType.DATETIME),
            new FieldAttribute(
                Source.EMPTY,
                "date_range",
                new EsField("date_range", DataType.DATE_RANGE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));

        var query = function.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER);

        // Should generate a RangeQuery with CONTAINS relation to check if range field contains the date point
        assertThat(
            query,
            equalTo(
                new RangeQuery(
                    Source.EMPTY,
                    "date_range",
                    dateValue,  // lower bound (same as upper for point)
                    true,        // include lower
                    dateValue,  // upper bound (same as lower for point)
                    true,        // include upper
                    null,        // format
                    null,        // zoneId
                    ShapeRelation.CONTAINS  // relation: check if range field contains this point
                )
            )
        );
    }

    public void testLuceneQuery_BothFields_NonTranslatable() {
        // Both are fields - cannot push down (requires per-document evaluation)
        var function = new InRange(
            Source.EMPTY,
            new FieldAttribute(
                Source.EMPTY,
                "hire_date",
                new EsField("hire_date", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            new FieldAttribute(
                Source.EMPTY,
                "date_range",
                new EsField("date_range", DataType.DATE_RANGE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            )
        );

        assertThat(function.translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }
}

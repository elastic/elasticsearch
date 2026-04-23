/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport.Pushability;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;

/**
 * Tests for {@link ParquetRsAggregatePushdownSupport}. Verifies the predicate
 * advertises a form as pushable iff
 * {@code PushAggregatesToExternalSource.resolveFromStats} can actually answer it
 * from file-level statistics.
 */
public class ParquetRsAggregatePushdownSupportTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private final ParquetRsAggregatePushdownSupport support = new ParquetRsAggregatePushdownSupport();

    // --- COUNT ---

    public void testCountStarPushable() {
        // COUNT(*) parses to Count over a foldable literal.
        assertEquals(Pushability.YES, push(new Count(SRC, Literal.keyword(SRC, "*"))));
    }

    public void testCountLiteralPushable() {
        assertEquals(Pushability.YES, push(new Count(SRC, new Literal(SRC, 1, DataType.INTEGER))));
    }

    public void testCountAttributePushable() {
        assertEquals(Pushability.YES, push(new Count(SRC, intField("age"))));
    }

    public void testCountComplexExpressionNotPushable() {
        // COUNT(LENGTH(name)) cannot be answered from file stats: the predicate must reject it
        // even though the executor would harmlessly skip pushdown at runtime.
        Expression complex = new Length(SRC, kwField("name"));
        assertEquals(Pushability.NO, push(new Count(SRC, complex)));
    }

    public void testCountWithFilterNotPushable() {
        Expression filter = new GreaterThan(SRC, intField("age"), new Literal(SRC, 18, DataType.INTEGER));
        Count countWithFilter = new Count(SRC, intField("age"), filter, AggregateFunction.NO_WINDOW);
        assertEquals(Pushability.NO, push(countWithFilter));
    }

    // --- MIN ---

    public void testMinAttributePushable() {
        assertEquals(Pushability.YES, push(new Min(SRC, intField("age"))));
    }

    public void testMinComplexExpressionNotPushable() {
        Expression complex = new Length(SRC, kwField("name"));
        assertEquals(Pushability.NO, push(new Min(SRC, complex)));
    }

    public void testMinWithFilterNotPushable() {
        Expression filter = new GreaterThan(SRC, intField("age"), new Literal(SRC, 18, DataType.INTEGER));
        Min minWithFilter = new Min(SRC, intField("age"), filter, AggregateFunction.NO_WINDOW);
        assertEquals(Pushability.NO, push(minWithFilter));
    }

    // --- MAX ---

    public void testMaxAttributePushable() {
        assertEquals(Pushability.YES, push(new Max(SRC, intField("age"))));
    }

    public void testMaxComplexExpressionNotPushable() {
        Expression complex = new Length(SRC, kwField("name"));
        assertEquals(Pushability.NO, push(new Max(SRC, complex)));
    }

    public void testMaxWithFilterNotPushable() {
        Expression filter = new GreaterThan(SRC, intField("age"), new Literal(SRC, 18, DataType.INTEGER));
        Max maxWithFilter = new Max(SRC, intField("age"), filter, AggregateFunction.NO_WINDOW);
        assertEquals(Pushability.NO, push(maxWithFilter));
    }

    // --- Other aggregates ---

    public void testSumNotPushable() {
        // The Parquet stats API exposes min/max/null-count but not sum; SUM is not pushable.
        assertEquals(Pushability.NO, push(new Sum(SRC, intField("age"))));
    }

    // --- Combinations ---

    public void testGroupingsBlockPushdown() {
        // Even a fully pushable aggregate cannot be pushed if there are GROUP BY clauses,
        // because the file-level stats roll up everything to a single row.
        assertEquals(
            Pushability.NO,
            support.canPushAggregates(List.of(new Count(SRC, Literal.keyword(SRC, "*"))), List.of(intField("age")))
        );
    }

    public void testAllPushableAggregatesPushable() {
        assertEquals(
            Pushability.YES,
            support.canPushAggregates(
                List.of(new Count(SRC, Literal.keyword(SRC, "*")), new Min(SRC, intField("age")), new Max(SRC, intField("age"))),
                List.of()
            )
        );
    }

    public void testMixedPushableAndUnpushableNotPushable() {
        Expression complex = new Length(SRC, kwField("name"));
        assertEquals(Pushability.NO, support.canPushAggregates(List.of(new Min(SRC, intField("age")), new Count(SRC, complex)), List.of()));
    }

    public void testEmptyAggregatesPushable() {
        // Vacuously true: no aggregates to push means nothing to reject.
        assertEquals(Pushability.YES, support.canPushAggregates(List.of(), List.of()));
    }

    // --- Helpers ---

    private Pushability push(Expression aggregate) {
        return support.canPushAggregates(List.of(aggregate), List.of());
    }

    private static FieldAttribute intField(String name) {
        return field(name, DataType.INTEGER);
    }

    private static FieldAttribute kwField(String name) {
        return field(name, DataType.KEYWORD);
    }

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(SRC, name, new EsField(name, dataType, Collections.emptyMap(), true, TimeSeriesFieldType.NONE));
    }
}

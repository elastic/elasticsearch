/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link OrcFilterPushdownSupport} verifying RECHECK semantics.
 * The critical invariant: remainder always contains ALL original filters,
 * ensuring FilterExec is never removed from the execution plan.
 */
public class OrcFilterPushdownSupportTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;
    private final OrcFilterPushdownSupport support = new OrcFilterPushdownSupport();

    public void testAllPushableRemainderContainsAllFilters() {
        // Even when all expressions can be pushed, remainder must contain all of them
        Expression eq = eq("age", DataType.INTEGER, 30);
        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(eq));

        assertTrue("Should have pushed filter", result.hasPushedFilter());
        assertThat(result.pushedFilter(), instanceOf(OrcPushedExpressions.class));
        assertTrue("Remainder must not be empty", result.hasRemainder());
        assertEquals("Remainder must contain all original filters", 1, result.remainder().size());
    }

    public void testMixedPushableAndUnpushable() {
        Expression pushable = eq("age", DataType.INTEGER, 30);
        Expression unpushable = eq("data", DataType.UNSUPPORTED, "x");

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(pushable, unpushable));

        assertTrue("Should have pushed filter", result.hasPushedFilter());
        assertTrue("Should have remainder", result.hasRemainder());
        assertEquals("Remainder must contain ALL original filters", 2, result.remainder().size());
    }

    public void testAllUnpushable() {
        Expression unpushable = eq("data", DataType.UNSUPPORTED, "x");

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(unpushable));

        assertFalse("Should not have pushed filter", result.hasPushedFilter());
        assertTrue("Should have remainder", result.hasRemainder());
        assertEquals(1, result.remainder().size());
    }

    public void testMultiplePushableRemainderContainsAll() {
        Expression a = eq("age", DataType.INTEGER, 30);
        Expression b = eq("name", DataType.KEYWORD, new BytesRef("Alice"));

        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(a, b));

        assertTrue("Should have pushed filter", result.hasPushedFilter());
        assertEquals("Remainder must contain ALL original filters", 2, result.remainder().size());
    }

    public void testCanPushReturnsRecheck() {
        Expression eq = eq("age", DataType.INTEGER, 30);
        assertEquals(FilterPushdownSupport.Pushability.RECHECK, support.canPush(eq));
    }

    public void testCanPushReturnsNoForUnsupported() {
        Expression eq = eq("data", DataType.UNSUPPORTED, "x");
        assertEquals(FilterPushdownSupport.Pushability.NO, support.canPush(eq));
    }

    public void testEmptyFilterList() {
        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of());
        assertFalse(result.hasPushedFilter());
    }

    // --- Helpers ---

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(SOURCE, name, new EsField(name, dataType, Collections.emptyMap(), true, TimeSeriesFieldType.NONE));
    }

    private static Expression eq(String fieldName, DataType type, Object value) {
        Object literalValue = value;
        if (value instanceof String s) {
            literalValue = new BytesRef(s);
        }
        return new Equals(SOURCE, field(fieldName, type), new Literal(SOURCE, literalValue, type));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ExpressionTranslatorsTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    public void testRangeWithNonFoldableBoundsOnNestedFieldReturnsNestedQuery() {
        EsField nestedEsField = new EsField("nested", DataTypes.NESTED, Collections.emptyMap(), false);
        FieldAttribute nestedParent = new FieldAttribute(SOURCE, "nested", nestedEsField);

        EsField dateEsField = new EsField("date", DataTypes.DATETIME, Collections.emptyMap(), true);
        FieldAttribute nestedDateField = new FieldAttribute(SOURCE, nestedParent, "nested.date", dateEsField);

        assertTrue(nestedDateField.isNested());
        assertEquals("nested", nestedDateField.nestedParent().name());

        // Use the field itself as lower bound to make it non-foldable
        Expression nonFoldableLower = nestedDateField;
        Expression foldableUpper = new Literal(SOURCE, ZonedDateTime.now(ZoneOffset.UTC), DataTypes.DATETIME);

        Range range = new Range(SOURCE, nestedDateField, nonFoldableLower, true, foldableUpper, true, ZoneOffset.UTC);
        assertFalse(range.lower().foldable());
        assertTrue(range.upper().foldable());

        Query result = ExpressionTranslators.Ranges.doTranslate(range, new QlTranslatorHandler());

        assertThat(result, instanceOf(NestedQuery.class));
        String queryString = result.asBuilder().toString();
        assertThat(queryString, containsString("\"nested\""));
        assertThat(queryString, containsString("\"script\""));
    }

    public void testRangeWithNonFoldableBoundsOnNonNestedFieldReturnsScriptQuery() {
        EsField dateEsField = new EsField("date", DataTypes.DATETIME, Collections.emptyMap(), true);
        FieldAttribute dateField = new FieldAttribute(SOURCE, "date", dateEsField);

        assertFalse(dateField.isNested());

        Expression nonFoldableLower = dateField;
        Expression foldableUpper = new Literal(SOURCE, ZonedDateTime.now(ZoneOffset.UTC), DataTypes.DATETIME);

        Range range = new Range(SOURCE, dateField, nonFoldableLower, true, foldableUpper, true, ZoneOffset.UTC);

        Query result = ExpressionTranslators.Ranges.doTranslate(range, new QlTranslatorHandler());

        assertThat(result, instanceOf(ScriptQuery.class));
    }
}

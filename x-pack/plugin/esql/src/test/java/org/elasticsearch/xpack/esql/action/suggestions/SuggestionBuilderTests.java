/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlSuggestionsResponse.FieldSuggestion;
import org.elasticsearch.xpack.esql.action.suggestions.SuggestionBuilder.StatisticKind;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

public class SuggestionBuilderTests extends ESTestCase {

    private LogicalPlan analyze(String query) {
        LogicalPlan plan = analyzer().addEmployees("test").query(query);
        // Analysis adds a default LIMIT and emits a header warning; consume it.
        assertWarnings("No limit defined, adding default limit of [1000]");
        return plan;
    }

    public void testFieldsFromKeepSchema() {
        // The schema source for a pipe position is the whole plan (last command KEEP).
        LogicalPlan plan = analyze("FROM test | KEEP first_name, emp_no");
        Map<String, FieldSuggestion> fields = SuggestionBuilder.fieldsFromSchema(plan);
        assertEquals(2, fields.size());
        assertEquals("keyword", fields.get("first_name").type());
        assertEquals("integer", fields.get("emp_no").type());
        assertNull(fields.get("first_name").values());
        assertNull(fields.get("first_name").range());
    }

    public void testFieldsFromFullSchemaContainExpectedTypes() {
        LogicalPlan plan = analyze("FROM test");
        Map<String, FieldSuggestion> fields = SuggestionBuilder.fieldsFromSchema(plan);
        assertEquals("keyword", fields.get("first_name").type());
        assertEquals("text", fields.get("gender").type());
        assertEquals("date", fields.get("hire_date").type());
    }

    public void testStatisticClassification() {
        assertEquals(StatisticKind.VALUES, SuggestionBuilder.statisticFor(DataType.KEYWORD));
        assertEquals(StatisticKind.VALUES, SuggestionBuilder.statisticFor(DataType.BOOLEAN));
        assertEquals(StatisticKind.VALUES, SuggestionBuilder.statisticFor(DataType.IP));
        assertEquals(StatisticKind.RANGE, SuggestionBuilder.statisticFor(DataType.DATETIME));
        assertEquals(StatisticKind.RANGE, SuggestionBuilder.statisticFor(DataType.LONG));
        assertEquals(StatisticKind.RANGE, SuggestionBuilder.statisticFor(DataType.INTEGER));
        assertEquals(StatisticKind.RANGE, SuggestionBuilder.statisticFor(DataType.DOUBLE));
        assertEquals(StatisticKind.RANGE, SuggestionBuilder.statisticFor(DataType.FLOAT));
        assertEquals(StatisticKind.NONE, SuggestionBuilder.statisticFor(DataType.TEXT));
    }
}

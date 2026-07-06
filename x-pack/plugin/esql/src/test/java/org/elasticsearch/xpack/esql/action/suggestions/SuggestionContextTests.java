/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.suggestions.SuggestionContext.Kind;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

/**
 * Verifies completion-context detection against fully analyzed plans built from the "test" index
 * (mapping-basic.json: {@code first_name}/{@code last_name} keyword, {@code emp_no}/{@code salary}
 * integer, {@code gender} text, ...). Sources on the analyzed plan are pristine (the optimizer is
 * intentionally not run here — the note guarantees source positions survive on the original nodes,
 * and detection only needs those original nodes).
 */
public class SuggestionContextTests extends ESTestCase {

    private LogicalPlan analyze(String query) {
        LogicalPlan plan = analyzer().addEmployees("test").query(query);
        // Analysis adds a default LIMIT and emits a header warning; consume it so the base-class
        // no-warnings check passes.
        assertWarnings("No limit defined, adding default limit of [1000]");
        return plan;
    }

    private SuggestionContext detect(String query, int cursor) {
        LogicalPlan plan = analyze(query);
        return SuggestionContext.detect(plan, new CursorLocation(query), cursor);
    }

    public void testCursorInStringLiteralEquality() {
        String query = "FROM test | WHERE first_name == \"Ale\"";
        int cursor = query.indexOf("Ale") + 1; // inside the string literal
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.STRING_LITERAL_EQUALITY, context.kind());
        assertEquals("first_name", context.targetField());
    }

    public void testCursorOnNumericLiteralRange() {
        String query = "FROM test | WHERE salary > 500";
        int cursor = query.indexOf("500") + 1; // on the numeric literal
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.NUMERIC_LITERAL_RANGE, context.kind());
        assertEquals("salary", context.targetField());
    }

    public void testCursorAtFieldNameSlotInWhere() {
        String query = "FROM test | WHERE first_name == \"x\"";
        int cursor = query.indexOf("first_name") + 3; // on the field name, not the literal
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.FIELD_NAME, context.kind());
        assertNull(context.targetField());
    }

    public void testCursorAtPipePosition() {
        String query = "FROM test | KEEP first_name\n";
        int cursor = query.length(); // past the last command
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.PIPE_POSITION, context.kind());
        assertNull(context.command());
    }

    public void testCursorInStringLiteralWithSupplementaryPlaneCharacter() {
        // U+1F600 GRINNING FACE sits inside the literal itself, before the cursor: 1 code point,
        // 2 UTF-16 units. This exercises the containment-range bug described in the suggestions
        // API spec directly, since the literal's own Source range must still contain the cursor.
        String emoji = "\uD83D\uDE00";
        String query = "FROM test | WHERE first_name == \"" + emoji + "Ale\"";
        int cursor = query.indexOf("Ale") + 1; // inside the string literal, after the emoji
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.STRING_LITERAL_EQUALITY, context.kind());
        assertEquals("first_name", context.targetField());
    }

    public void testCursorAtFieldNameWithSupplementaryPlaneCharacterBeforeIt() {
        // The emoji sits earlier in the query, before the cursor's target token, so every
        // downstream Source range must already be shifted correctly for this to resolve.
        String emoji = "\uD83D\uDE00";
        String query = "FROM test | WHERE first_name == \"" + emoji + "\" OR last_name == \"x\"";
        int cursor = query.indexOf("last_name") + 3; // on the field name, not the literal
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.FIELD_NAME, context.kind());
    }

    public void testCursorWithBmpNonAsciiCharacterBeforeIt() {
        // Accented Latin/CJK stay within the BMP: UTF-16 units and code points coincide, so this
        // already works without the code-point fix. Contrast with the supplementary-plane cases.
        String query = "FROM test | WHERE first_name == \"caf\u00e9\" OR last_name == \"x\"";
        int cursor = query.indexOf("last_name") + 3;
        SuggestionContext context = detect(query, cursor);
        assertEquals(Kind.FIELD_NAME, context.kind());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
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

    private SuggestionContext detect(CursorMarker marker) {
        LogicalPlan plan = analyze(marker.query());
        return SuggestionContext.detect(plan, new CursorLocation(marker.query()), marker.cursor());
    }

    public void testCursorInStringLiteralEquality() {
        // Cursor sits inside the string literal.
        SuggestionContext context = detect(CursorMarker.of("FROM test | WHERE first_name == \"A<*>le\""));
        assertEquals(Kind.STRING_LITERAL_EQUALITY, context.kind());
        assertEquals("first_name", context.targetField());
    }

    public void testCursorOnNumericLiteralRange() {
        // Cursor sits on the numeric literal.
        SuggestionContext context = detect(CursorMarker.of("FROM test | WHERE salary > 5<*>00"));
        assertEquals(Kind.NUMERIC_LITERAL_RANGE, context.kind());
        assertEquals("salary", context.targetField());
    }

    public void testCursorAtFieldNameSlotInWhere() {
        // Cursor sits on the field name, not the literal.
        SuggestionContext context = detect(CursorMarker.of("FROM test | WHERE fir<*>st_name == \"x\""));
        assertEquals(Kind.FIELD_NAME, context.kind());
        assertNull(context.targetField());
    }

    public void testCursorAtPipePosition() {
        // Cursor is past the last command; "end of string" is inherently a trailing marker.
        SuggestionContext context = detect(CursorMarker.of("FROM test | KEEP first_name\n<*>"));
        assertEquals(Kind.PIPE_POSITION, context.kind());
        assertNull(context.command());
    }

    public void testCursorInStringLiteralWithSupplementaryPlaneCharacter() {
        // U+1F600 GRINNING FACE sits inside the literal itself, before the cursor: 1 code point,
        // 2 UTF-16 units. This exercises the containment-range bug described in the suggestions
        // API spec directly, since the literal's own Source range must still contain the cursor.
        String emoji = "\uD83D\uDE00";
        SuggestionContext context = detect(CursorMarker.of("FROM test | WHERE first_name == \"" + emoji + "A<*>le\""));
        assertEquals(Kind.STRING_LITERAL_EQUALITY, context.kind());
        assertEquals("first_name", context.targetField());
    }

    public void testCursorAtFieldNameWithSupplementaryPlaneCharacterBeforeIt() {
        // The emoji sits earlier in the query, before the cursor's target token, so every
        // downstream Source range must already be shifted correctly for this to resolve.
        String emoji = "\uD83D\uDE00";
        SuggestionContext context = detect(CursorMarker.of("FROM test | WHERE first_name == \"" + emoji + "\" OR las<*>t_name == \"x\""));
        assertEquals(Kind.FIELD_NAME, context.kind());
    }

    public void testCursorWithBmpNonAsciiCharacterBeforeIt() {
        // Accented Latin/CJK stay within the BMP: UTF-16 units and code points coincide, so this
        // already works without the code-point fix. Contrast with the supplementary-plane cases.
        SuggestionContext context = detect(CursorMarker.of("FROM test | WHERE first_name == \"caf\u00e9\" OR las<*>t_name == \"x\""));
        assertEquals(Kind.FIELD_NAME, context.kind());
    }

    public void testCursorInStringLiteralEqualityAcrossMultipleLines() {
        // The query spans 3 lines and the cursor sits on a non-first line, inside the string
        // literal — the case most likely to break if line/column translation (CursorLocation) and
        // context detection disagree about newline handling.
        SuggestionContext context = detect(CursorMarker.of("FROM test\n| WHERE first_name == \"<*>Ale\"\n| KEEP first_name"));
        assertEquals(Kind.STRING_LITERAL_EQUALITY, context.kind());
        assertEquals("first_name", context.targetField());
    }

    /**
     * IP_LOCATION-bearing plan built via ROW (no index needed, per {@code AnalyzerTests}'s own pattern for
     * this command): the analyzer test fixture resolves IP_LOCATION's function/database schema statically
     * ({@code EsqlTestUtils#TEST_IP_LOCATION_RESOLUTION}), so this needs no real .mmdb lookup and stays in
     * the "complex tier" as originally scoped, rather than being scaled back.
     */
    public void testFieldNameContextAroundIpLocationCommand() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        CursorMarker marker = CursorMarker.of("ROW ip = \"1.2.3.4\" | IP_LOCATION g = ip | KEEP g.cou<*>ntry_iso_code");
        LogicalPlan plan = analyzer().query(marker.query());
        assertWarnings("No limit defined, adding default limit of [1000]");
        SuggestionContext context = SuggestionContext.detect(plan, new CursorLocation(marker.query()), marker.cursor());
        assertEquals(Kind.FIELD_NAME, context.kind());
        assertNull(context.targetField());

        var fields = SuggestionBuilder.fieldsFromSchema(context.schemaSource());
        assertEquals("keyword", fields.get("g.country_iso_code").type());
        assertEquals("keyword", fields.get("g.city_name").type());
    }

    public void testPipePositionContextAfterIpLocationCommand() {
        assumeTrue("requires ip_location command capability", EsqlCapabilities.Cap.IP_LOCATION_COMMAND.isEnabled());
        String query = "ROW ip = \"1.2.3.4\" | IP_LOCATION g = ip\n";
        LogicalPlan plan = analyzer().query(query);
        assertWarnings("No limit defined, adding default limit of [1000]");
        SuggestionContext context = SuggestionContext.detect(plan, new CursorLocation(query), query.length());
        assertEquals(Kind.PIPE_POSITION, context.kind());

        var fields = SuggestionBuilder.fieldsFromSchema(context.schemaSource());
        assertEquals("keyword", fields.get("g.country_iso_code").type());
        assertEquals("geo_point", fields.get("g.location").type());
    }
}

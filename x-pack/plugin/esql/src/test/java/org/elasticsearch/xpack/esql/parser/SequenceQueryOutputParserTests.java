/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.containsString;

/** Invalid declarations fail before searches rather than silently selecting a different field context. */
public class SequenceQueryOutputParserTests extends ESTestCase {
    private static final String BASE = "SEQUENCE BY key WITH {\"maxspan\":\"1m\"} PRIMARY 2 (FROM a), (FROM b)";

    public void testPrimaryReferenceUsesOrdinaryNames() {
        rejected(BASE + " | EVAL x = MEMBER(2, user.name)", "Use ordinary field names");
        rejected(BASE + " | EVAL user.name = \"edit\" | EVAL x = MEMBER(2, user.name)", "Use ordinary field names");
        rejected(BASE + " | RENAME user.name AS actor | EVAL x = MEMBER(2, user.name)", "Use ordinary field names");
    }

    public void testPrimaryKeywordDoesNotReserveFieldName() {
        assertNotNull(TEST_PARSER.createStatement(BASE.replace("BY key", "BY primary")));
        assertNotNull(TEST_PARSER.createStatement(BASE.replace("BY key", "BY primary").replace(" PRIMARY 2", "")));
        rejected(BASE.replace("PRIMARY 2", "UNRELATED 2"), "Expected PRIMARY");
    }

    public void testLiteralMemberAndFieldRequired() {
        rejected(BASE + " | EVAL x = MEMBER(1 + 0, user.name)", "literal member number");
        rejected(BASE + " | EVAL x = MEMBER(1, \"user.name\")", "field identifier");
        rejected(BASE + " | EVAL x = MEMBER(1, TO_LOWER(user.name))", "field identifier");
        rejected(BASE + " | EVAL x = MEMBER(0, user.name)", "one-based member");
        rejected(BASE + " | EVAL x = MEMBER(3, user.name)", "one-based member");
    }

    public void testExplicitPrimaryAndTerminalDeclarationRequired() {
        rejected(BASE.replace(" PRIMARY 2", "") + " | RETURN MATCHES", "explicit PRIMARY");
        rejected(BASE.replace(" PRIMARY 2", "") + " | EVAL x = MEMBER(1, user.name)", "explicit PRIMARY");
        rejected(BASE.replace("PRIMARY 2", "PRIMARY 0"), "one-based member");
        rejected(BASE.replace("PRIMARY 2", "PRIMARY 3"), "one-based member");
        rejected(BASE + " | RETURN MATCHES | LIMIT 1", "final command");
        rejected(BASE + " | RETURN MATCHES | RETURN MATCHES", "final command");
    }

    public void testUnsupportedGrainAndNestedMemberFailClosed() {
        rejected(BASE + " | STATS c = COUNT(*) | RETURN MATCHES", "outside the sequence output prototype");
        rejected(BASE.replace("(FROM a)", "(FROM a | EVAL x = MEMBER(1, user.name))"), "only available after SEQUENCE");
        rejected("FROM a | RETURN MATCHES", "explicit PRIMARY");
    }

    /** Selecting a default member namespace does not impose retained-evidence grain restrictions. */
    public void testPrimaryPresentationAllowsOrdinaryUnaryCommands() {
        assertNotNull(TEST_PARSER.createStatement(BASE + " | STATS c = COUNT(*)"));
        assertNotNull(TEST_PARSER.createStatement(BASE + " | MV_EXPAND key"));
    }

    /** Event transforms remain available without capture, while complete-match retention stays on simple stages. */
    public void testEvidenceStageBoundaryRemainsNarrow() {
        for (String transform : new String[] { "MV_EXPAND key", "DISSECT key \"%{extracted}\"", "GROK key \"%{WORD:extracted}\"" }) {
            String query = BASE.replace("(FROM a)", "(FROM a | " + transform + ")");
            assertNotNull(TEST_PARSER.createStatement(query));
            var error = expectThrows(IllegalArgumentException.class, () -> TEST_PARSER.createStatement(query + " | RETURN MATCHES"));
            assertThat(error.getMessage(), containsString("outside sequence output stage boundary"));
        }
    }

    private static void rejected(String query, String message) {
        var error = expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement(query));
        assertThat(error.getMessage(), containsString(message));
    }
}

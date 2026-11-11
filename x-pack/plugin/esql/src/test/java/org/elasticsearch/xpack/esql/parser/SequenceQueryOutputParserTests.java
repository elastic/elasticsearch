/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Invalid declarations fail before searches rather than silently selecting a different field context. */
public class SequenceQueryOutputParserTests extends ESTestCase {
    private static final EsqlParser TEST_PARSER = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));
    private static final String BASE = "SEQUENCE BY key WITH {\"maxspan\":\"1m\"} PRIMARY 2 (FROM a), (FROM b)";

    public void testPrimaryReferenceUsesOrdinaryNames() {
        rejected(BASE + " | EVAL x = MEMBER(2, user.name)", "Use ordinary field names");
        rejected(BASE + " | EVAL user.name = \"edit\" | EVAL x = MEMBER(2, user.name)", "Use ordinary field names");
        rejected(BASE + " | RENAME user.name AS actor | EVAL x = MEMBER(2, user.name)", "Use ordinary field names");
    }

    public void testPrimaryKeywordDoesNotReserveFieldName() {
        assertNotNull(TEST_PARSER.createStatement(BASE.replace("BY key", "BY primary")));
        assertNotNull(TEST_PARSER.createStatement(BASE + " | EVAL selected = primary"));
        rejected(BASE.replace("PRIMARY 2", "UNRELATED 2"), "Expected PRIMARY");
    }

    /** Every sequence selects its ordinary field context, including queries without retained evidence. */
    public void testMissingPrimaryRejected() {
        String sequence = BASE.replace(" PRIMARY 2", "");
        for (String suffix : new String[] { "", " | KEEP stage1.*", " | RETURN MATCHES", " | EVAL x = MEMBER(1, user.name)" }) {
            rejected(sequence + suffix, "SEQUENCE requires an explicit PRIMARY member");
        }
    }

    public void testLiteralMemberAndFieldRequired() {
        rejected(BASE + " | EVAL x = MEMBER(1 + 0, user.name)", "literal member number");
        rejected(BASE + " | EVAL x = MEMBER(1, \"user.name\")", "field identifier");
        rejected(BASE + " | EVAL x = MEMBER(1, TO_LOWER(user.name))", "field identifier");
        rejected(BASE + " | EVAL x = MEMBER(0, user.name)", "one-based member");
        rejected(BASE + " | EVAL x = MEMBER(3, user.name)", "one-based member");
    }

    public void testExplicitPrimaryAndTerminalDeclarationRequired() {
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
        for (String[] transform : new String[][] {
            { "MV_EXPAND key", "MvExpand" },
            { "DISSECT key \"%{extracted}\"", "Dissect" },
            { "GROK key \"%{WORD:extracted}\"", "Grok" } }) {
            String query = BASE.replace("(FROM a)", "(FROM a\n  | " + transform[0] + ")");
            assertNotNull(TEST_PARSER.createStatement(query));
            var error = expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement(query + " | RETURN MATCHES"));
            assertEquals("outside sequence output stage boundary: " + transform[1], error.getErrorMessage());
            assertEquals(2, error.getLineNumber());
            assertEquals(5, error.getColumnNumber());
        }
    }

    /** PRIMARY declarations inside expression-held subqueries must fail before transient nodes reach analysis. */
    public void testSequenceInMembershipSubqueryIsRejected() {
        for (String retention : new String[] { "", " | RETURN MATCHES" }) {
            String sequence = "FROM (" + BASE + retention + ")";
            String scalar = "key IN (" + sequence + " | KEEP key)";
            String tuple = "(key, user.name) NOT IN (" + sequence + " | KEEP key, user.name)";
            for (String query : new String[] {
                "FROM a | WHERE " + scalar,
                "FROM a\n| WHERE " + tuple,
                "FROM a | WHERE key IN (FROM b | WHERE " + scalar + " | KEEP key)",
                "FROM a | WHERE (key, user.name) IN (FROM b | WHERE " + tuple + " | KEEP key, user.name)",
                BASE.replace("(FROM a)", "(FROM a | WHERE " + scalar + ")"),
                "EXPLAIN (FROM a | WHERE " + scalar + ")" }) {
                var error = expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement(query));
                assertThat(error.getErrorMessage(), containsString("SEQUENCE is not supported as an IN subquery source"));
                assertEquals(query.contains("\n") ? 2 : 1, error.getLineNumber());
                assertTrue("The nested sequence must have a source column", error.getColumnNumber() > 0);
            }
        }
    }

    /** Ordinary membership filters remain available both before matching and after PRIMARY presentation. */
    public void testOrdinaryMembershipSubqueriesRemainAvailable() {
        for (String predicate : new String[] { "key IN (FROM c | KEEP key)", "(key, user.name) NOT IN (FROM c | KEEP key, user.name)" }) {
            assertNotNull(TEST_PARSER.createStatement(BASE + " | WHERE " + predicate));
            String stages = BASE.replace("(FROM a)", "(FROM a | WHERE " + predicate + ")");
            assertNotNull(TEST_PARSER.createStatement(stages));
            assertNotNull(TEST_PARSER.createStatement(stages + " | RETURN MATCHES"));
        }
    }

    /** A declaration hidden in a LET body must fail before the binding is expanded into the main query. */
    public void testLetBoundPrimaryIsRejectedBeforeExpansion() {
        rejectedLetBody(BASE);
    }

    /** Retention hidden inside a binding has the same explicit LET boundary as a top-level declaration. */
    public void testLetBoundRetainedMatchesIsRejectedBeforeExpansion() {
        rejectedLetBody(BASE + " | RETURN MATCHES");
    }

    /** MEMBER cannot hide in an otherwise ordinary binding and reach ordinary function resolution later. */
    public void testLetBoundMemberIsRejectedBeforeExpansion() {
        rejectedLetBody("FROM a | EVAL supporting = MEMBER(1, user.name)");
    }

    /** A declaration inside an expression-held subquery is still inside the unsupported LET body. */
    public void testLetBoundOutputHiddenInMembershipSubqueriesIsRejected() {
        rejectedLetBody("FROM a | WHERE key IN (FROM (" + BASE + ") | KEEP key)");
        rejectedLetBody("FROM a | WHERE (key, user.name) IN (FROM (" + BASE + ") | KEEP key, user.name)");
    }

    /** MEMBER calls in a membership RHS must not evade the same LET boundary as calls in the binding's main pipeline. */
    public void testLetBoundMemberHiddenInMembershipSubqueryIsRejected() {
        rejectedLetBody("FROM a | WHERE key IN (FROM b | EVAL key = MEMBER(1, user.name) | KEEP key)");
    }

    /** Ordinary LET remains available; selecting PRIMARY does not expand the sequence LET boundary. */
    public void testOrdinaryLetBindingsRemainAvailable() {
        var ordinary = TEST_PARSER.createStatement("LET s = (FROM a | KEEP key); FROM s");
        assertEquals(1, ordinary.letBindings().size());
        assertEquals("s", ordinary.letBindings().getFirst().name());
        assertTrue(ordinary.letBindings().getFirst().plan().collect(Sequence.class).isEmpty());
        rejectedLetBody(BASE);
        rejected("LET s = (" + BASE.replace(" PRIMARY 2", "") + "); FROM s", "SEQUENCE requires an explicit PRIMARY member");
    }

    private static void rejectedLetBody(String body) {
        var error = expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement("LET s = (" + body + "); FROM s"));
        assertThat(error.getErrorMessage(), containsString("LET is outside the sequence output prototype"));
        assertEquals(1, error.getLineNumber());
        assertTrue("The hidden declaration must have a source column", error.getColumnNumber() > 0);
    }

    private static void rejected(String query, String message) {
        var error = expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement(query));
        assertThat(error.getMessage(), containsString(message));
    }
}

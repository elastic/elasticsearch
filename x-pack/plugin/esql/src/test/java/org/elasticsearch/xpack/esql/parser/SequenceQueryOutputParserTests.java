/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceMatchOutput;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Invalid declarations fail before searches rather than silently selecting a different field context. */
public class SequenceQueryOutputParserTests extends ESTestCase {
    private static final EsqlParser TEST_PARSER = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));
    private static final String BASE = "SEQUENCE BY key WITH {\"maxspan\":\"1m\"} PRIMARY 2 (FROM a), (FROM b)";

    private static final String NAMED_BASE = "SEQUENCE BY key WITH {\"maxspan\":\"1m\"} PRIMARY finish "
        + "(FROM a) AS start, (FROM b) AS finish";

    /** Named and numbered references share the same protected field binding after lowering. */
    public void testNamedAndPositionalReferencesShareBinding() {
        var plan = TEST_PARSER.createStatement(NAMED_BASE + " | EVAL named = MEMBER(start, user.name), numbered = MEMBER(1, user.name)")
            .plan();
        List<SequenceMatchOutput.MemberReference> references = new ArrayList<>();
        plan.forEachExpressionDown(SequenceMatchOutput.MemberReference.class, references::add);
        assertEquals(2, references.size());
        assertEquals(0, references.getFirst().stage());
        assertEquals("user.name", references.getFirst().field());
        assertEquals(references.getFirst().id(), references.getLast().id());
        assertEquals(1, plan.collect(SequenceMatchOutput.Scope.class).getFirst().primary());
        assertNotNull(TEST_PARSER.createStatement(NAMED_BASE.replace("PRIMARY finish", "PRIMARY 2")));
        assertNotNull(TEST_PARSER.createStatement(NAMED_BASE.replace("(FROM a) AS start", "(FROM a)") + " | EVAL x = MEMBER(1, key)"));
    }

    public void testNamedStageErrors() {
        rejected(NAMED_BASE.replace("AS finish", "AS start"), "Duplicate SEQUENCE stage name [start]");
        rejected(NAMED_BASE.replace("PRIMARY finish", "PRIMARY absent"), "Unknown SEQUENCE stage name [absent] in PRIMARY");
        rejected(NAMED_BASE + " | EVAL x = MEMBER(absent, key)", "Unknown SEQUENCE stage name [absent] in MEMBER");
        rejected(NAMED_BASE + " | EVAL x = MEMBER(Start, key)", "Unknown SEQUENCE stage name [Start] in MEMBER");
        rejected(NAMED_BASE.replace("PRIMARY finish", "PRIMARY Finish"), "Unknown SEQUENCE stage name [Finish] in PRIMARY");
        rejected(NAMED_BASE.replace("AS start", "UNRELATED start"), "Expected AS before the SEQUENCE stage name");
        rejected(NAMED_BASE.replace("AS start", "AS and"), "stage name [and] must be an unquoted field identifier");
        rejected(NAMED_BASE.replace(" PRIMARY finish", ""), "SEQUENCE requires an explicit PRIMARY member");
        rejected(NAMED_BASE + " | EVAL x = MEMBER(finish, key)", "Use ordinary field names for the PRIMARY member");
        rejected(NAMED_BASE + " | EVAL x = MEMBER(start.key, key)", "unquoted stage name");
        rejected(NAMED_BASE + " | EVAL x = MEMBER([start].[key], key)", "unquoted stage name");
        rejected(NAMED_BASE + " | EVAL x = MEMBER(\"start\", key)", "literal member number");
        rejected(NAMED_BASE + " | EVAL x = MEMBER(`start`, key)", "unquoted stage name");
        rejected(
            NAMED_BASE.replace("(FROM a)", "(FROM a | EVAL x = MEMBER(start, key))"),
            "only supported in the final WHERE of a SEQUENCE stage"
        );
    }

    /** A malformed stage declaration must be diagnosed before semantic checks use its supposed label. */
    public void testStageAliasKeywordPrecedesLabelledStageDiagnostics() {
        for (String query : List.of(
            NAMED_BASE.replace("AS start", "UNRELATED start WITHIN 1s"),
            NAMED_BASE.replace("AS start", "UNRELATED start BY key"),
            NAMED_BASE.replace("AS start", "AS start BY start_key").replace("AS finish", "UNRELATED finish BY finish_key, extra_key")
        )) {
            rejected(query, "Expected AS before the SEQUENCE stage name");
        }
    }

    /** An invalid identifier cannot become the stage label in an incoming-limit or duplicate-key message. */
    public void testStageAliasNamePrecedesLabelledStageDiagnostics() {
        for (String suffix : List.of(" WITHIN 1s", " BY key")) {
            rejected(NAMED_BASE.replace("AS start", "AS and" + suffix), "SEQUENCE stage name [and] must be an unquoted field identifier");
        }
    }

    /** A stage-local authoring error should identify the label the query author used. */
    public void testNamedStageDiagnosticsIdentifyTheAuthorLabel() {
        rejected(NAMED_BASE.replace("(FROM b)", "(FROM b | STATS total = COUNT(*))"), "is not supported inside a SEQUENCE stage [finish]");
        rejected(NAMED_BASE.replace("(FROM a)", "(ROW key = 1)"), "SEQUENCE stages must start with FROM; member [start]");
        rejected(
            NAMED_BASE.replace("AS start", "AS start BY start_key").replace("AS finish", "AS finish BY finish_key, extra_key"),
            "All SEQUENCE stages must declare the same number of local BY columns; member [finish]"
        );
        rejected(
            NAMED_BASE.replace("AS start", "AS start BY start_key").replace("AS finish", "AS finish BY key"),
            "Duplicate SEQUENCE BY column [key] in member [finish]"
        );
    }

    /** Request parameters must not become literal or named stage declarations during expression substitution. */
    public void testStageReferencesRejectParameters() {
        for (var parameter : List.of(
            new QueryParam("stage", "start", DataType.KEYWORD, ParserUtils.ParamClassification.IDENTIFIER),
            new QueryParam("stage", 1, DataType.INTEGER, ParserUtils.ParamClassification.VALUE)
        )) {
            var error = expectThrows(
                ParsingException.class,
                () -> TEST_PARSER.createStatement(NAMED_BASE + " | EVAL x = MEMBER(?stage, key)", new QueryParams(List.of(parameter)))
            );
            assertThat(error.getMessage(), containsString("MEMBER requires a literal member number"));
        }
        for (String alias : List.of("`start`", "start.part", "1", "true")) {
            expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement(NAMED_BASE.replace("AS start", "AS " + alias)));
        }
    }

    /** Aliases are scoped to each ordinary union branch, exactly like positional MEMBER references. */
    public void testNamedStageBranchAndSubqueryBoundaries() {
        String first = NAMED_BASE + " | EVAL evidence = MEMBER(start, key) | KEEP key, evidence";
        String second = NAMED_BASE.replace("PRIMARY finish", "PRIMARY start")
            + " | EVAL evidence = MEMBER(finish, key) | KEEP key, evidence";
        assertNotNull(TEST_PARSER.createStatement("FROM (" + first + "), (" + second + ") | STATS count = COUNT(*)"));
        rejected("FROM (" + first + "), (FROM c) | EVAL x = MEMBER(start, key)", "MEMBER cannot escape its SEQUENCE branch");
        rejected("FROM (" + first + "), (FROM c | EVAL x = MEMBER(start, key))", "MEMBER requires SEQUENCE with an explicit PRIMARY");
        rejected(
            "FROM (" + first + "), (" + NAMED_BASE.replace("AS start", "AS other") + " | EVAL x = MEMBER(start, key))",
            "Unknown SEQUENCE stage name [start] in MEMBER"
        );
        rejected(
            NAMED_BASE + " | WHERE key IN (FROM c | EVAL key = MEMBER(start, key) | KEEP key)",
            "MEMBER is not available inside an IN subquery"
        );
        rejectedLetBody(NAMED_BASE);
    }

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
        rejected(
            BASE.replace("(FROM a)", "(FROM a | EVAL x = MEMBER(1, user.name))"),
            "only supported in the final WHERE of a SEQUENCE stage"
        );
        rejected("FROM a | RETURN MATCHES", "explicit PRIMARY");
    }

    /** Selecting a default member namespace does not impose retained-evidence grain restrictions. */
    public void testPrimaryPresentationAllowsOrdinaryUnaryCommands() {
        assertNotNull(TEST_PARSER.createStatement(BASE + " | STATS c = COUNT(*)"));
        assertNotNull(TEST_PARSER.createStatement(BASE + " | MV_EXPAND key"));
    }

    /** Extraction preserves an event row and can keep its authentic originals for either primary. */
    public void testRetainedStagesAllowExtraction() {
        for (String extraction : new String[] { "DISSECT key \"%{extracted}\"", "GROK key \"%{WORD:extracted}\"" }) {
            for (int primary : new int[] { 1, 2 }) {
                String query = BASE.replace("PRIMARY 2", "PRIMARY " + primary)
                    .replace("(FROM a)", "(FROM a | " + extraction + " | KEEP key, extracted)");
                assertNotNull(TEST_PARSER.createStatement(query + " | RETURN MATCHES"));
            }
        }
    }

    /** Parsing inside an event stage does not grant outer commands a new retained-evidence grain. */
    public void testRetainedExtractionKeepsOuterBoundary() {
        for (String extraction : new String[] { "DISSECT key \"%{extracted}\"", "GROK key \"%{WORD:extracted}\"" }) {
            rejected(BASE + " | " + extraction + " | RETURN MATCHES", "outside the sequence output prototype");
        }
    }

    /** An ordinary union prepares each sequence branch before combining the public processed fields. */
    public void testOrdinaryUnionPreparesIndependentSequenceBranches() {
        String first = BASE + " | EVAL other = MEMBER(1, key) | KEEP key, other";
        String second = BASE.replace("PRIMARY 2", "PRIMARY 1") + " | EVAL other = MEMBER(2, key) | KEEP key, other";
        for (String branches : new String[] {
            "(" + first + "), (FROM c | KEEP key)",
            "(FROM c | KEEP key), (" + first + ")",
            "(" + first + "), (" + second + ")",
            "(" + second + "), (" + first + ")" }) {
            var plan = TEST_PARSER.createStatement("FROM " + branches + " | STATS c = COUNT(*) | SORT c | LIMIT 10").plan();
            assertTrue(plan.collect(SequenceMatchOutput.Declaration.class).isEmpty());
            assertEquals(plan.collect(Sequence.class).size(), plan.collect(SequenceMatchOutput.Scope.class).size());
            assertFalse(plan.collect(Sequence.class).isEmpty());
        }
    }

    /** Unsupported multi-input composition must not blame an absent RETURN MATCHES command. */
    public void testOrdinaryUnionCompositionNamesSequenceBoundary() {
        String union = "FROM (" + BASE + " | KEEP key), (" + BASE.replace("PRIMARY 2", "PRIMARY 1") + " | KEEP key)";
        assertNotNull(TEST_PARSER.createStatement(union + " | STATS c = COUNT(*)"));
        for (String command : new String[] { "LOOKUP JOIN lookup_index ON key", "FORK (WHERE key IS NOT NULL) (WHERE key IS NULL)" }) {
            var error = expectThrows(ParsingException.class, () -> TEST_PARSER.createStatement(union + " | " + command));
            assertThat(error.getErrorMessage(), containsString("SEQUENCE output is outside the prototype in this command composition"));
            assertFalse(error.getErrorMessage(), error.getErrorMessage().contains("RETURN MATCHES"));
        }
    }

    /** A sequence's MEMBER scope is unavailable to its sibling branch and to commands above the union. */
    public void testMemberCannotEscapeOrdinaryUnionBranch() {
        String union = "FROM (" + BASE + " | KEEP key), (FROM c | KEEP key)";
        rejected(union + " | EVAL other = MEMBER(1, key)", "MEMBER cannot escape its SEQUENCE branch");
        rejected("FROM (" + BASE + "), (FROM c | EVAL other = MEMBER(1, key))", "MEMBER requires SEQUENCE with an explicit PRIMARY");
        rejected(
            "FROM (" + BASE + "), (" + BASE.replace("PRIMARY 2", "PRIMARY 1") + " | EVAL other = MEMBER(1, key))",
            "Use ordinary field names for the PRIMARY member"
        );
    }

    /** Expression-held subqueries are separate scopes, including when their MEMBER call is hidden another IN deep. */
    public void testMemberCannotEscapeThroughMembershipSubquery() {
        String union = "FROM (" + BASE + " | KEEP key), (FROM c | KEEP key)";
        for (String predicate : new String[] {
            "key IN (FROM c | EVAL key = MEMBER(1, key) | KEEP key)",
            "(key, key) IN (FROM c | EVAL other = MEMBER(1, key) | KEEP key, other)",
            "key IN (FROM c | WHERE key IN (FROM d | EVAL key = MEMBER(1, key) | KEEP key) | KEEP key)" }) {
            rejected(union + " | WHERE " + predicate, "MEMBER is not available inside an IN subquery");
            rejected("FROM (" + BASE + "), (FROM c | WHERE " + predicate + ")", "MEMBER is not available inside an IN subquery");
        }
    }

    /** Retained originals have one match context and cannot become ordinary union rows. */
    public void testOrdinaryUnionRejectsRetainedMatchesBeforeLowering() {
        rejected("FROM (" + BASE + " | RETURN MATCHES), (FROM c)", "RETURN MATCHES is not supported inside an ordinary union");
        rejected("FROM (FROM c), (" + BASE + " | RETURN MATCHES)", "RETURN MATCHES is not supported inside an ordinary union");
        rejected("FROM (" + BASE + "), (FROM c) | RETURN MATCHES", "RETURN MATCHES cannot retain an ordinary union");
    }

    /** Preparing an outer branch does not authorize sequence declarations hidden in event stages or IN/LET bodies. */
    public void testUnionPreparationPreservesNestedRestrictions() {
        rejected(
            "FROM (" + BASE.replace("(FROM a)", "(FROM (" + BASE + "))") + "), (FROM c)",
            "outside the prototype in this command composition"
        );
        rejected(
            "FROM (" + BASE + "), (FROM c | WHERE key IN (FROM (" + BASE + ") | KEEP key))",
            "SEQUENCE is not supported as an IN subquery source"
        );
        rejected("LET s = (FROM c); FROM (" + BASE + "), (FROM s)", "LET is outside the sequence output prototype");
    }

    /** Expansion inside either retained stage does not grant outer expansion a retained-evidence grain. */
    public void testEvidenceStageBoundaryRemainsNarrow() {
        for (int primary : new int[] { 1, 2 }) {
            for (String expansion : new String[] { "MV_EXPAND key", "MV_EXPAND other", "MV_EXPAND other | MV_EXPAND key" }) {
                String query = BASE.replace("PRIMARY 2", "PRIMARY " + primary)
                    .replace("(FROM a)", "(FROM a\n  | " + expansion + ")")
                    .replace("(FROM b)", "(FROM b\n  | " + expansion + ")");
                assertNotNull(TEST_PARSER.createStatement(query));
                assertNotNull(TEST_PARSER.createStatement(query + " | RETURN MATCHES"));
            }
        }
        rejected(BASE + " | MV_EXPAND key | RETURN MATCHES", "outside the sequence output prototype");
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

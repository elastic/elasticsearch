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
    private static final String BASE = "SEQUENCE BY key WITHIN 1m PRIMARY 2 (FROM a), (FROM b)";

    private static final String NAMED_BASE = "SEQUENCE BY key WITHIN 1m PRIMARY finish " + "(FROM a) AS start, (FROM b) AS finish";

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

    /** The prototype reserves ECS 9.5.0 field-set names independently of the indices or fields in the query. */
    public void testEcsFieldSetStageNamesAreRejected() {
        for (String fieldSet : List.of(
            "process",
            "file",
            "host",
            "user",
            "network",
            "base",
            "as",
            "code_signature",
            "data_stream",
            "entity_reference",
            "gen_ai",
            "os",
            "risk",
            "tracing",
            "x509"
        )) {
            rejected(
                NAMED_BASE.replace("AS start", "AS " + fieldSet),
                "stage name [" + fieldSet + "] is an ECS field set name; name the stage for what the event does instead"
            );
        }
        rejected(NAMED_BASE.replace("PRIMARY finish", "PRIMARY host").replace("AS finish", "AS host"), "is an ECS field set name");
        assertNotNull(TEST_PARSER.createStatement(BASE + " | EVAL earlier=MEMBER(1,process.name)"));
        assertNotNull(TEST_PARSER.createStatement(NAMED_BASE.replace("AS start", "AS Process") + " | EVAL x=MEMBER(Process,process.name)"));
        assertNotNull(TEST_PARSER.createStatement(NAMED_BASE.replace("AS start", "AS label") + " | EVAL x=MEMBER(label,label)"));
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

    /** Named MEMBER calls remain local to the top-level sequence pipeline. */
    public void testNamedMemberCannotEscapeMembershipSubquery() {
        rejected(
            NAMED_BASE + " | WHERE key IN (FROM c | EVAL key = MEMBER(start, key) | KEEP key)",
            "MEMBER is not available inside an IN subquery"
        );
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

    /** Every sequence selects its ordinary field context, through ordinary commands. */
    public void testMissingPrimaryRejected() {
        String sequence = BASE.replace(" PRIMARY 2", "");
        for (String suffix : new String[] { "", " | KEEP stage1.*", " | EVAL x = MEMBER(1, user.name)" }) {
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

    public void testPrimaryRequiresExistingStage() {
        rejected(BASE.replace("PRIMARY 2", "PRIMARY 0"), "one-based member");
        rejected(BASE.replace("PRIMARY 2", "PRIMARY 3"), "one-based member");
    }

    public void testNestedMemberFailsClosed() {
        rejected(
            BASE.replace("(FROM a)", "(FROM a | EVAL x = MEMBER(1, user.name))"),
            "only supported in the final WHERE of a SEQUENCE stage"
        );
    }

    /** Selecting a primary namespace preserves ordinary unary commands. */
    public void testPrimaryPresentationAllowsOrdinaryUnaryCommands() {
        assertNotNull(TEST_PARSER.createStatement(BASE + " | STATS c = COUNT(*)"));
        assertNotNull(TEST_PARSER.createStatement(BASE + " | MV_EXPAND key"));
    }

    /** Extraction remains available in either selected primary stage. */
    public void testStagesAllowExtraction() {
        for (String extraction : new String[] { "DISSECT key \"%{extracted}\"", "GROK key \"%{WORD:extracted}\"" }) {
            for (int primary : new int[] { 1, 2 }) {
                String query = BASE.replace("PRIMARY 2", "PRIMARY " + primary)
                    .replace("(FROM a)", "(FROM a | " + extraction + " | KEEP key, extracted)");
                assertNotNull(TEST_PARSER.createStatement(query));
            }
        }
    }

    /** Expression-held subqueries are separate scopes, including when their MEMBER call is hidden another IN deep. */
    public void testMemberCannotEscapeThroughMembershipSubquery() {
        for (String predicate : new String[] {
            "key IN (FROM c | EVAL key = MEMBER(1, key) | KEEP key)",
            "(key, key) IN (FROM c | EVAL other = MEMBER(1, key) | KEEP key, other)",
            "key IN (FROM c | WHERE key IN (FROM d | EVAL key = MEMBER(1, key) | KEEP key) | KEEP key)" }) {
            rejected(BASE + " | WHERE " + predicate, "MEMBER is not available inside an IN subquery");
        }
    }

    /** Both stages keep ordinary expansion support for either primary. */
    public void testStagesAllowExpansion() {
        for (int primary : new int[] { 1, 2 }) {
            for (String expansion : new String[] { "MV_EXPAND key", "MV_EXPAND other", "MV_EXPAND other | MV_EXPAND key" }) {
                String query = BASE.replace("PRIMARY 2", "PRIMARY " + primary)
                    .replace("(FROM a)", "(FROM a\n  | " + expansion + ")")
                    .replace("(FROM b)", "(FROM b\n  | " + expansion + ")");
                assertNotNull(TEST_PARSER.createStatement(query));
            }
        }
    }

    /** One SEQUENCE owns the request source; diagnostic grammar must never authorize a nested execution. */
    public void testSequenceMustBeTopLevelSource() {
        for (String query : List.of(
            "FROM (" + BASE + ")",
            "FROM (" + BASE + "), (FROM c)",
            "FROM (FROM c), (" + NAMED_BASE + ")",
            "FROM (" + BASE + "), (" + NAMED_BASE + ")",
            "FROM a | WHERE key IN (FROM (" + BASE + ") | KEEP key)",
            "FROM a | WHERE (key, user.name) NOT IN (FROM (" + BASE + ") | KEEP key, user.name)",
            "FROM a | WHERE key IN (FROM b | WHERE key IN (FROM (" + BASE + ") | KEEP key) | KEEP key)",
            BASE.replace("(FROM a)", "(FROM (" + BASE + "))"),
            "LET s = (" + BASE + "); FROM s",
            "EXPLAIN (FROM (" + BASE + "))"
        )) {
            rejected(query, "SEQUENCE must start the query; it is not supported inside a union or subquery");
        }
        // The IN lexer requires an ordinary source command before this diagnostic-only nested declaration.
        rejected("FROM a | WHERE key IN (" + BASE + " | KEEP key)", "mismatched input");
        assertNotNull(TEST_PARSER.createStatement("EXPLAIN (" + BASE + ")"));
    }

    /** Ordinary membership filters remain available both before matching and after PRIMARY presentation. */
    public void testOrdinaryMembershipSubqueriesRemainAvailable() {
        for (String predicate : new String[] { "key IN (FROM c | KEEP key)", "(key, user.name) NOT IN (FROM c | KEEP key, user.name)" }) {
            assertNotNull(TEST_PARSER.createStatement(BASE + " | WHERE " + predicate));
            String stages = BASE.replace("(FROM a)", "(FROM a | WHERE " + predicate + ")");
            assertNotNull(TEST_PARSER.createStatement(stages));
        }
    }

    /** MEMBER cannot hide in an otherwise ordinary binding and reach ordinary function resolution later. */
    public void testLetBoundMemberIsRejectedBeforeExpansion() {
        rejectedLetBody("FROM a | EVAL supporting = MEMBER(1, user.name)");
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
    }

    /** The retired ending is recognized solely to guide authors to the smaller output surface. */
    public void testReturnMatchesIsRemoved() {
        rejected(BASE + " | RETURN MATCHES", "use the ordinary [sequence_provenance] column or MEMBER(stage, field)");
    }

    /** Lookup joins follow the output's left spine; multi-branch FORK remains outside this prototype. */
    public void testLookupJoinAfterSequenceAndForkBoundary() {
        assertNotNull(TEST_PARSER.createStatement(BASE + " | LOOKUP JOIN supporting ON key | KEEP key"));
        assertNotNull(
            TEST_PARSER.createStatement(
                BASE + " | LOOKUP JOIN supporting ON key | LOOKUP JOIN other ON key | EVAL earlier=MEMBER(1,key) | KEEP key,earlier"
            )
        );
        rejected(BASE + " | FORK (KEEP key) (KEEP key)", "Command [Fork] is outside the sequence output prototype");
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

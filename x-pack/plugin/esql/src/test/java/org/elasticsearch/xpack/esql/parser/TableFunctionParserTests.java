/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.junit.Before;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Table functions in a {@code FROM} source list.
 * <p>
 * No table function yields a relation yet, so a call that is well formed is refused with "is not supported
 * yet". That refusal is raised last — after the name resolves and after the function has checked the shape
 * of the call — so a test reaching it has shown the query lexed, parsed, resolved and passed validation,
 * which is what these tests are about. What a successful payload fold produces is not observable from here
 * and is covered where the folded payload reaches a plan node.
 */
public class TableFunctionParserTests extends AbstractStatementParserTests {

    private static final String NOT_SUPPORTED = "[DATASET] is not supported yet";

    @Before
    public void checkCapability() {
        assumeTrue("requires table functions in FROM", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    public void testCallWithPayload() {
        expectError("FROM DATASET(my_source, {\"resource\": \"s3://bucket/data/*.parquet\"})", NOT_SUPPORTED);
    }

    public void testNameIsCaseInsensitive() {
        expectError("from dataset(my_source, {\"resource\": \"x\"})", "[dataset] is not supported yet");
        expectError("FROM DaTaSeT(my_source, {\"resource\": \"x\"})", "[DaTaSeT] is not supported yet");
    }

    /**
     * A value parameter cannot name anything, and an unbound identifier parameter would be read as a data
     * source literally called "null" with no error recorded, so the grammar admits neither.
     */
    public void testParameterIsNotADataSourceName() {
        expectError("FROM DATASET(?p, {\"resource\": \"x\"})", "mismatched input '?p'");
        expectError("FROM DATASET(??p, {\"resource\": \"x\"})", "mismatched input '??p'");
    }

    /**
     * An unbound parameter in a payload must not be read as a user-written null: the two are the same
     * {@code Literal} but for the MISSING_PARAMETER sentinel, and only the parser's own recorded "Unknown
     * query parameter" error names the real cause. That error is drained at the end of plan building, so
     * while every well-formed call is still refused it is the refusal that surfaces; what this pins is that
     * the misleading cause does not.
     */
    public void testUnboundParameterInPayloadIsNotReadAsNull() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM DATASET(my_source, {\"resource\": ?p})"));
        assertThat(e.getMessage(), not(containsString("to null")));
    }

    /**
     * The payload stands for the JSON a dataset registration carries, so a constant ES|QL admits but JSON
     * cannot express is refused rather than converted. An unsigned long is the sharpest case: its stored
     * long is a biased representation, so passing it through would hand on a different number.
     */
    public void testPayloadValueTypesOutsideJson() {
        expectError("FROM DATASET(my_source, {\"x\": 1 day})", "[DATASET] definition cannot set [x] to a value of type [date_period]");
        expectError("FROM DATASET(my_source, {\"x\": 3 hours})", "[DATASET] definition cannot set [x] to a value of type [time_duration]");
        expectError(
            "FROM DATASET(my_source, {\"x\": 18446744073709551615})",
            "[DATASET] definition cannot set [x] to a value of type [unsigned_long]"
        );
        expectError(
            "FROM DATASET(my_source, {\"spec\": {\"x\": 1 day}})",
            "[DATASET] definition cannot set [spec.x] to a value of type [date_period]"
        );
    }

    public void testPayloadValueTypesInsideJson() {
        expectError("FROM DATASET(my_source, {\"s\": \"x\", \"i\": 1, \"d\": 2.5, \"b\": true})", NOT_SUPPORTED);
        expectError("FROM DATASET(my_source, {\"l\": 9223372036854775807})", NOT_SUPPORTED);
    }

    /** A payload diagnostic points at the key, since a value may carry no position of its own. */
    public void testPayloadDiagnosticCarriesAPosition() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM DATASET(my_source, {\"resource\": null})"));
        assertThat(e.getMessage(), startsWith("line 1:26:"));
    }

    /**
     * The lookahead fires only when '(' is the very next character, so a space makes the name an index
     * pattern. Pinned rather than fixed: in a source list a space before '(' is far more often a comma
     * missing before a subquery than it is a call, and widening the lookahead over whitespace would take
     * `FROM foo (FROM bar)` for a call to a function named foo.
     */
    public void testSpaceBeforeTheParenthesisIsNotACall() {
        expectError("FROM DATASET (my_source, {\"resource\": \"x\"})", "mismatched input '('");
    }

    public void testQuotedAndBackquotedDataSourceName() {
        expectError("FROM DATASET(\"my source\", {\"resource\": \"x\"})", NOT_SUPPORTED);
        expectError("FROM DATASET(`my source`, {\"resource\": \"x\"})", NOT_SUPPORTED);
    }

    public void testEmptyPayloadIsStillAPayload() {
        expectError("FROM DATASET(my_source, {})", NOT_SUPPORTED);
    }

    public void testNestedPayload() {
        expectError("FROM DATASET(my_source, {\"resource\": \"x\", \"partition_spec\": {\"kind\": \"hive\"}})", NOT_SUPPORTED);
    }

    public void testListValueInPayload() {
        expectError("FROM DATASET(my_source, {\"resource\": \"x\", \"file_exclusions\": [\"a\", \"b\"]})", NOT_SUPPORTED);
    }

    /**
     * The lexer leaves FROM_MODE to read the call and has to come back to it, or the rest of the source list
     * would lex as an expression. Each of these puts something after the closing parenthesis that only
     * FROM_MODE can read.
     */
    public void testHeterogeneousSourceList() {
        expectError("FROM foo, bar*, DATASET(my_source, {\"resource\": \"x\"})", NOT_SUPPORTED);
        expectError("FROM DATASET(my_source, {\"resource\": \"x\"}), foo, bar*", NOT_SUPPORTED);
        expectError("FROM foo, DATASET(my_source, {\"resource\": \"x\"}), bar*", NOT_SUPPORTED);
        expectError("FROM remote:foo, DATASET(my_source, {\"resource\": \"x\"})", NOT_SUPPORTED);
    }

    public void testFollowedByMetadata() {
        expectError("FROM DATASET(my_source, {\"resource\": \"x\"}) METADATA _index", NOT_SUPPORTED);
    }

    public void testFollowedByPipe() {
        expectError("FROM DATASET(my_source, {\"resource\": \"x\"}) | LIMIT 1", NOT_SUPPORTED);
    }

    public void testTwoCallsInOneSourceList() {
        expectError("FROM DATASET(a, {\"resource\": \"x\"}), DATASET(b, {\"resource\": \"y\"})", NOT_SUPPORTED);
    }

    public void testUnknownFunction() {
        expectError("FROM NOSUCH(my_source, {\"resource\": \"x\"})", "unknown table function [NOSUCH], expected one of [dataset]");
    }

    public void testDataSourceNameRequired() {
        expectError("FROM DATASET({\"resource\": \"x\"})", "extraneous input '{' expecting {QUOTED_STRING");
    }

    public void testPayloadRequired() {
        expectError("FROM DATASET(my_source)", "[DATASET] requires a dataset definition");
    }

    public void testOneDataSourceOnly() {
        expectError("FROM DATASET(a, b, {\"resource\": \"x\"})", "[DATASET] takes one data source name, found [2]");
        expectError("FROM DATASET(a, b)", "[DATASET] takes one data source name, found [2]");
    }

    public void testPayloadKeysMustBeQuoted() {
        expectError("FROM DATASET(my_source, {resource: \"x\"})", "mismatched input 'resource'");
    }

    public void testPayloadValueCannotBeNull() {
        expectError("FROM DATASET(my_source, {\"resource\": null})", "[DATASET] definition sets [resource] to null");
    }

    public void testNestedPayloadValueCannotBeNull() {
        expectError(
            "FROM DATASET(my_source, {\"partition_spec\": {\"kind\": null}})",
            "[DATASET] definition sets [partition_spec.kind] to null"
        );
    }

    /** A repeated key whose first value is a scalar is already refused before the payload is folded. */
    public void testPayloadKeySetTwiceWithScalarValues() {
        expectError(
            "FROM DATASET(my_source, {\"resource\": \"x\", \"resource\": \"y\"})",
            "Duplicated named parameters with the same name [resource] is not supported"
        );
        expectError(
            "FROM DATASET(my_source, {\"spec\": {\"kind\": \"hive\", \"kind\": \"none\"}})",
            "Duplicated named parameters with the same name [kind] is not supported"
        );
    }

    /**
     * That upstream check does not record a key whose value is a nested payload, so a key repeated with two
     * nested values reaches the fold with both entries. Keeping the last silently is what this refuses.
     */
    public void testPayloadKeySetTwiceWithNestedValues() {
        expectError(
            "FROM DATASET(my_source, {\"spec\": {\"kind\": \"hive\"}, \"spec\": {\"kind\": \"none\"}})",
            "[DATASET] definition sets [spec] more than once"
        );
    }

    /**
     * A source pattern character cannot begin a call, so the lookahead that spots one must not fire on a
     * pattern that happens to be followed by a parenthesis. Such a query was an error before table functions
     * existed and stays one — but as a malformed source list, not as a call to a function nobody registered.
     */
    public void testPatternFollowedByParenthesisIsNotACall() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM foo*(a)"));
        assertThat(e.getMessage(), not(containsString("unknown table function")));
    }

    /** A parenthesis opening a subquery still opens one: the new lookahead only fires when a name precedes it. */
    public void testSubqueryStillParses() {
        assumeTrue("requires subquery in FROM", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        var plan = query("FROM foo, (FROM bar)");
        var unionAll = as(plan, UnionAll.class);
        assertEquals(2, unionAll.children().size());
        as(unionAll.children().get(0), UnresolvedRelation.class);
    }
}

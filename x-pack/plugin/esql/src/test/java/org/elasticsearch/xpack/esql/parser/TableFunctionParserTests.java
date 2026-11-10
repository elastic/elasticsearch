/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionArgument;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Table functions in a {@code FROM} source list, as far as the parser takes them: any name followed by {@code (}
 * becomes an {@link UnresolvedTableFunctionRelation} carrying the call verbatim, which analysis later replaces
 * with whatever relation the named function returns.
 * <p>
 * So these tests read the folded arguments off the parsed plan rather than inferring them from a refusal. The
 * function name here is arbitrary and denotes nothing: the parser never consults the registry, which is why these
 * tests pass with no function registered at all. What a particular function then makes of a call is that
 * function's own test.
 */
public class TableFunctionParserTests extends AbstractStatementParserTests {

    @Before
    public void checkCapability() {
        assumeTrue("requires table functions in FROM", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    /** Every table-function call in the plan, in plan order. */
    private List<TableFunctionInvocation> calls(String query) {
        List<TableFunctionInvocation> found = new ArrayList<>();
        query(query).forEachDown(node -> {
            if (node instanceof UnresolvedTableFunctionRelation leaf) {
                found.add(leaf.call());
            }
        });
        return found;
    }

    private UnresolvedTableFunctionRelation onlyLeaf(String query) {
        List<UnresolvedTableFunctionRelation> found = new ArrayList<>();
        query(query).forEachDown(node -> {
            if (node instanceof UnresolvedTableFunctionRelation leaf) {
                found.add(leaf);
            }
        });
        assertThat(found, hasSize(1));
        return found.get(0);
    }

    private TableFunctionInvocation onlyCall(String query) {
        return onlyLeaf(query).call();
    }

    private static List<String> argumentValues(TableFunctionInvocation call) {
        return call.arguments()
            .stream()
            .filter(a -> a instanceof TableFunctionArgument.Name)
            .map(a -> ((TableFunctionArgument.Name) a).value())
            .toList();
    }

    public void testCallWithPayload() {
        TableFunctionInvocation call = onlyCall("FROM EXAMPLE(my_source, \"res\", {\"resource\": \"s3://bucket/data/*.parquet\"})");
        assertThat(call.name(), equalTo("EXAMPLE"));
        assertThat(argumentValues(call), equalTo(List.of("my_source", "res")));
        assertThat(call.name(1), equalTo("res"));
    }

    /**
     * A lone call is the plan's root, not a one-child {@code UnionAll}. A source list of one index does not acquire
     * a union either, and every test here finds the call with {@code forEachDown}, so nothing else would notice the
     * difference — while a one-child union would reach the optimizer and every rule that matches on one.
     */
    public void testALoneCallStandsAloneRatherThanUnderAUnion() {
        as(query("FROM EXAMPLE(my_source, \"res\")"), UnresolvedTableFunctionRelation.class);
    }

    /** The name is kept as typed, for diagnostics; the registry is what matches without regard to case. */
    public void testNameIsCaseInsensitive() {
        assertThat(onlyCall("from example(my_source, \"res\", {\"x\": 1})").name(), equalTo("example"));
        assertThat(onlyCall("FROM DaTaSeT(my_source, \"res\", {\"x\": 1})").name(), equalTo("DaTaSeT"));
    }

    /**
     * A value parameter cannot name anything, and an unbound identifier parameter would be read as a data
     * source literally called "null" with no error recorded, so the grammar admits neither.
     */
    public void testParameterIsNotADataSourceName() {
        expectError("FROM EXAMPLE(?p, {\"resource\": \"x\"})", "mismatched input '?p'");
        expectError("FROM EXAMPLE(??p, {\"resource\": \"x\"})", "mismatched input '??p'");
    }

    /**
     * An unbound parameter in a payload must not be read as a user-written null: the two are the same
     * {@code Literal} but for the MISSING_PARAMETER sentinel, and only the parser's own recorded "Unknown
     * query parameter" error names the real cause. That error is drained at the end of plan building, so
     * while every well-formed call is still refused it is the refusal that surfaces; what this pins is that
     * the misleading cause does not.
     */
    public void testUnboundParameterInPayloadIsNotReadAsNull() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM EXAMPLE(my_source, \"res\", {\"resource\": ?p})"));
        assertThat(e.getMessage(), not(containsString("to null")));
    }

    /**
     * The payload stands for the JSON an example registration carries, so a constant ES|QL admits but JSON
     * cannot express is refused rather than converted. An unsigned long is the sharpest case: its stored
     * long is a biased representation, so passing it through would hand on a different number.
     */
    public void testPayloadValueTypesOutsideJson() {
        expectError("FROM EXAMPLE(my_source, \"res\", {\"x\": 1 day})", "[EXAMPLE] cannot set [x] to a value of type [date_period]");
        expectError("FROM EXAMPLE(my_source, \"res\", {\"x\": 3 hours})", "[EXAMPLE] cannot set [x] to a value of type [time_duration]");
        expectError(
            "FROM EXAMPLE(my_source, \"res\", {\"x\": 18446744073709551615})",
            "[EXAMPLE] cannot set [x] to a value of type [unsigned_long]"
        );
        expectError(
            "FROM EXAMPLE(my_source, \"res\", {\"spec\": {\"x\": 1 day}})",
            "[EXAMPLE] cannot set [spec.x] to a value of type [date_period]"
        );
    }

    public void testPayloadValueTypesInsideJson() {
        assertThat(
            onlyCall("FROM EXAMPLE(my_source, \"res\", {\"s\": \"x\", \"i\": 1, \"d\": 2.5, \"b\": true})").namedValues(2),
            equalTo(Map.of("s", "x", "i", 1, "d", 2.5, "b", true))
        );
        assertThat(
            onlyCall("FROM EXAMPLE(my_source, \"res\", {\"l\": 9223372036854775807})").namedValues(2),
            equalTo(Map.of("l", Long.MAX_VALUE))
        );
    }

    /** A payload diagnostic points at the key, since a value may carry no position of its own. */
    public void testPayloadDiagnosticCarriesAPosition() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM EXAMPLE(my_source, \"res\", {\"resource\": null})"));
        assertThat(e.getMessage(), startsWith("line 1:33:"));
    }

    /**
     * The lookahead fires only when '(' is the very next character, so a space makes the name an index
     * pattern. Pinned rather than fixed: in a source list a space before '(' is far more often a comma
     * missing before a subquery than it is a call, and widening the lookahead over whitespace would take
     * `FROM foo (FROM bar)` for a call to a function named foo.
     */
    public void testSpaceBeforeTheParenthesisIsNotACall() {
        expectError("FROM EXAMPLE (my_source, {\"resource\": \"x\"})", "mismatched input '('");
    }

    public void testQuotedAndBackquotedDataSourceName() {
        assertThat(
            argumentValues(onlyCall("FROM EXAMPLE(\"my source\", \"res\", {\"resource\": \"x\"})")),
            equalTo(List.of("my source", "res"))
        );
        assertThat(
            argumentValues(onlyCall("FROM EXAMPLE(`my source`, \"res\", {\"resource\": \"x\"})")),
            equalTo(List.of("my source", "res"))
        );
    }

    /** An empty payload is a payload: a function may require one and still accept it empty. */
    public void testEmptyPayloadIsStillAPayload() {
        assertThat(onlyCall("FROM EXAMPLE(my_source, \"res\", {})").namedValues(2), equalTo(Map.of()));
    }

    public void testNestedPayload() {
        assertThat(
            onlyCall("FROM EXAMPLE(my_source, \"res\", {\"resource\": \"x\", \"partition_spec\": {\"kind\": \"hive\"}})").namedValues(2),
            equalTo(Map.of("resource", "x", "partition_spec", Map.of("kind", "hive")))
        );
    }

    public void testListValueInPayload() {
        assertThat(
            onlyCall("FROM EXAMPLE(my_source, \"res\", {\"resource\": \"x\", \"file_exclusions\": [\"a\", \"b\"]})").namedValues(2),
            equalTo(Map.of("resource", "x", "file_exclusions", List.of("a", "b")))
        );
    }

    /**
     * The lexer leaves FROM_MODE to read the call and has to come back to it, or the rest of the source list
     * would lex as an expression. Each of these puts something after the closing parenthesis that only
     * FROM_MODE can read.
     */
    public void testHeterogeneousSourceList() {
        assertThat(calls("FROM EXAMPLE(my_source, \"res\", {\"resource\": \"x\"}), foo, bar*"), hasSize(1));
        assertThat(calls("FROM foo, EXAMPLE(my_source, \"res\", {\"resource\": \"x\"}), bar*"), hasSize(1));
        assertThat(calls("FROM remote:foo, EXAMPLE(my_source, \"res\", {\"resource\": \"x\"})"), hasSize(1));
    }

    /** The patterns and the call both survive, side by side under one union — which is what heterogeneous means. */
    public void testHeterogeneousSourceListKeepsBothSides() {
        UnionAll union = as(query("FROM foo, bar*, EXAMPLE(my_source, \"res\", {\"resource\": \"x\"})"), UnionAll.class);
        assertThat(union.children(), hasSize(2));
        assertThat(as(union.children().get(0), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("foo,bar*"));
        assertThat(
            argumentValues(as(union.children().get(1), UnresolvedTableFunctionRelation.class).call()),
            equalTo(List.of("my_source", "res"))
        );
    }

    /** METADATA names reach the leaf, so a table function carries them exactly as a pattern does. */
    public void testFollowedByMetadata() {
        UnresolvedTableFunctionRelation leaf = onlyLeaf("FROM EXAMPLE(my_source, \"res\", {\"resource\": \"x\"}) METADATA _index");
        assertThat(leaf.metadataFields().stream().map(NamedExpression::name).toList(), equalTo(List.of("_index")));
    }

    public void testFollowedByPipe() {
        assertThat(
            argumentValues(onlyCall("FROM EXAMPLE(my_source, \"res\", {\"resource\": \"x\"}) | LIMIT 1")),
            equalTo(List.of("my_source", "res"))
        );
    }

    public void testTwoCallsInOneSourceList() {
        List<TableFunctionInvocation> found = calls(
            "FROM EXAMPLE(a, \"res\", {\"resource\": \"x\"}), EXAMPLE(b, \"res\", {\"resource\": \"y\"})"
        );
        assertThat(found, hasSize(2));
        assertThat(found.stream().map(call -> call.name(0)).toList(), equalTo(List.of("a", "b")));
    }

    /**
     * A {@code {...}} stands wherever a name stands, including alone and first. Which positions a particular
     * function accepts is its signature's business, not the grammar's, so nothing here refuses this.
     */
    public void testAPayloadStandsInAnyPosition() {
        assertThat(onlyCall("FROM EXAMPLE({\"resource\": \"x\"})").arguments(), hasSize(1));
        assertThat(onlyCall("FROM EXAMPLE({\"a\": 1}, name)").arguments(), hasSize(2));
        assertThat(onlyCall("FROM EXAMPLE({\"a\": 1}, {\"b\": 2})").arguments(), hasSize(2));

        TableFunctionInvocation call = onlyCall("FROM EXAMPLE({\"a\": 1}, name)");
        assertThat(call.arguments().get(0), instanceOf(TableFunctionArgument.NamedValues.class));
        assertThat(call.arguments().get(1), instanceOf(TableFunctionArgument.Name.class));
    }

    /** The options are the last argument, so a call may stop before them. */
    public void testOptionsMayBeOmitted() {
        TableFunctionInvocation call = onlyCall("FROM EXAMPLE(my_source, \"s3://bucket/a.parquet\")");
        assertThat(call.name(0), equalTo("my_source"));
        assertThat(call.name(1), equalTo("s3://bucket/a.parquet"));
        assertThat(call.namedValues(2), nullValue());
    }

    public void testPayloadKeysMustBeQuoted() {
        expectError("FROM EXAMPLE(my_source, \"res\", {resource: \"x\"})", "mismatched input 'resource'");
    }

    public void testPayloadValueCannotBeNull() {
        expectError("FROM EXAMPLE(my_source, \"res\", {\"resource\": null})", "[EXAMPLE] sets [resource] to null");
    }

    public void testNestedPayloadValueCannotBeNull() {
        expectError(
            "FROM EXAMPLE(my_source, \"res\", {\"partition_spec\": {\"kind\": null}})",
            "[EXAMPLE] sets [partition_spec.kind] to null"
        );
    }

    /** A repeated key whose first value is a scalar is already refused before the payload is folded. */
    public void testPayloadKeySetTwiceWithScalarValues() {
        expectError(
            "FROM EXAMPLE(my_source, \"res\", {\"resource\": \"x\", \"resource\": \"y\"})",
            "Duplicated named parameters with the same name [resource] is not supported"
        );
        expectError(
            "FROM EXAMPLE(my_source, \"res\", {\"spec\": {\"kind\": \"hive\", \"kind\": \"none\"}})",
            "Duplicated named parameters with the same name [kind] is not supported"
        );
    }

    /**
     * That upstream check does not record a key whose value is a nested payload, so a key repeated with two
     * nested values reaches the fold with both entries. Keeping the last silently is what this refuses.
     */
    public void testPayloadKeySetTwiceWithNestedValues() {
        expectError(
            "FROM EXAMPLE(my_source, \"res\", {\"spec\": {\"kind\": \"hive\"}, \"spec\": {\"kind\": \"none\"}})",
            "[EXAMPLE] sets [spec] more than once"
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

    /**
     * TS admits the same source list as FROM, so a call reaches it and is refused there with its own wording
     * rather than the subquery message that shares the branch.
     */
    public void testTimeSeriesCommandRefusesALoneTableFunction() {
        expectError("TS numbers(a)", "Table functions are not supported in TS command");
    }

    public void testTimeSeriesCommandRefusesATableFunctionBesideAPattern() {
        expectError("TS foo, numbers(a)", "Table functions are not supported in TS command");
    }
}

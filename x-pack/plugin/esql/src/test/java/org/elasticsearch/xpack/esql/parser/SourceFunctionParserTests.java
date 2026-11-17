/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedSourceFunctionRelation;
import org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionArgument;
import org.elasticsearch.xpack.esql.sourcefunction.SourceFunctionInvocation;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Source functions in a {@code FROM} source list, as far as the parser takes them: any name followed by {@code (}
 * becomes an {@link UnresolvedSourceFunctionRelation} carrying the call verbatim, which analysis later replaces
 * with whatever relation the named function returns.
 * <p>
 * So these tests read the folded arguments off the parsed plan rather than inferring them from a refusal. The
 * function name here is arbitrary and denotes nothing: the parser never consults the registry, which is why these
 * tests pass with no function registered at all. What a particular function then makes of a call is that
 * function's own test.
 */
public class SourceFunctionParserTests extends AbstractStatementParserTests {

    @Before
    public void checkCapability() {
        assumeTrue("requires source functions in FROM", EsqlCapabilities.Cap.FROM_SOURCE_FUNCTION.isEnabled());
    }

    /** Every source-function call in the plan, in plan order. */
    private List<SourceFunctionInvocation> calls(String query) {
        List<SourceFunctionInvocation> found = new ArrayList<>();
        query(query).forEachDown(node -> {
            if (node instanceof UnresolvedSourceFunctionRelation leaf) {
                found.add(leaf.call());
            }
        });
        return found;
    }

    private UnresolvedSourceFunctionRelation onlyLeaf(String query) {
        List<UnresolvedSourceFunctionRelation> found = new ArrayList<>();
        query(query).forEachDown(node -> {
            if (node instanceof UnresolvedSourceFunctionRelation leaf) {
                found.add(leaf);
            }
        });
        assertThat(found, hasSize(1));
        return found.get(0);
    }

    private SourceFunctionInvocation onlyCall(String query) {
        return onlyLeaf(query).call();
    }

    private SourceFunctionInvocation onlyCall(String query, QueryParams params) {
        List<SourceFunctionInvocation> found = new ArrayList<>();
        query(query, params).forEachDown(node -> {
            if (node instanceof UnresolvedSourceFunctionRelation leaf) {
                found.add(leaf.call());
            }
        });
        assertThat(found, hasSize(1));
        return found.get(0);
    }

    private static List<String> argumentValues(SourceFunctionInvocation call) {
        return call.arguments()
            .stream()
            .filter(a -> a instanceof SourceFunctionArgument.Value)
            .map(a -> (String) ((SourceFunctionArgument.Value) a).value())
            .toList();
    }

    private static List<DataType> types(SourceFunctionInvocation call) {
        return call.arguments().stream().map(a -> ((SourceFunctionArgument.Value) a).type()).toList();
    }

    private static List<Object> values(SourceFunctionInvocation call) {
        return call.arguments().stream().map(a -> ((SourceFunctionArgument.Value) a).value()).toList();
    }

    public void testCallWithPayload() {
        SourceFunctionInvocation call = onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"s3://bucket/data/*.parquet\"})");
        assertThat(call.name(), equalTo("EXAMPLE"));
        assertThat(argumentValues(call), equalTo(List.of("my_source", "res")));
        assertThat(call.string(1), equalTo("res"));
    }

    /**
     * A lone call is the plan's root, not a one-child {@code UnionAll}. A source list of one index does not acquire
     * a union either, and every test here finds the call with {@code forEachDown}, so nothing else would notice the
     * difference — while a one-child union would reach the optimizer and every rule that matches on one.
     */
    public void testALoneCallStandsAloneRatherThanUnderAUnion() {
        as(query("FROM EXAMPLE(\"my_source\", \"res\")"), UnresolvedSourceFunctionRelation.class);
    }

    /** The name is kept as typed, for diagnostics; the registry is what matches without regard to case. */
    public void testNameIsCaseInsensitive() {
        assertThat(onlyCall("from example(\"my_source\", \"res\", {\"x\": 1})").name(), equalTo("example"));
        assertThat(onlyCall("FROM DaTaSeT(\"my_source\", \"res\", {\"x\": 1})").name(), equalTo("DaTaSeT"));
    }

    /**
     * A value parameter is one of {@code constant}'s alternatives, so it arrives folded to whatever the request
     * bound — which is how a request passes a data source it does not want written into the query text.
     */
    public void testABoundParameterIsAnArgument() {
        SourceFunctionInvocation call = onlyCall(
            "FROM EXAMPLE(?p, \"res\")",
            new QueryParams(List.of(new QueryParam("p", "my_source", DataType.KEYWORD, ParserUtils.ParamClassification.VALUE)))
        );
        assertThat(argumentValues(call), equalTo(List.of("my_source", "res")));
    }

    /**
     * An unbound one is refused with the parser's own recorded cause. It folds to a null literal and is carried as
     * one, and what the user reads is the "Unknown query parameter" error the parse already recorded, drained as
     * soon as the plan is built — not a complaint about a null argument they never wrote.
     */
    public void testAnUnboundParameterSurfacesItsOwnError() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM EXAMPLE(?p, \"res\")"));
        assertThat(e.getMessage(), containsString("Unknown query parameter [p]"));
    }

    /**
     * The identifier form is {@code doubleParameter} rather than a constant, and it is not admitted: it folds to
     * an {@code UnresolvedAttribute} named "null" with no error recorded, which would quietly name a data source.
     */
    public void testAnIdentifierParameterIsNotAnArgument() {
        expectError("FROM EXAMPLE(??p, {\"resource\": \"x\"})", "mismatched input '??p'");
    }

    /**
     * The value-parameter form carrying an identifier or pattern classification is a different path, and one a
     * request really can write: the constant rule admits {@code ?p}, so what decides is the classification the
     * request declared, and {@code visitParam} hands back an unresolved name rather than a literal for either of
     * the two that are not {@code VALUE}. Refused against the argument, because the alternative is an
     * {@code IllegalStateException} the REST layer has no arm for and the user reads as a 500.
     */
    public void testAParameterDeclaredAsAnIdentifierIsRefused() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> query(
                "FROM EXAMPLE(?p, \"res\")",
                new QueryParams(List.of(new QueryParam("p", "my_source", DataType.KEYWORD, ParserUtils.ParamClassification.IDENTIFIER)))
            )
        );
        assertThat(e.getMessage(), containsString("Query parameter [?p] [my_source] declared as an identifier, cannot be used as a value"));
    }

    /** And the pattern classification, which folds to the other unresolved shape and is named as what it is. */
    public void testAParameterDeclaredAsAPatternIsRefused() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> query(
                "FROM EXAMPLE(?p, \"res\")",
                new QueryParams(List.of(new QueryParam("p", "my_*", DataType.KEYWORD, ParserUtils.ParamClassification.PATTERN)))
            )
        );
        assertThat(e.getMessage(), containsString("Query parameter [?p] [my_*] declared as a pattern, cannot be used as a value"));
    }

    /**
     * A call buys no extra expression depth. The name token carries its own {@code (}, so nothing emits an
     * {@code LP} for a call while its {@code )} still emits an {@code RP}, and counted only there each call would
     * leave the depth counter one lower for the rest of the query -- a query may make many, so an expression
     * after them could nest that much past the bound that exists to stop the parse overflowing the stack. Both
     * arms, because the one without a call is what says the bound itself still works.
     */
    public void testACallBuysNoExtraExpressionDepth() {
        int past = ExpressionBuilder.MAX_EXPRESSION_DEPTH + 1;
        String deep = "(".repeat(past) + "1" + ")".repeat(past);

        ParsingException withoutACall = expectThrows(ParsingException.class, () -> query("FROM idx | EVAL x = " + deep));
        assertThat(withoutACall.getMessage(), containsString("maximum expression depth"));

        ParsingException withACall = expectThrows(ParsingException.class, () -> query("FROM EXAMPLE(\"1\") | EVAL x = " + deep));
        assertThat(withACall.getMessage(), containsString("maximum expression depth"));
    }

    /**
     * An unbound parameter in a payload must not be read as a user-written null: the two are the same
     * {@code Literal} but for the MISSING_PARAMETER sentinel, and only the parser's own recorded "Unknown
     * query parameter" error names the real cause. That error is drained at the end of plan building, so
     * while every well-formed call is still refused it is the refusal that surfaces; what this pins is that
     * the misleading cause does not.
     */
    public void testUnboundParameterInPayloadIsNotReadAsNull() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": ?p})"));
        assertThat(e.getMessage(), not(containsString("to null")));
    }

    /**
     * The payload stands for the JSON an example registration carries, so a constant ES|QL admits but JSON
     * cannot express is refused rather than converted. An unsigned long is the sharpest case: its stored
     * long is a biased representation, so passing it through would hand on a different number.
     */
    public void testPayloadValueTypesOutsideJson() {
        expectError("FROM EXAMPLE(\"my_source\", \"res\", {\"x\": 1 day})", "[EXAMPLE] cannot set [x] to a value of type [date_period]");
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"x\": 3 hours})",
            "[EXAMPLE] cannot set [x] to a value of type [time_duration]"
        );
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"x\": 18446744073709551615})",
            "[EXAMPLE] cannot set [x] to a value of type [unsigned_long]"
        );
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"spec\": {\"x\": 1 day}})",
            "[EXAMPLE] cannot set [spec.x] to a value of type [date_period]"
        );
    }

    public void testPayloadValueTypesInsideJson() {
        assertThat(
            onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {\"s\": \"x\", \"i\": 1, \"d\": 2.5, \"b\": true})").namedValues(2),
            equalTo(Map.of("s", "x", "i", 1, "d", 2.5, "b", true))
        );
        assertThat(
            onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {\"l\": 9223372036854775807})").namedValues(2),
            equalTo(Map.of("l", Long.MAX_VALUE))
        );
    }

    /** A payload diagnostic points at the key, since a value may carry no position of its own. */
    public void testPayloadDiagnosticCarriesAPosition() {
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> query("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": null})")
        );
        assertThat(e.getMessage(), startsWith("line 1:35:"));
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

    public void testQuotedDataSourceName() {
        assertThat(
            argumentValues(onlyCall("FROM EXAMPLE(\"my source\", \"res\", {\"resource\": \"x\"})")),
            equalTo(List.of("my source", "res"))
        );
    }

    /**
     * Neither identifier form is an argument. A data source is not an index pattern and nothing else in ES|QL
     * writes one bare, so admitting it would have been the only place a non-index name went unquoted.
     */
    public void testAnIdentifierIsNotAnArgument() {
        expectError("FROM EXAMPLE(my_source, \"res\")", "mismatched input 'my_source'");
        expectError("FROM EXAMPLE(`my source`, \"res\")", "mismatched input '`my source`'");
    }

    /**
     * Every other constant parses too, and carries its own type: what a given position accepts is the function's
     * signature's question, so nothing is refused here for being a number rather than a name.
     */
    public void testAnyConstantIsAnArgument() {
        assertThat(types(onlyCall("FROM EXAMPLE(1, 2.5, true)")), equalTo(List.of(DataType.INTEGER, DataType.DOUBLE, DataType.BOOLEAN)));
        assertThat(values(onlyCall("FROM EXAMPLE(1, 2.5, true)")), equalTo(List.of(1, 2.5, true)));
        assertThat(types(onlyCall("FROM EXAMPLE(-3, 9223372036854775807)")), equalTo(List.of(DataType.INTEGER, DataType.LONG)));
        assertThat(types(onlyCall("FROM EXAMPLE(1 day)")), equalTo(List.of(DataType.DATE_PERIOD)));
        assertThat(values(onlyCall("FROM EXAMPLE([1, 2])")), equalTo(List.of(List.of(1, 2))));
        assertThat(values(onlyCall("FROM EXAMPLE([\"a\", \"b\"])")), equalTo(List.of(List.of("a", "b"))));
        // The fourth array alternative, which the other three do not stand in for: each is its own rule in the
        // grammar, so one of them could be lost without the others noticing.
        assertThat(values(onlyCall("FROM EXAMPLE([true, false])")), equalTo(List.of(List.of(true, false))));
    }

    /** A written NULL is a constant, and reaches the signature as one rather than being refused as a syntax error. */
    public void testAWrittenNullIsAnArgument() {
        assertThat(types(onlyCall("FROM EXAMPLE(NULL)")), equalTo(List.of(DataType.NULL)));
        assertThat(onlyCall("FROM EXAMPLE(NULL)").arguments(), hasSize(1));
    }

    /**
     * An expression over constants is not an argument, for the reason LIMIT 5*3 is not one: there is nothing in
     * scope at this point in the parse, so the position is pinned in the grammar rather than folded in code.
     */
    public void testAnExpressionOverConstantsIsNotAnArgument() {
        expectError("FROM EXAMPLE(5*3)", "mismatched input '*'");
        expectError("FROM EXAMPLE(\"a\" + \"b\")", "mismatched input '+'");
        expectError("FROM EXAMPLE(TO_LOWER(\"A\"))", "mismatched input 'TO_LOWER'");
    }

    /** An empty payload is a payload: a function may require one and still accept it empty. */
    public void testEmptyPayloadIsStillAPayload() {
        assertThat(onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {})").namedValues(2), equalTo(Map.of()));
    }

    public void testNestedPayload() {
        assertThat(
            onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\", \"partition_spec\": {\"kind\": \"hive\"}})").namedValues(
                2
            ),
            equalTo(Map.of("resource", "x", "partition_spec", Map.of("kind", "hive")))
        );
    }

    public void testListValueInPayload() {
        assertThat(
            onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\", \"file_exclusions\": [\"a\", \"b\"]})").namedValues(2),
            equalTo(Map.of("resource", "x", "file_exclusions", List.of("a", "b")))
        );
    }

    /**
     * The argument list is optional in the grammar, so a call with nothing in the parentheses parses and arrives
     * with no arguments. What to make of that is the function's signature's question, not the parser's.
     */
    public void testACallWithNoArgumentsParsesAndCarriesNone() {
        assertThat(onlyCall("FROM EXAMPLE()").arguments(), hasSize(0));
        assertThat(onlyCall("FROM EXAMPLE()").name(), equalTo("EXAMPLE"));
    }

    /**
     * A multivalued parameter can hold a null element, and the fold has to carry it through rather than render it
     * as the text "null" or drop it, which would change the list a function is handed.
     */
    public void testANullElementInAParameterListSurvivesTheFold() {
        SourceFunctionInvocation call = onlyCall(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"file_exclusions\": ?p})",
            new QueryParams(List.of(new QueryParam("p", Arrays.asList("a", null), DataType.KEYWORD, ParserUtils.ParamClassification.VALUE)))
        );

        assertThat(call.namedValues(2).keySet(), contains("file_exclusions"));
        assertThat((List<?>) call.namedValues(2).get("file_exclusions"), contains("a", null));
    }

    /**
     * The lexer leaves FROM_MODE to read the call and has to come back to it, or the rest of the source list
     * would lex as an expression. Each of these puts something after the closing parenthesis that only
     * FROM_MODE can read.
     */
    public void testHeterogeneousSourceList() {
        assertThat(calls("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\"}), foo, bar*"), hasSize(1));
        assertThat(calls("FROM foo, EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\"}), bar*"), hasSize(1));
        assertThat(calls("FROM remote:foo, EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\"})"), hasSize(1));
    }

    /** The patterns and the call both survive, side by side under one union — which is what heterogeneous means. */
    public void testHeterogeneousSourceListKeepsBothSides() {
        UnionAll union = as(query("FROM foo, bar*, EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\"})"), UnionAll.class);
        assertThat(union.children(), hasSize(2));
        assertThat(as(union.children().get(0), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("foo,bar*"));
        assertThat(
            argumentValues(as(union.children().get(1), UnresolvedSourceFunctionRelation.class).call()),
            equalTo(List.of("my_source", "res"))
        );
    }

    /** METADATA names reach the leaf, so a source function carries them exactly as a pattern does. */
    public void testFollowedByMetadata() {
        UnresolvedSourceFunctionRelation leaf = onlyLeaf("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\"}) METADATA _index");
        assertThat(leaf.metadataFields().stream().map(NamedExpression::name).toList(), equalTo(List.of("_index")));
    }

    public void testFollowedByPipe() {
        assertThat(
            argumentValues(onlyCall("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\"}) | LIMIT 1")),
            equalTo(List.of("my_source", "res"))
        );
    }

    public void testTwoCallsInOneSourceList() {
        List<SourceFunctionInvocation> found = calls(
            "FROM EXAMPLE(\"a\", \"res\", {\"resource\": \"x\"}), EXAMPLE(\"b\", \"res\", {\"resource\": \"y\"})"
        );
        assertThat(found, hasSize(2));
        assertThat(found.stream().map(call -> call.string(0)).toList(), equalTo(List.of("a", "b")));
    }

    /**
     * One call written once is one leaf, even where a later command reaches it more than once. {@code FORK} hands
     * every branch the one plan above it, so walking down from the {@code Fork} arrives at the same leaf object
     * once per branch. Anything collecting calls out of a plan has to count distinct leaves rather than arrivals,
     * which is what {@code SourceFunctionResolver} does and what this pins.
     */
    public void testAForkReachesTheOneLeafOncePerBranch() {
        List<UnresolvedSourceFunctionRelation> found = new ArrayList<>();
        query("FROM EXAMPLE(\"a\") | FORK (WHERE x == 1) (WHERE x == 2)").forEachDown(node -> {
            if (node instanceof UnresolvedSourceFunctionRelation leaf) {
                found.add(leaf);
            }
        });

        assertThat(found, hasSize(2));
        assertSame(found.get(0), found.get(1));
    }

    /**
     * A {@code {...}} stands wherever a name stands, including alone and first. Which positions a particular
     * function accepts is its signature's business, not the grammar's, so nothing here refuses this.
     */
    public void testAPayloadStandsInAnyPosition() {
        assertThat(onlyCall("FROM EXAMPLE({\"resource\": \"x\"})").arguments(), hasSize(1));
        assertThat(onlyCall("FROM EXAMPLE({\"a\": 1}, \"a_name\")").arguments(), hasSize(2));
        assertThat(onlyCall("FROM EXAMPLE({\"a\": 1}, {\"b\": 2})").arguments(), hasSize(2));

        SourceFunctionInvocation call = onlyCall("FROM EXAMPLE({\"a\": 1}, \"a_name\")");
        assertThat(call.arguments().get(0), instanceOf(SourceFunctionArgument.NamedValues.class));
        assertThat(call.arguments().get(1), instanceOf(SourceFunctionArgument.Value.class));
    }

    /** An optional argument is the last one a signature declares, so a call may stop before it. */
    public void testOptionsMayBeOmitted() {
        SourceFunctionInvocation call = onlyCall("FROM EXAMPLE(\"my_source\", \"s3://bucket/a.parquet\")");
        assertThat(call.string(0), equalTo("my_source"));
        assertThat(call.string(1), equalTo("s3://bucket/a.parquet"));
        assertThat(call.namedValues(2), nullValue());
    }

    public void testPayloadKeysMustBeQuoted() {
        expectError("FROM EXAMPLE(\"my_source\", \"res\", {resource: \"x\"})", "mismatched input 'resource'");
    }

    public void testPayloadValueCannotBeNull() {
        expectError("FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": null})", "[EXAMPLE] sets [resource] to null");
    }

    public void testNestedPayloadValueCannotBeNull() {
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"partition_spec\": {\"kind\": null}})",
            "[EXAMPLE] sets [partition_spec.kind] to null"
        );
    }

    /** A repeated key whose first value is a scalar is already refused before the payload is folded. */
    public void testPayloadKeySetTwiceWithScalarValues() {
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"resource\": \"x\", \"resource\": \"y\"})",
            "Duplicated named parameters with the same name [resource] is not supported"
        );
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"spec\": {\"kind\": \"hive\", \"kind\": \"none\"}})",
            "Duplicated named parameters with the same name [kind] is not supported"
        );
    }

    /**
     * That upstream check does not record a key whose value is a nested payload, so a key repeated with two
     * nested values reaches the fold with both entries. Keeping the last silently is what this refuses.
     */
    public void testPayloadKeySetTwiceWithNestedValues() {
        expectError(
            "FROM EXAMPLE(\"my_source\", \"res\", {\"spec\": {\"kind\": \"hive\"}, \"spec\": {\"kind\": \"none\"}})",
            "[EXAMPLE] sets [spec] more than once"
        );
    }

    /**
     * A source pattern character cannot begin a call, so the lookahead that spots one must not fire on a
     * pattern that happens to be followed by a parenthesis. Such a query was an error before source functions
     * existed and stays one — but as a malformed source list, not as a call to a function nobody registered.
     */
    public void testPatternFollowedByParenthesisIsNotACall() {
        ParsingException e = expectThrows(ParsingException.class, () -> query("FROM foo*(a)"));
        assertThat(e.getMessage(), not(containsString("unknown source function")));
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
    public void testTimeSeriesCommandRefusesALoneSourceFunction() {
        expectError("TS numbers(\"a\")", "Source functions are not supported in TS command");
    }

    public void testTimeSeriesCommandRefusesASourceFunctionBesideAPattern() {
        expectError("TS foo, numbers(\"a\")", "Source functions are not supported in TS command");
    }

    /**
     * And a source list holding one of each names both. The two forms share the refusal, so naming only the one
     * that happens to be tested first sends the user to delete their subquery and meet the same error again.
     */
    public void testTimeSeriesCommandRefusesBothFormsByName() {
        expectError("TS foo, (FROM bar), numbers(\"a\")", "Subqueries and source functions are not supported in TS command");
    }
}

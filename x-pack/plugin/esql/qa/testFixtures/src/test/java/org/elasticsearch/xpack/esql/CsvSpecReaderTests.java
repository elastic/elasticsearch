/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.DatasetSource;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for the {@code dataset:} preamble directive parsed by {@link CsvSpecReader}. The directive
 * grammar is {@code dataset: <name>: "<resource>" [WITH {<json>}] [// comment]}; these tests pin the
 * non-trivial bits: escape decoding in the resource, quote/brace-aware scanning (so {@code //} inside the
 * resource or a JSON value is not a comment), trailing whitespace/comment tolerance, and the rejection of
 * stray trailing tokens.
 */
public class CsvSpecReaderTests extends ESTestCase {

    /**
     * Feeds the supplied preamble directive line(s) through a fresh parser followed by a trivial
     * {@code FROM foo} query and an empty result section, returning the assembled test case so the
     * accumulated {@link CsvTestCase#datasetSources} can be asserted.
     */
    private static CsvTestCase parse(String... preamble) {
        SpecReader.Parser parser = CsvSpecReader.specParser();
        for (String line : preamble) {
            parser.parse(line);
        }
        parser.parse("FROM foo;");
        Object result = parser.parse(";");
        assertThat(result, instanceOf(CsvTestCase.class));
        return (CsvTestCase) result;
    }

    /**
     * Feeds a query through a fresh parser, followed by any {@code warning:} or {@code warningRegex:}
     * directives (which must appear in the result section, after the query), then closes the result
     * section and returns the assembled test case.
     */
    private static CsvTestCase parseWithWarnings(String... resultSectionLines) {
        SpecReader.Parser parser = CsvSpecReader.specParser();
        parser.parse("FROM foo;");
        for (String line : resultSectionLines) {
            parser.parse(line);
        }
        Object result = parser.parse(";");
        assertThat(result, instanceOf(CsvTestCase.class));
        return (CsvTestCase) result;
    }

    private static DatasetSource parseOne(String directive) {
        CsvTestCase testCase = parse(directive);
        assertThat(testCase.datasetSources, hasSize(1));
        return testCase.datasetSources.get(0);
    }

    public void testResourceOnly() {
        DatasetSource source = parseOne("dataset: employees: \"{{employees}}\"");
        assertThat(source.name(), equalTo("employees"));
        assertThat(source.resource(), equalTo("{{employees}}"));
        assertThat(source.withJson(), nullValue());
    }

    public void testResourceWithJson() {
        DatasetSource source = parseOne("dataset: hl: \"{{employees}}\" WITH {\"header_row\": false}");
        assertThat(source.name(), equalTo("hl"));
        assertThat(source.resource(), equalTo("{{employees}}"));
        assertThat(source.withJson(), equalTo("{\"header_row\": false}"));
    }

    public void testEscapedQuoteInResourceIsDecoded() {
        // directive text: dataset: q: "ab\"cd"
        DatasetSource source = parseOne("dataset: q: \"ab\\\"cd\"");
        assertThat(source.resource(), equalTo("ab\"cd"));
        assertThat(source.withJson(), nullValue());
    }

    public void testEscapedBackslashInResourceIsDecoded() {
        // directive text: dataset: q: "a\\b" -> resource a\b
        DatasetSource source = parseOne("dataset: q: \"a\\\\b\"");
        assertThat(source.resource(), equalTo("a\\b"));
    }

    public void testDoubleSlashInResourceIsNotAComment() {
        DatasetSource source = parseOne("dataset: u: \"http://host/path\"");
        assertThat(source.resource(), equalTo("http://host/path"));
        assertThat(source.withJson(), nullValue());
    }

    public void testTrailingCommentAfterResource() {
        DatasetSource source = parseOne("dataset: c: \"{{x}}\" // no WITH here");
        assertThat(source.resource(), equalTo("{{x}}"));
        assertThat(source.withJson(), nullValue());
    }

    public void testTrailingCommentAfterWith() {
        DatasetSource source = parseOne("dataset: c: \"{{x}}\" WITH {\"a\": 1}   // trailing note");
        assertThat(source.resource(), equalTo("{{x}}"));
        assertThat(source.withJson(), equalTo("{\"a\": 1}"));
    }

    public void testDoubleSlashInsideJsonValueIsNotAComment() {
        DatasetSource source = parseOne("dataset: c: \"{{x}}\" WITH {\"url\": \"http://h//x\"}");
        assertThat(source.withJson(), equalTo("{\"url\": \"http://h//x\"}"));
    }

    public void testNestedBraceInJsonIsMatched() {
        DatasetSource source = parseOne("dataset: c: \"{{x}}\" WITH {\"a\": {\"b\": 1}}");
        assertThat(source.withJson(), equalTo("{\"a\": {\"b\": 1}}"));
    }

    public void testRepeatableDirectiveAccumulates() {
        CsvTestCase testCase = parse("dataset: a: \"{{x}}\"", "dataset: b: \"{{y}}\" WITH {\"k\": \"v\"}");
        assertThat(testCase.datasetSources, hasSize(2));
        assertThat(testCase.datasetSources.get(0).name(), equalTo("a"));
        assertThat(testCase.datasetSources.get(1).name(), equalTo("b"));
        assertThat(testCase.datasetSources.get(1).withJson(), equalTo("{\"k\": \"v\"}"));
    }

    public void testStrayTrailingTokenRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("dataset: c: \"{{x}}\" WITH {\"a\": 1} garbage")
        );
        assertThat(e.getMessage(), containsString("unexpected trailing token after WITH JSON object"));
    }

    public void testUnterminatedResourceRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("dataset: c: \"{{x}}")
        );
        assertThat(e.getMessage(), containsString("unterminated resource string"));
    }

    public void testUnterminatedWithBraceRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("dataset: c: \"{{x}}\" WITH {\"a\": 1")
        );
        assertThat(e.getMessage(), containsString("unterminated WITH JSON object"));
    }

    public void testNonWithNonCommentTrailerRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("dataset: c: \"{{x}}\" bogus")
        );
        assertThat(e.getMessage(), containsString("expected WITH or a // comment after the resource"));
    }

    public void testMissingQuotedResourceRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("dataset: c: bare")
        );
        assertThat(e.getMessage(), containsString("a name and a quoted resource are required"));
    }

    // param: directive tests

    /**
     * Feeds the {@code param:} directives through a fresh parser followed by {@code query} and an empty result section.
     */
    private static CsvTestCase parseWithParams(String query, String... params) {
        SpecReader.Parser parser = CsvSpecReader.specParser();
        for (String line : params) {
            parser.parse(line);
        }
        parser.parse(query + ";");
        Object result = parser.parse(";");
        assertThat(result, instanceOf(CsvTestCase.class));
        return (CsvTestCase) result;
    }

    public void testNoParams() {
        CsvTestCase testCase = parse();
        assertThat(testCase.params, empty());
        assertThat(testCase.paramsJson(), nullValue());
        assertFalse(testCase.paramsCanBeNames());
    }

    public void testNamedParams() {
        CsvTestCase testCase = parse(
            "param: s = \"a string with = and {braces}\"",
            "param: i=5",
            "param: d = -1.5",
            "param: b = true",
            "param: n = null",
            "param: a = [1, 2]",
            "PARAM: v = {\"value\": \"x\"}",
            "param: f = {\"identifier\": \"first_name\"}"
        );
        assertThat(testCase.params, hasSize(8));
        assertThat(testCase.params.get(0), equalTo(new CsvSpecReader.Param("s", "\"a string with = and {braces}\"")));
        assertThat(testCase.params.get(1), equalTo(new CsvSpecReader.Param("i", "5")));
        assertThat(
            testCase.paramsJson(),
            equalTo(
                "[{\"s\": \"a string with = and {braces}\"}, {\"i\": 5}, {\"d\": -1.5}, {\"b\": true}, {\"n\": null}, {\"a\": [1,2]}, "
                    + "{\"v\": {\"value\":\"x\"}}, {\"f\": {\"identifier\":\"first_name\"}}]"
            )
        );
    }

    public void testUnnamedParams() {
        CsvTestCase testCase = parse("param: \"F\"", "param: 10008", "param: [\"a\", \"b\"]", "param: \"x = 1\"");
        assertThat(testCase.params, hasSize(4));
        assertThat(testCase.params.get(0), equalTo(new CsvSpecReader.Param(null, "\"F\"")));
        assertThat(testCase.params.get(3), equalTo(new CsvSpecReader.Param(null, "\"x = 1\"")));
        assertThat(testCase.paramsJson(), equalTo("[\"F\", 10008, [\"a\",\"b\"], \"x = 1\"]"));
    }

    public void testParamCommentIsNotSent() {
        CsvTestCase testCase = parse("param: x = 1 // note", "param: y = [1, /* two */ 2]");
        assertThat(testCase.paramsJson(), equalTo("[{\"x\": 1}, {\"y\": [1,2]}]"));
    }

    public void testParamsDoNotLeakIntoNextTest() {
        SpecReader.Parser parser = CsvSpecReader.specParser();
        parser.parse("param: x = 1");
        parser.parse("FROM foo;");
        assertThat(((CsvTestCase) parser.parse(";")).params, hasSize(1));
        parser.parse("FROM bar;");
        assertThat(((CsvTestCase) parser.parse(";")).params, empty());
    }

    public void testParamInResultsIsNotADirective() {
        CsvTestCase testCase = parseWithWarnings("param:keyword | other:integer", "param: 42 | 1");
        assertThat(testCase.params, empty());
        assertThat(testCase.expectedResults, equalTo("param:keyword | other:integer\r\nparam: 42 | 1\r\n"));
    }

    public void testUnquotedStringParamRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("param: first = Georgi")
        );
        assertThat(e.getMessage(), containsString("Invalid param [param: first = Georgi]"));
    }

    public void testParamWithTrailingContentRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CsvSpecReader.specParser().parse("param: x = 1 2"));
        assertThat(e.getMessage(), containsString("with a single JSON value"));
    }

    public void testEmptyParamRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CsvSpecReader.specParser().parse("param: x ="));
        assertThat(e.getMessage(), containsString("with a single JSON value"));
    }

    public void testNonFiniteNumberParamRejected() {
        for (String line : List.of("param: x = 1e309", "param: x = [1, -1e400]", "param: x = {\"value\": 1e309}")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CsvSpecReader.specParser().parse(line));
            assertThat(e.getMessage(), containsString("numbers must be finite"));
        }
    }

    public void testUnnamedObjectParamRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CsvSpecReader.specParser().parse("param: {\"x\": 1}")
        );
        assertThat(e.getMessage(), containsString("an unnamed parameter can't be an object"));
    }

    public void testMixedNamedAndUnnamedParamsRejected() {
        SpecReader.Parser parser = CsvSpecReader.specParser();
        parser.parse("param: x = 1");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.parse("param: 2"));
        assertThat(e.getMessage(), containsString("named and unnamed parameters can't be mixed"));
    }

    public void testParamsCanBeNames() {
        assertFalse(parseWithParams("FROM foo | WHERE a == ?x", "param: x = \"a\"").paramsCanBeNames());
        assertFalse(parseWithParams("FROM foo | WHERE a == ?x", "param: x = {\"value\": \"a\"}").paramsCanBeNames());
        assertFalse(parseWithParams("FROM foo | WHERE a == ??x").paramsCanBeNames());
        assertTrue(parseWithParams("FROM foo | WHERE ??x == ?y", "param: x = \"a\"", "param: y = 1").paramsCanBeNames());
        assertTrue(parseWithParams("FROM foo | KEEP ??1", "param: \"a\"").paramsCanBeNames());
        assertTrue(parseWithParams("FROM foo | KEEP ??", "param: \"a\"").paramsCanBeNames());
        assertTrue(parseWithParams("FROM foo | KEEP ?x", "param: x = {\"identifier\": \"a\"}").paramsCanBeNames());
        assertTrue(parseWithParams("FROM foo | KEEP ?x", "param: x = {\"Pattern\": \"a*\"}").paramsCanBeNames());
    }

    public void testPromqlIndexIsParam() {
        assertTrue(parseWithParams("PROMQL index=?idx step=1m (avg(x))", "param: idx = \"k8s\"").promqlIndexIsParam());
        assertTrue(parseWithParams("PROMQL step=1m index = ?idx (avg(x))", "param: idx = \"k8s\"").promqlIndexIsParam());
        assertFalse(parseWithParams("PROMQL index=k8s step=?step (avg(x))", "param: step = \"1m\"").promqlIndexIsParam());
        assertFalse(parseWithParams("PROMQL index=?idx (avg(x))").promqlIndexIsParam());
    }

    // makeWarningsOptional() tests

    public void testMakeWarningsOptionalConvertsExactStringsToPatterns() {
        CsvTestCase testCase = parseWithWarnings("warning: Column [x] is missing");
        assertThat(testCase.assertWarnings(false), instanceOf(AssertWarnings.ExactStrings.class));

        testCase.makeWarningsOptional();

        assertThat(testCase.assertWarnings(false), instanceOf(AssertWarnings.AllowedRegexes.class));
    }

    public void testMakeWarningsOptionalAllowsExpectedWarningToBeAbsent() {
        CsvTestCase testCase = parseWithWarnings("warning: Column [x] is missing");
        testCase.makeWarningsOptional();
        // no warnings returned → passes (optional)
        testCase.assertWarnings(false).assertWarnings(List.of(), null);
    }

    public void testMakeWarningsOptionalStillAcceptsExpectedWarning() {
        CsvTestCase testCase = parseWithWarnings("warning: Column [x] is missing");
        testCase.makeWarningsOptional();
        // exact expected warning returned → still passes
        testCase.assertWarnings(false).assertWarnings(List.of("Column [x] is missing"), null);
    }

    public void testMakeWarningsOptionalStillRejectsUnexpectedWarning() {
        CsvTestCase testCase = parseWithWarnings("warning: Column [x] is missing");
        testCase.makeWarningsOptional();
        AssertionError e = expectThrows(
            AssertionError.class,
            () -> testCase.assertWarnings(false).assertWarnings(List.of("Something completely unexpected"), null)
        );
        assertThat(e.getMessage(), containsString("Unexpected warning"));
    }

    public void testMakeWarningsOptionalIsNoOpWithNoExpectedWarnings() {
        CsvTestCase testCase = parseWithWarnings();
        assertThat(testCase.assertWarnings(false), instanceOf(AssertWarnings.NoWarnings.class));
        testCase.makeWarningsOptional();
        assertThat(testCase.assertWarnings(false), instanceOf(AssertWarnings.NoWarnings.class));
    }

    public void testMakeWarningsOptionalIsNoOpWithRegexWarnings() {
        CsvTestCase testCase = parseWithWarnings("warningRegex: Column \\[.+\\] is missing");
        assertThat(testCase.assertWarnings(false), instanceOf(AssertWarnings.AllowedRegexes.class));
        testCase.makeWarningsOptional();
        // regex warnings are already optional (AllowedRegexes doesn't require all patterns to match)
        assertThat(testCase.assertWarnings(false), instanceOf(AssertWarnings.AllowedRegexes.class));
    }
}

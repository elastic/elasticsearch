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

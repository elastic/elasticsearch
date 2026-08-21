/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedMetadataAttributeExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedEqlRelation;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for parsing the {@code EQL "<query>"} source command into an {@link UnresolvedEqlRelation}.
 *
 * <p>The {@code EQL} grammar surface is gated to snapshot builds via
 * {@link EsqlCapabilities.Cap#EQL_COMMAND}, which the grammar predicates read directly; each parsing
 * test asserts that snapshot precondition, and the release-build rejection is covered by
 * {@link #testEqlCommandUnavailableOnRealReleaseBuild}.
 */
public class EqlCommandParsingTests extends AbstractStatementParserTests {

    public void testEventQuery() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-* \"process where true\"");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.query(), instanceOf(Literal.class));
        assertThat(BytesRefs.toString(as(eql.query(), Literal.class).value()), equalTo("process where true"));
        assertThat(eql.indexPattern().indexPattern(), equalTo("logs-*"));
        assertThat(eql.options().size(), equalTo(0));
    }

    public void testSequenceQuery() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-* \"sequence [process where true] [network where true]\"");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(
            BytesRefs.toString(as(eql.query(), Literal.class).value()),
            equalTo("sequence [process where true] [network where true]")
        );
    }

    public void testMultipleIndexPatterns() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-a,logs-b \"process where true\"");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.indexPattern().indexPattern(), equalTo("logs-a,logs-b"));
    }

    public void testQuotedAndTripleQuotedIndexPattern() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // The EQL leading pattern reuses FROM's full indexPattern grammar, so quoted and triple-quoted forms parse
        // the same way — the quotes are stripped to the bare pattern string.
        assertEqlIndexPattern("logs-2026,logs-old", "\"logs-2026\",\"logs-old\"");
        assertEqlIndexPattern("logs-2026", "\"\"\"logs-2026\"\"\"");
    }

    public void testDateMathIndexPattern() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // Date-math index expressions flow through unchanged, exactly as for FROM.
        assertEqlIndexPattern("<logstash-{now/d}>", "<logstash-{now/d}>");
        assertEqlIndexPattern("<logstash-{now/M{yyyy.MM}}>", "<logstash-{now/M{yyyy.MM}}>");
    }

    public void testRemoteClusterIndexPattern() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // Cross-cluster (cluster:index) patterns parse; the analyzer/EQL engine handle remote resolution downstream.
        assertEqlIndexPattern("cluster:logs", "cluster:logs");
        assertEqlIndexPattern("cluster*:logs-*", "cluster*:logs-*");
    }

    public void testSelectorIndexPattern() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());
        assumeTrue("requires index component selectors", EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled());

        // Component selectors (::data / ::failures) parse on the EQL leading pattern the same as FROM.
        assertEqlIndexPattern("logs::data", "logs::data");
        assertEqlIndexPattern("logs::failures", "logs::failures");
    }

    public void testInvalidIndexPatternRejected() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // A fully-quoted pattern with an invalid character is a semantic error, not a silent misbind into the EQL engine.
        expectError(
            "EQL \"index|pattern\" \"process where true\"",
            "Invalid index name [index|pattern], must not contain the following characters"
        );
    }

    public void testQuotedQueryWithoutPatternFailsToParse() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // The grammar requires a query string AFTER the leading pattern, so a lone quoted string binds as the index
        // pattern and the missing query is a parse error — the quoted query is never silently consumed as the pattern.
        ParsingException e = expectThrows(ParsingException.class, () -> query("EQL \"process where true\""));
        assertThat(e.getMessage(), containsString("line 1:"));
    }

    /** Parses {@code EQL <pattern> "process where true"} and asserts the leading pattern resolves to {@code expected}. */
    private void assertEqlIndexPattern(String expected, String pattern) {
        UnresolvedEqlRelation eql = as(query("EQL " + pattern + " \"process where true\""), UnresolvedEqlRelation.class);
        assertThat(eql.indexPattern().indexPattern(), equalTo(expected));
    }

    public void testMetadataParsesIntoRelation() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-* \"process where true\" METADATA _index");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.metadataFields(), hasSize(1));
        NamedExpression index = eql.metadataFields().get(0);
        assertThat(index, instanceOf(MetadataAttribute.class));
        assertThat(index.name(), equalTo("_index"));
        assertThat(index.dataType(), equalTo(DataType.KEYWORD));
    }

    public void testMetadataMultipleFieldsKeepDeclaredOrder() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-* \"process where true\" METADATA _source, _id, _index");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.metadataFields().stream().map(NamedExpression::name).toList(), contains("_source", "_id", "_index"));
    }

    public void testMetadataDuplicateFieldRejected() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        ParsingException e = expectThrows(ParsingException.class, () -> query("EQL logs-* \"process where true\" METADATA _id, _id"));
        assertThat(e.getMessage(), containsString("metadata field [_id] already declared"));
    }

    public void testMetadataUnknownNameParsesUnresolved() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // The parser accepts any name; the analyzer decides what the EQL delegate can populate (message parity with FROM).
        var plan = query("EQL logs-* \"process where true\" METADATA _bogus");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.metadataFields(), hasSize(1));
        assertThat(eql.metadataFields().get(0), instanceOf(UnresolvedMetadataAttributeExpression.class));
    }

    public void testMetadataWithOptionsCombined() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-* \"process where true\" METADATA _id WITH { \"size\": 5 }");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.metadataFields().stream().map(NamedExpression::name).toList(), contains("_id"));
        assertThat(eql.options().get("size"), equalTo(5));
    }

    public void testMetadataAfterWithFailsToParse() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // Grammar order is METADATA before WITH; the reverse must not parse.
        expectThrows(ParsingException.class, () -> query("EQL logs-* \"process where true\" WITH { \"size\": 5 } METADATA _id"));
    }

    public void testWithOptions() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL logs-* \"process where true\" WITH { \"size\": 100 }");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.indexPattern().indexPattern(), equalTo("logs-*"));
        Map<String, Object> options = eql.options();
        assertThat(options.size(), equalTo(1));
        assertThat(options.get("size"), equalTo(100));
    }

    public void testWithOversizedUnsignedLongSizeRejected() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // 2^63 folds to an unsigned_long literal stored biased (raw long 0); it must be rejected as out-of-range at
        // parse — not silently wrapped to size 0, which would present an empty result as complete.
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> query("EQL logs-* \"process where true\" WITH { \"size\": 9223372036854775808 }")
        );
        assertThat(e.getMessage(), containsString("[size]"));
        assertThat(e.getMessage(), containsString("9223372036854775808"));
    }

    /**
     * Regression guard mirroring {@code IcebergParsingTests#testExternalCommandUnavailableOnRealReleaseBuild}:
     * only meaningfully executes on a genuine release build (e.g. {@code -Dbuild.snapshot=false}), where it
     * asserts the {@code EQL} grammar surface is rejected.
     */
    public void testEqlCommandUnavailableOnRealReleaseBuild() {
        assumeFalse("only exercises the real release-build gate", Build.current().isSnapshot());

        assertFalse(
            "EQL command capability must not be reported as enabled on a release build",
            EsqlCapabilities.Cap.EQL_COMMAND.isEnabled()
        );

        EsqlParser prodParser = new EsqlParser(new EsqlConfig(TEST_FUNCTION_REGISTRY));
        ParsingException pe = expectThrows(ParsingException.class, () -> prodParser.createStatement("EQL \"process where true\""));
        assertThat(pe.getMessage(), containsString("line 1:1:"));
    }
}

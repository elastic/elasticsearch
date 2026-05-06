/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for parsing the EXTERNAL command.
 *
 * <p>The {@code EXTERNAL} grammar surface is gated to snapshot builds; release builds reject it at
 * the lexer level. Each test asserts the snapshot precondition rather than executing under release
 * — release-build CI silently skips, which is the intended behaviour for a snapshot-only feature.
 */
public class IcebergParsingTests extends AbstractStatementParserTests {

    public void testIcebergCommandWithSimplePath() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EXTERNAL \"s3://bucket/table\"");

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation iceberg = as(plan, UnresolvedExternalRelation.class);

        assertThat(iceberg.tablePath(), instanceOf(Literal.class));
        Literal pathLiteral = as(iceberg.tablePath(), Literal.class);
        assertThat(BytesRefs.toString(pathLiteral.value()), equalTo("s3://bucket/table"));
        assertThat(iceberg.config().size(), equalTo(0));
    }

    public void testIcebergCommandWithParameters() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("""
            EXTERNAL "s3://bucket/table"
                WITH { "access_key": "AKIAIOSFODNN7EXAMPLE", "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" }
            """);

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation iceberg = as(plan, UnresolvedExternalRelation.class);

        assertThat(iceberg.tablePath(), instanceOf(Literal.class));
        Literal pathLiteral = as(iceberg.tablePath(), Literal.class);
        assertThat(BytesRefs.toString(pathLiteral.value()), equalTo("s3://bucket/table"));

        Map<String, Object> config = iceberg.config();
        assertThat(config.size(), equalTo(2));

        assertThat(config.containsKey("access_key"), equalTo(true));
        assertThat(config.get("access_key"), equalTo("AKIAIOSFODNN7EXAMPLE"));

        assertThat(config.containsKey("secret_key"), equalTo(true));
        assertThat(config.get("secret_key"), equalTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
    }

    public void testIcebergCommandWithBooleanParameter() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EXTERNAL \"s3://bucket/table\" WITH { \"use_cache\": true }");

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation iceberg = as(plan, UnresolvedExternalRelation.class);

        Map<String, Object> config = iceberg.config();
        assertThat(config.size(), equalTo(1));
        assertThat(config.containsKey("use_cache"), equalTo(true));
        assertThat(config.get("use_cache"), equalTo(true));
    }

    public void testIcebergCommandNotAvailableInProduction() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // Create a parser with production mode (dev version = false)
        EsqlConfig config = new EsqlConfig(false, TEST_FUNCTION_REGISTRY);
        EsqlParser prodParser = new EsqlParser(config);

        ParsingException pe = expectThrows(ParsingException.class, () -> prodParser.createStatement("EXTERNAL \"s3://bucket/table\""));
        assertThat(pe.getMessage(), containsString("mismatched input 'EXTERNAL'"));
    }

    public void testIcebergCommandWithPipedCommands() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EXTERNAL \"s3://bucket/table\" | WHERE age > 25 | LIMIT 10");

        // The plan should be a Limit with Filter underneath, and UnresolvedExternalRelation at the bottom
        assertNotNull(plan);
        assertThat(plan, instanceOf(org.elasticsearch.xpack.esql.plan.logical.Limit.class));
    }

    public void testIcebergCommandWithParquetFile() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EXTERNAL \"s3://bucket/data/file.parquet\"");

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation iceberg = as(plan, UnresolvedExternalRelation.class);

        assertThat(iceberg.tablePath(), instanceOf(Literal.class));
        Literal pathLiteral = as(iceberg.tablePath(), Literal.class);
        assertThat(BytesRefs.toString(pathLiteral.value()), equalTo("s3://bucket/data/file.parquet"));
    }

    public void testIcebergCommandWithParameter() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // Test with positional parameter placeholder
        var plan = query("EXTERNAL ?", new QueryParams(List.of(paramAsConstant(null, "s3://bucket/table"))));

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation iceberg = as(plan, UnresolvedExternalRelation.class);

        assertNotNull(iceberg.tablePath());
    }

    public void testWithOptionUnboundParameterThrows() {
        // The post-substitution invariant — every WITH-option value is a Literal — is enforced
        // because unbound parameters never get past the parser's own parameter-count check, which
        // fires upstream of foldOptionLiterals. The end-user observes a ParsingException either way.
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        ParsingException pe = expectThrows(
            ParsingException.class,
            () -> query("EXTERNAL \"s3://bucket/table\" WITH { \"k\": ? }", new QueryParams(List.of()))
        );
        assertThat(pe.getMessage(), containsString("Not enough actual parameters"));
    }

    public void testWithOptionNullLiteralThrows() {
        // A Literal whose value is null is also a strict-rule violation: option values must be
        // meaningful, not null. Without this check the entry would silently drop and downstream
        // would behave as if the option weren't set.
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        ParsingException pe = expectThrows(ParsingException.class, () -> query("EXTERNAL \"s3://bucket/table\" WITH { \"k\": null }"));
        assertThat(pe.getMessage(), containsString("EXTERNAL option [k] has null value"));
    }

    /**
     * A naive Windows local path embedded as {@code "file://C:\\dir\\file"} fails to parse because
     * the QUOTED_STRING grammar treats {@code \\} as an escape introducer (see
     * x-pack/plugin/esql/src/main/antlr/lexer/Expression.g4: ESCAPE_SEQUENCE only allows
     * {@code \\t \\n \\r \\" \\\\}). Tests must produce {@code file:///C:/...} URIs, e.g. via
     * {@code StoragePath.fileUri(Path)}, so EXTERNAL queries are parser-safe across OSes.
     */
    public void testExternalCommandRejectsBackslashesInUri() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        ParsingException pe = expectThrows(ParsingException.class, () -> query("EXTERNAL \"file://C:\\build\\data.parquet\""));
        assertThat(pe.getMessage(), containsString("token recognition error"));
    }

    /**
     * Forward-slash {@code file:///C:/...} URIs (the form {@code StoragePath.fileUri(Path)} produces
     * on Windows) parse cleanly, ensuring the EXTERNAL command works on Windows when callers use the
     * helper instead of {@code Path.toAbsolutePath().toString()}.
     */
    public void testExternalCommandAcceptsWindowsFileUri() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        String uri = "file:///C:/build/data.parquet";
        var plan = query("EXTERNAL \"" + uri + "\"");

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation external = as(plan, UnresolvedExternalRelation.class);
        Literal pathLiteral = as(external.tablePath(), Literal.class);
        assertThat(BytesRefs.toString(pathLiteral.value()), equalTo(uri));
    }
}

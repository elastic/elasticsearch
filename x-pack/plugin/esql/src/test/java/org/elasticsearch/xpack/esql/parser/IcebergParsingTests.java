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

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for parsing the EXTERNAL command.
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
        assertThat(iceberg.params().size(), equalTo(0));
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

        Map<String, org.elasticsearch.xpack.esql.core.expression.Expression> params = iceberg.params();
        assertThat(params.size(), equalTo(2));

        assertThat(params.containsKey("access_key"), equalTo(true));
        assertThat(params.get("access_key"), instanceOf(Literal.class));
        assertThat(BytesRefs.toString(as(params.get("access_key"), Literal.class).value()), equalTo("AKIAIOSFODNN7EXAMPLE"));

        assertThat(params.containsKey("secret_key"), equalTo(true));
        assertThat(params.get("secret_key"), instanceOf(Literal.class));
        assertThat(
            BytesRefs.toString(as(params.get("secret_key"), Literal.class).value()),
            equalTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        );
    }

    public void testIcebergCommandWithBooleanParameter() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EXTERNAL \"s3://bucket/table\" WITH { \"use_cache\": true }");

        assertThat(plan, instanceOf(UnresolvedExternalRelation.class));
        UnresolvedExternalRelation iceberg = as(plan, UnresolvedExternalRelation.class);

        Map<String, org.elasticsearch.xpack.esql.core.expression.Expression> params = iceberg.params();
        assertThat(params.size(), equalTo(1));
        assertThat(params.containsKey("use_cache"), equalTo(true));
        assertThat(params.get("use_cache"), instanceOf(Literal.class));
        assertThat(as(params.get("use_cache"), Literal.class).value(), equalTo(true));
    }

    public void testIcebergCommandNotAvailableInProduction() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        // Create a parser with production mode (dev version = false)
        EsqlConfig config = new EsqlConfig(false);
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
}

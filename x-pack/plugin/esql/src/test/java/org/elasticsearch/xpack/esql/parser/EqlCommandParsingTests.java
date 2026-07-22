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
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedEqlRelation;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

        var plan = query("EQL \"process where true\"");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(eql.query(), instanceOf(Literal.class));
        assertThat(BytesRefs.toString(as(eql.query(), Literal.class).value()), equalTo("process where true"));
        assertThat(eql.options().size(), equalTo(0));
    }

    public void testSequenceQuery() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL \"sequence [process where true] [network where true]\"");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        assertThat(
            BytesRefs.toString(as(eql.query(), Literal.class).value()),
            equalTo("sequence [process where true] [network where true]")
        );
    }

    public void testWithOptions() {
        assumeTrue("requires snapshot builds", Build.current().isSnapshot());

        var plan = query("EQL \"process where true\" WITH { \"indices\": \"logs-*\", \"size\": 100 }");

        UnresolvedEqlRelation eql = as(plan, UnresolvedEqlRelation.class);
        Map<String, Object> options = eql.options();
        assertThat(options.size(), equalTo(2));
        assertThat(options.get("indices"), equalTo("logs-*"));
        assertThat(options.get("size"), equalTo(100));
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

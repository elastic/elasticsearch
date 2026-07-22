/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SOURCE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

/**
 * Unit tests for analysis of the {@code EQL "<query>"} source command, which delegates execution to the EQL
 * engine. The analyzer parses the EQL query string ({@code ResolveEqlRelation}) to determine the result mode
 * (event / sequence / sample) and stage count, then fixes the {@link EqlRelation} output schema. These tests
 * assert that fixed schema for each mode.
 */
public class AnalyzerEqlTests extends ESTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testEventQuerySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL \"process where true\""));

        assertThat(names(output), contains("_index", "_id", "_source"));
        assertThat(types(output), contains(KEYWORD, KEYWORD, SOURCE));
    }

    public void testSequenceQuerySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL \"sequence [process where true] [network where true]\""));

        assertThat(names(output), contains("_seq", "_position", "join_keys", "_index", "_id", "_source"));
        assertThat(types(output), contains(LONG, INTEGER, KEYWORD, KEYWORD, KEYWORD, SOURCE));
    }

    public void testSampleQuerySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL \"sample by host [process where true] [network where true]\""));

        assertThat(names(output), contains("_seq", "_position", "join_keys", "_index", "_id", "_source"));
        assertThat(types(output), contains(LONG, INTEGER, KEYWORD, KEYWORD, KEYWORD, SOURCE));
    }

    public void testSequenceSchemaIsFixedRegardlessOfStageCount() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A three-stage sequence yields the exact same (unnested) schema as a two-stage one.
        List<Attribute> output = eqlLeafOutput(analyze("EQL \"sequence [process where true] [network where true] [file where true]\""));

        assertThat(names(output), contains("_seq", "_position", "join_keys", "_index", "_id", "_source"));
    }

    private static LogicalPlan analyze(String query) {
        return analyzer().buildAnalyzer().analyze(TEST_PARSER.parseQuery(query));
    }

    private static List<Attribute> eqlLeafOutput(LogicalPlan analyzed) {
        List<EqlRelation> leaves = new ArrayList<>();
        analyzed.forEachDown(EqlRelation.class, leaves::add);
        assertThat("analyzed plan must contain exactly one EqlRelation", leaves, hasSize(1));
        return leaves.get(0).output();
    }

    private static List<String> names(List<Attribute> attrs) {
        return attrs.stream().map(Attribute::name).toList();
    }

    private static List<DataType> types(List<Attribute> attrs) {
        return attrs.stream().map(Attribute::dataType).toList();
    }
}

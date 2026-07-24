/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for analysis of the {@code EQL <indexPattern> "<query>"} source command, which delegates execution to
 * the EQL engine. {@code ResolveEqlRelation} resolves the target index pattern through the SAME field-caps path
 * {@code FROM} uses — so the output is one typed column per mapped field — and parses the EQL query string to
 * determine the result mode (event / sequence / sample), prepending the sequence synthetics for non-event modes.
 * These tests assert the resolved schema for each mode, plus the unknown-index and unconvertible-type paths.
 */
public class AnalyzerEqlTests extends ESTestCase {

    private static final String INDEX = "eql_test";

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testEventQuerySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL eql_test \"process where true\""));

        assertThat(names(output), contains("@timestamp", "category", "ingested", "name", "pid"));
        assertThat(types(output), contains(DATETIME, KEYWORD, DATETIME, KEYWORD, LONG));
    }

    public void testSequenceQuerySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL eql_test \"sequence [process where true] [network where true]\""));

        assertThat(
            names(output),
            contains("_sequence", "_sequence_stage", "join_keys", "@timestamp", "category", "ingested", "name", "pid")
        );
        assertThat(types(output), contains(LONG, INTEGER, KEYWORD, DATETIME, KEYWORD, DATETIME, KEYWORD, LONG));
    }

    public void testSampleQuerySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL eql_test \"sample by category [process where true] [network where true]\""));

        assertThat(
            names(output),
            contains("_sequence", "_sequence_stage", "join_keys", "@timestamp", "category", "ingested", "name", "pid")
        );
        assertThat(types(output), contains(LONG, INTEGER, KEYWORD, DATETIME, KEYWORD, DATETIME, KEYWORD, LONG));
    }

    public void testUnknownIndexFailsVerification() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // Mirror production: preAnalysis resolves the pattern to an invalid resolution rather than leaving it absent.
        analyzer().addIndex("missing_index", IndexResolution.invalid("Unknown index [missing_index]"))
            .error("EQL missing_index \"process where true\"", containsString("Unknown index [missing_index]"));
    }

    public void testUnconvertibleTypeBecomesUnsupportedColumn() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // date_nanos is not in EqlPageConverter.CONVERTIBLE_TYPES, so the column must surface as unsupported (like FROM).
        IndexResolution resolution = indexWith(
            "eql_unconv",
            Map.of("name", field("name", KEYWORD), "ts_nanos", field("ts_nanos", DATE_NANOS))
        );
        LogicalPlan analyzed = analyzer().addIndex("eql_unconv", resolution)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_unconv \"process where true\""));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(names(output), contains("name", "ts_nanos"));
        assertThat(types(output), contains(KEYWORD, UNSUPPORTED));
    }

    public void testEventQuerySchemaWithMetadata() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL eql_test \"process where true\" METADATA _index, _id, _source"));

        assertThat(names(output), contains("@timestamp", "category", "ingested", "name", "pid", "_index", "_id", "_source"));
        assertThat(types(output), contains(DATETIME, KEYWORD, DATETIME, KEYWORD, LONG, KEYWORD, KEYWORD, DataType.SOURCE));
        // The trailing three are metadata attributes, not mapped fields.
        output.subList(5, 8).forEach(a -> assertThat(a, instanceOf(MetadataAttribute.class)));
    }

    public void testSequenceQuerySchemaWithMetadata() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL eql_test \"sequence [process where true] [network where true]\" METADATA _id"));

        // Synthetics first, mapped fields in the middle, metadata last.
        assertThat(
            names(output),
            contains("_sequence", "_sequence_stage", "join_keys", "@timestamp", "category", "ingested", "name", "pid", "_id")
        );
        assertThat(output.get(8), instanceOf(MetadataAttribute.class));
    }

    public void testMetadataDeclaredOrderPreserved() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(analyze("EQL eql_test \"process where true\" METADATA _source, _index"));

        // Declared order, not registry or alphabetical order.
        assertThat(names(output.subList(output.size() - 2, output.size())), contains("_source", "_index"));
    }

    public void testUnsupportedMetadataFieldFails() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error(
                "EQL eql_test \"process where true\" METADATA _score",
                containsString("metadata field [_score] is not supported by the EQL command")
            );
        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error(
                "EQL eql_test \"process where true\" METADATA _version",
                containsString("metadata field [_version] is not supported by the EQL command")
            );
    }

    public void testSampleQuerySchemaWithMetadata() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(
            analyze("EQL eql_test \"sample by category [process where true] [network where true]\" METADATA _id")
        );

        assertThat(output.get(output.size() - 1).name(), equalTo("_id"));
        assertThat(output.get(output.size() - 1), instanceOf(MetadataAttribute.class));
    }

    public void testMultipleMetadataErrorsReported() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error(
                "EQL eql_test \"process where true\" METADATA _score, _bogus",
                allOf(
                    containsString("metadata field [_score] is not supported by the EQL command"),
                    containsString("unknown metadata field [_bogus]")
                )
            );
    }

    public void testUnknownMetadataFieldFails() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error("EQL eql_test \"process where true\" METADATA _bogus", containsString("unknown metadata field [_bogus]"));
    }

    public void testWildcardMetadataFails() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // The EQL delegate resolves no custom tags, so a wildcard cannot match anything.
        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error("EQL eql_test \"process where true\" METADATA _i*", containsString("unknown metadata field [_i*]"));
    }

    public void testUnknownIndexWinsOverBadMetadata() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // Index resolution is validated before metadata, so the "Unknown index" message takes precedence.
        analyzer().addIndex("missing_index", IndexResolution.invalid("Unknown index [missing_index]"))
            .error("EQL missing_index \"process where true\" METADATA _score", containsString("Unknown index [missing_index]"));
    }

    public void testEmptyMappingWithMetadataYieldsMetadataOnlySchema() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // An empty mapping plus a METADATA clause must yield the real metadata column, not the NULL NO_FIELDS placeholder.
        IndexResolution resolution = indexWith("eql_empty", Map.of());
        LogicalPlan analyzed = analyzer().addIndex("eql_empty", resolution)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_empty \"process where true\" METADATA _id"));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(names(output), contains("_id"));
        assertThat(types(output), contains(KEYWORD));
        assertThat(output.get(0), instanceOf(MetadataAttribute.class));
    }

    public void testEmptyMappingYieldsNoFields() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // An index with no mapping must not produce a zero-column relation; mirror FROM and emit NO_FIELDS.
        IndexResolution resolution = indexWith("eql_empty", Map.of());
        LogicalPlan analyzed = analyzer().addIndex("eql_empty", resolution)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_empty \"process where true\""));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(output, hasSize(1));
        assertThat(types(output), contains(DataType.NULL));
    }

    private static LogicalPlan analyze(String query) {
        return analyzer().addIndex(INDEX, "mapping-eql_test.json").buildAnalyzer().analyze(TEST_PARSER.parseQuery(query));
    }

    private static IndexResolution indexWith(String name, Map<String, EsField> mapping) {
        return IndexResolution.valid(
            new EsIndex(name, mapping, Map.of(name, IndexMode.STANDARD), Map.of("", List.of(name)), Map.of("", List.of(name)))
        );
    }

    private static EsField field(String name, DataType type) {
        return new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
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

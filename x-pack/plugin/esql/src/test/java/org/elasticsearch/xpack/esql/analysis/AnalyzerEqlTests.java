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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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

    public void testMappedFieldCollidingWithSequenceSyntheticFails() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A mapped field literally named _sequence collides with the sequence synthetic column; fail loud at analysis
        // rather than emit two output columns of the same name (which a downstream KEEP/SORT could not disambiguate).
        IndexResolution resolution = indexWith(
            "eql_collide",
            Map.of("_sequence", field("_sequence", KEYWORD), "name", field("name", KEYWORD))
        );
        analyzer().addIndex("eql_collide", resolution)
            .error(
                "EQL eql_collide \"sequence [process where true] [network where true]\"",
                containsString("[_sequence] collides with the EQL command's reserved column")
            );
    }

    public void testMappedFieldCollidingWithDeclaredMetadataFails() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A mapped field named _id collides with a declared METADATA _id column (the metadata arm of the guard).
        IndexResolution resolution = indexWith("eql_meta_collide", Map.of("_id", field("_id", KEYWORD), "name", field("name", KEYWORD)));
        analyzer().addIndex("eql_meta_collide", resolution)
            .error(
                "EQL eql_meta_collide \"process where true\" METADATA _id",
                containsString("[_id] collides with the EQL command's reserved column")
            );
    }

    public void testMappedSequenceFieldAllowedInEventMode() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // In EVENT mode there is no _sequence synthetic, so a mapped field named _sequence is an ordinary column, not
        // a collision — the guard must not reject it.
        IndexResolution resolution = indexWith(
            "eql_event_seq",
            Map.of("_sequence", field("_sequence", KEYWORD), "name", field("name", KEYWORD))
        );
        List<Attribute> output = eqlLeafOutput(
            analyzer().addIndex("eql_event_seq", resolution)
                .buildAnalyzer()
                .analyze(TEST_PARSER.parseQuery("EQL eql_event_seq \"process where true\""))
        );
        assertThat(names(output), containsInAnyOrder("_sequence", "name"));
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

    public void testMalformedEqlQueryFails() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A syntactically invalid EQL query string fails at resolve time (ResolveEqlRelation parses the string with the
        // EQL parser) as a ParsingException — the most likely user error surfaces as a 400 with the offending query,
        // not as a 500 deep in the EQL engine at runtime.
        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error("EQL eql_test \"process where\"", ParsingException.class, containsString("cannot parse EQL query [process where]"));
    }

    public void testUnionTypeFieldFlagged() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A field with conflicting types across the pattern's indices (long here, keyword there — the standard shape of
        // a wildcard security pattern) is diagnosed through the blessed FROM flagTypeConflicts() path: the column
        // surfaces as unsupported carrying a message that names the ambiguity, not a generic EQL limitation.
        LinkedHashMap<String, Set<String>> typesToIndices = new LinkedHashMap<>();
        typesToIndices.put("keyword", Set.of("eql_a"));
        typesToIndices.put("long", Set.of("eql_b"));
        InvalidMappedField pidField = new InvalidMappedField("pid", typesToIndices);
        IndexResolution resolution = indexWith("eql_union", Map.of("name", field("name", KEYWORD), "pid", pidField));
        LogicalPlan analyzed = analyzer().addIndex("eql_union", resolution)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_union \"process where true\""));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(names(output), contains("name", "pid"));
        Attribute pid = output.get(1);
        assertThat(pid, instanceOf(UnsupportedAttribute.class));
        assertThat(pid.dataType(), equalTo(UNSUPPORTED));
        assertThat(
            ((UnsupportedAttribute) pid).unresolvedMessage(),
            allOf(containsString("Cannot use field [pid] due to ambiguities"), containsString("keyword"), containsString("long"))
        );
    }

    public void testFieldCapsUnsupportedFieldPassesThrough() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A field field-caps ALREADY resolved as unsupported (e.g. a binary field) passes through the type gate
        // untouched, keeping its original type info — it is not re-wrapped as an "EQL does not support" column.
        IndexResolution resolution = indexWith(
            "eql_binary",
            Map.of("name", field("name", KEYWORD), "blob", new UnsupportedEsField("blob", List.of("binary"), null, Map.of()))
        );
        LogicalPlan analyzed = analyzer().addIndex("eql_binary", resolution)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_binary \"process where true\""));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(names(output), containsInAnyOrder("name", "blob"));
        Attribute blob = output.stream().filter(a -> a.name().equals("blob")).findFirst().orElseThrow();
        assertThat(blob, instanceOf(UnsupportedAttribute.class));
        assertThat(((UnsupportedAttribute) blob).field().getOriginalTypes(), contains("binary"));
    }

    public void testNumericQueryParamStringifiedThenRejected() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A non-string query literal (here a numeric parameter) is stringified before being handed to the EQL parser,
        // which then rejects it — proving the extract path handles non-BytesRef literals, not only quoted strings.
        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .error("EQL eql_test ?", ParsingException.class, containsString("cannot parse EQL query [5]"), 5);
    }

    public void testNonLiteralQueryLeftUnresolvedForVerifier() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // When the query is not a resolvable string literal (here a null-valued parameter), ResolveEqlRelation extracts
        // no query string and leaves the node unresolved for the verifier to reject, rather than throwing at resolve time.
        analyzer().addIndex(INDEX, "mapping-eql_test.json").error("EQL eql_test ?", containsString("Unresolved EQL query"), (Object) null);
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

    public void testNullifyDoesNotCrashOnEqlSource() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // Regression: nullify over an EQL source used to hit assertSourceType's default throw (a 500-class error).
        List<Attribute> output = eqlLeafOutput(
            analyze("EQL eql_test \"process where true\" | WHERE foo == \"x\"", UnmappedResolution.NULLIFY)
        );

        // The downstream-referenced unmapped field is appended last as a NULL-typed column.
        Attribute foo = output.get(output.size() - 1);
        assertThat(foo.name(), equalTo("foo"));
        assertThat(foo.dataType(), equalTo(DataType.NULL));
    }

    public void testLoadAddsKeywordColumnForDownstreamReference() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(
            analyze("EQL eql_test \"process where true\" | WHERE foo == \"x\"", UnmappedResolution.LOAD)
        );

        Attribute foo = output.get(output.size() - 1);
        assertThat(foo.name(), equalTo("foo"));
        assertThat(foo.dataType(), equalTo(KEYWORD));
        assertThat(foo, instanceOf(FieldAttribute.class));
        assertThat(((FieldAttribute) foo).field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }

    public void testDefaultFailsUnknownColumnDownstream() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .unmappedResolution(UnmappedResolution.DEFAULT)
            .error("EQL eql_test \"process where true\" | WHERE foo == \"x\"", containsString("Unknown column [foo]"));
    }

    public void testSequenceModeUnmappedColumnAppendedLast() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(
            analyze("EQL eql_test \"sequence [process where true] [network where true]\" | WHERE foo == \"x\"", UnmappedResolution.LOAD)
        );

        // Order stays synthetics, mapped fields, then the unmapped column last.
        assertThat(
            names(output),
            contains("_sequence", "_sequence_stage", "join_keys", "@timestamp", "category", "ingested", "name", "pid", "foo")
        );
    }

    public void testMetadataThenUnmappedColumnOrder() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        List<Attribute> output = eqlLeafOutput(
            analyze("EQL eql_test \"process where true\" METADATA _index | WHERE foo == \"x\"", UnmappedResolution.NULLIFY)
        );

        // Metadata columns precede the appended unmapped column.
        assertThat(names(output.subList(output.size() - 2, output.size())), contains("_index", "foo"));
    }

    public void testEmptyMappingMarkerReplacedOnLoad() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // The NO_FIELDS placeholder must be replaced by the loaded column, not left alongside it.
        IndexResolution resolution = indexWith("eql_empty", Map.of());
        LogicalPlan analyzed = analyzer().addIndex("eql_empty", resolution)
            .unmappedResolution(UnmappedResolution.LOAD)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_empty \"process where true\" | WHERE foo == \"x\""));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(names(output), contains("foo"));
        assertThat(((FieldAttribute) output.get(0)).field(), instanceOf(PotentiallyUnmappedKeywordEsField.class));
    }

    public void testNullifyUnmappedJoinKeyDoesNotCrashOnEqlSource() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // LOOKUP JOIN puts the EqlRelation as a Join's (n-ary) direct leaf child — the OTHER path that reaches
        // assertSourceType. The unmapped join key must land on the EqlRelation, not throw.
        LogicalPlan analyzed = analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .addLanguagesLookup()
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_test \"process where true\" | LOOKUP JOIN languages_lookup ON language_code"));

        assertThat(names(eqlLeafOutput(analyzed)), hasItem("language_code"));
    }

    public void testEmptyMappingMarkerReplacedOnNullify() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // The NULLIFY branch of the NO_FIELDS replacement (sibling of testEmptyMappingMarkerReplacedOnLoad).
        IndexResolution resolution = indexWith("eql_empty", Map.of());
        LogicalPlan analyzed = analyzer().addIndex("eql_empty", resolution)
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery("EQL eql_empty \"process where true\" | WHERE foo == \"x\""));
        List<Attribute> output = eqlLeafOutput(analyzed);

        assertThat(names(output), contains("foo"));
        assertThat(output.get(0).dataType(), equalTo(DataType.NULL));
    }

    public void testMappedFieldNotTreatedAsUnmapped() {
        assumeTrue("requires EQL command support", EsqlCapabilities.Cap.EQL_COMMAND.isEnabled());

        // A downstream reference to a MAPPED field must not add a column.
        List<Attribute> output = eqlLeafOutput(
            analyze("EQL eql_test \"process where true\" | WHERE name == \"x\"", UnmappedResolution.LOAD)
        );

        assertThat(names(output), contains("@timestamp", "category", "ingested", "name", "pid"));
    }

    private static LogicalPlan analyze(String query) {
        return analyzer().addIndex(INDEX, "mapping-eql_test.json").buildAnalyzer().analyze(TEST_PARSER.parseQuery(query));
    }

    private static LogicalPlan analyze(String query, UnmappedResolution unmappedResolution) {
        return analyzer().addIndex(INDEX, "mapping-eql_test.json")
            .unmappedResolution(unmappedResolution)
            .buildAnalyzer()
            .analyze(TEST_PARSER.parseQuery(query));
    }

    private static IndexResolution indexWith(String name, Map<String, EsField> mapping) {
        return IndexResolution.valid(
            new EsIndex(
                name,
                mapping,
                Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)),
                Map.of("", List.of(name)),
                Map.of("", List.of(name))
            )
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

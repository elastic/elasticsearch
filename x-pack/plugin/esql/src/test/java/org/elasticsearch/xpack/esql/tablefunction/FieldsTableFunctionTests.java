/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactInvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * The columns the function declares, and the rows it builds from a resolution. Both halves are reachable without a cluster: a
 * resolution is the output of the field-caps merge and the input to this function, which is the seam these tests sit on.
 * {@code FieldsTableFunctionIT} covers the half above it, where the resolution comes from indices
 * that really disagree.
 */
public class FieldsTableFunctionTests extends ESTestCase {

    public void testColumnsAreDeclaredWhateverTheInvocation() {
        FieldsTableFunction function = noResolver();
        List<Attribute> columns = function.columns(call("logs-*"), null);

        assertThat(columns.stream().map(Attribute::name).toList(), equalTo(List.of("name", "type", "conflict", "conflicting_types")));
        for (Attribute column : columns) {
            assertThat(column.dataType(), equalTo(DataType.KEYWORD));
        }
        // The columns do not read the invocation or the context, which is what makes a pattern that matches nothing still declare them.
        assertThat(function.columns(call("nope-*"), null), equalTo(columns));
    }

    public void testAPatternMatchingNothingHasNoRows() {
        assertThat(FieldsTableFunction.rows(IndexResolution.empty("nope-*")), equalTo(List.of()));
    }

    /** The reason the function exists: the types that disagree and the indices holding them, as data rather than as an error string. */
    public void testAConflictedFieldCarriesItsTypesAndIndices() {
        Map<String, Set<String>> typesToIndices = new TreeMap<>();
        typesToIndices.put("keyword", Set.of("logs-2026.01"));
        typesToIndices.put("text", Set.of("logs-2026.02"));
        List<List<Object>> rows = FieldsTableFunction.rows(
            resolution(
                Map.of(
                    "message",
                    new InvalidMappedField("message", typesToIndices),
                    "host",
                    new KeywordEsField("host", new HashMap<>(), true, 2048, false, false, EsField.TimeSeriesFieldType.NONE)
                )
            )
        );

        assertThat(rows, hasSize(2));

        List<Object> host = rows.get(0);
        assertThat(host.get(0), equalTo("host"));
        assertThat(host.get(1), equalTo("keyword"));
        assertThat(host.get(2), nullValue());
        assertThat(host.get(3), nullValue());

        List<Object> message = rows.get(1);
        assertThat(message.get(0), equalTo("message"));
        // What an ordinary query sees for this field, and the whole of what it sees.
        assertThat(message.get(1), equalTo("unsupported"));
        assertThat((String) message.get(2), containsString("mapped as [2] incompatible types"));
        assertThat((String) message.get(2), containsString("[keyword] in [logs-2026.01]"));
        assertThat((String) message.get(2), containsString("[text] in [logs-2026.02]"));
        assertThat(message.get(3), equalTo("keyword, text"));
    }

    /**
     * The form the resolver actually produces on a current cluster keeps three index names per type and a count of the rest, so the
     * rendered detail says so and the type set stays complete. A column of index names would have dropped the rest silently.
     */
    public void testACompactConflictStatesItsOwnTruncation() {
        Map<DataType, Set<String>> typesToIndices = new TreeMap<>();
        typesToIndices.put(DataType.KEYWORD, Set.of("logs-1", "logs-2", "logs-3", "logs-4", "logs-5"));
        typesToIndices.put(DataType.LONG, Set.of("logs-6"));
        List<List<Object>> rows = FieldsTableFunction.rows(
            resolution(
                Map.of(
                    "code",
                    CompactInvalidMappedField.mappedEverywhere("code", typesToIndices, new HashMap<>(), EsField.TimeSeriesFieldType.NONE)
                )
            )
        );

        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).get(0), equalTo("code"));
        assertThat((String) rows.get(0).get(2), containsString("and [2] other indices"));
        assertThat(rows.get(0).get(3), equalTo("keyword, long"));
    }

    /** An object's children and a text field's sub-fields both live in the parent's properties, and both are names a query can use. */
    public void testChildFieldsAreRowsNamedByTheirFullPath() {
        Map<String, EsField> properties = new HashMap<>();
        properties.put("ip", new EsField("ip", DataType.IP, new HashMap<>(), true, EsField.TimeSeriesFieldType.NONE));
        EsField host = new EsField("host", DataType.OBJECT, properties, false, true, EsField.TimeSeriesFieldType.NONE);

        List<List<Object>> rows = FieldsTableFunction.rows(resolution(Map.of("host", host)));

        assertThat(rows.stream().map(row -> row.get(0)).toList(), equalTo(List.of("host", "host.ip")));
        assertThat(rows.get(0).get(1), equalTo("object"));
        assertThat(rows.get(1).get(1), equalTo("ip"));
    }

    public void testAnEmptyPatternIsRefused() {
        FieldsTableFunction function = noResolver();
        ParsingException e = expectThrows(ParsingException.class, () -> function.validate(call("  ")));
        assertThat(e.getMessage(), containsString("[fields] requires an index pattern, found an empty one"));
    }

    /** These tests never dispatch, so neither the resolver nor the executor is read: the row builder and the column list need neither. */
    private static FieldsTableFunction noResolver() {
        return new FieldsTableFunction(null, null);
    }

    private static IndexResolution resolution(Map<String, EsField> mapping) {
        return IndexResolution.valid(new EsIndex("logs-*", mapping, Map.of(), Map.of(), Map.of()));
    }

    private static TableFunctionInvocation call(String pattern) {
        return new TableFunctionInvocation(
            Source.EMPTY,
            FieldsTableFunction.NAME,
            List.of(new TableFunctionArgument.Name(Source.EMPTY, pattern))
        );
    }
}

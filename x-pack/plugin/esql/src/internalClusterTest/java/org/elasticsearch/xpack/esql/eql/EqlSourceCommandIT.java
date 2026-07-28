/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end integration tests for the {@code EQL} source command, which delegates to the EQL search transport action
 * and flattens the {@link org.elasticsearch.xpack.eql.action.EqlSearchResponse} into the fixed ES|QL schema
 * {@code _sequence, _index, _id, _source}. The cluster loads the EQL plugin (see build.gradle) so the delegation path
 * runs for real. The command is snapshot-only, so every test bails out on release builds.
 */
public class EqlSourceCommandIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "eql_test";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), EqlPlugin.class);
    }

    @Before
    public void checkSnapshotAndIndex() {
        assumeTrue("EQL source command is snapshot-only", Build.current().isSnapshot());
        assertAcked(indicesAdmin().prepareCreate(INDEX).setMapping("@timestamp", "type=date", "value", "type=long"));
        List<IndexRequestBuilder> docs = List.of(
            prepareIndex(INDEX).setId("1").setSource("@timestamp", "2024-01-01T00:00:01Z", "value", 1),
            prepareIndex(INDEX).setId("2").setSource("@timestamp", "2024-01-01T00:00:02Z", "value", 2),
            prepareIndex(INDEX).setId("3").setSource("@timestamp", "2024-01-01T00:00:03Z", "value", 3)
        );
        indexRandom(true, docs);
    }

    public void testEventQuery() {
        try (EsqlQueryResponse resp = run("EQL \"" + INDEX + "\" | \"any where true\" | KEEP _sequence, _index, _id, _source | SORT _id")) {
            assertThat(columnNames(resp), equalTo(List.of("_sequence", "_index", "_id", "_source")));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                // Plain event queries have no sequence ordinal.
                assertThat(row.get(0), nullValue());
                assertThat(row.get(1), equalTo(INDEX));
            }
            assertThat(rows.get(0).get(2), equalTo("1"));
            assertThat(rows.get(1).get(2), equalTo("2"));
            assertThat(rows.get(2).get(2), equalTo("3"));
            // _source is the raw stored document in whatever xContent type the framework indexed with
            // (JSON/YAML/SMILE/CBOR), so assert only that the column is populated rather than a specific encoding.
            assertThat((String) rows.get(0).get(3), not(emptyOrNullString()));
        }
    }

    public void testSequenceQuery() {
        String query = "EQL \""
            + INDEX
            + "\" | \"sequence [any where value == 1] [any where value == 2]\" | KEEP _sequence, _id | SORT _id";
        try (EsqlQueryResponse resp = run(query)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            // Both matched events belong to the first (0-based) sequence.
            assertThat(rows.get(0).get(0), equalTo(0L));
            assertThat(rows.get(1).get(0), equalTo(0L));
            assertThat(rows.get(0).get(1), equalTo("1"));
            assertThat(rows.get(1).get(1), equalTo("2"));
        }
    }

    public void testEventQueryUnquotedIndex() {
        // The index pattern is parsed like FROM, so a plain unquoted name (no surrounding double quotes) works too.
        try (EsqlQueryResponse resp = run("EQL " + INDEX + " | \"any where true\" | KEEP _id | SORT _id")) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo("1"));
            assertThat(rows.get(2).get(0), equalTo("3"));
        }
    }

    public void testEventQueryWildcardIndexPattern() {
        try (EsqlQueryResponse resp = run("EQL eql_te* | \"any where true\" | KEEP _index, _id")) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(3));
            for (List<Object> row : rows) {
                assertThat(row.get(0), equalTo(INDEX));
            }
        }
    }

    public void testCommaSeparatedIndexPatterns() {
        // Unquoted, comma-separated patterns must reach EQL as distinct indices (not one literal "a,b" name).
        createEventIndex("eql_multi_a", 2);
        createEventIndex("eql_multi_b", 3);
        try (EsqlQueryResponse resp = run("EQL eql_multi_a, eql_multi_b | \"any where true\" | KEEP _index | LIMIT 100")) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(5));
        }
    }

    public void testQuotedCommaSeparatedIndexPatterns() {
        createEventIndex("eql_q_a", 2);
        createEventIndex("eql_q_b", 3);
        try (EsqlQueryResponse resp = run("EQL \"eql_q_a,eql_q_b\" | \"any where true\" | KEEP _index | LIMIT 100")) {
            assertThat(getValuesList(resp), hasSize(5));
        }
    }

    public void testEmptyResultKeepsSchema() {
        try (EsqlQueryResponse resp = run("EQL \"" + INDEX + "\" | \"any where value == 999\"")) {
            assertThat(columnNames(resp), equalTo(List.of("_sequence", "_index", "_id", "_source")));
            assertThat(getValuesList(resp), empty());
        }
    }

    public void testLimitOverridesDefaultEqlSize() {
        // EQL defaults to size=10; with 15 matching events a LIMIT above the source must push size=15 so all 15 come back
        // (without the pushdown the query would silently return only 10).
        String index = "eql_limit_test";
        createEventIndex(index, 15);
        try (EsqlQueryResponse resp = run("EQL \"" + index + "\" | \"any where true\" | LIMIT 15")) {
            assertThat(getValuesList(resp), hasSize(15));
        }
    }

    public void testLimitBelowResultSize() {
        String index = "eql_limit_small";
        createEventIndex(index, 15);
        try (EsqlQueryResponse resp = run("EQL \"" + index + "\" | \"any where true\" | LIMIT 5")) {
            assertThat(getValuesList(resp), hasSize(5));
        }
    }

    private void createEventIndex(String index, int count) {
        assertAcked(indicesAdmin().prepareCreate(index).setMapping("@timestamp", "type=date", "value", "type=long"));
        List<IndexRequestBuilder> docs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            docs.add(
                prepareIndex(index).setId(Integer.toString(i))
                    .setSource("@timestamp", "2024-01-01T00:00:" + String.format("%02d", i) + "Z", "value", i)
            );
        }
        indexRandom(true, docs);
    }

    public void testMalformedEqlQueryErrorIsPropagated() {
        Exception e = expectThrows(Exception.class, () -> run("EQL \"" + INDEX + "\" | \"this is not valid eql\"").close());
        assertThat(e.getMessage(), containsString("line 1"));
    }

    public void testUnknownIndexErrorIsPropagated() {
        Exception e = expectThrows(Exception.class, () -> run("EQL \"missing_index\" | \"any where true\"").close());
        assertThat(e.getMessage(), containsString("missing_index"));
    }

    private static List<String> columnNames(EsqlQueryResponse resp) {
        return resp.columns().stream().map(ColumnInfoImpl::name).toList();
    }
}

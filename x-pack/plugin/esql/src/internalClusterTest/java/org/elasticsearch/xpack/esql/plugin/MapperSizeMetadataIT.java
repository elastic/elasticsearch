/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/**
 *  Tests for the _size metadata value when the mapper-size plugin is installed.
 */
public class MapperSizeMetadataIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MapperSizePlugin.class);
    }

    @Before
    public void setupIndex() {
        createAndPopulateIndex(this::ensureYellow);
    }

    public void testSizeMetadataPresent() {
        var query = """
            FROM test METADATA _size
            | EVAL is_long = _size > 50
            | KEEP id, is_long
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "is_long"));
            assertColumnTypes(resp.columns(), List.of("integer", "boolean"));
            assertValues(resp.values(), List.of(List.of(1, false), List.of(2, true)));
        }
    }

    public void testSizeMetadataFilter() {
        var query = """
            FROM test METADATA _size
            | WHERE _size > 50
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(2)));
        }
    }

    public void testSequenceReadsMapperSizeAsInteger() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
        for (String index : List.of("sequence-size-a", "sequence-size-b")) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(indexSettings(1, 0))
                    .setMapping("@timestamp", "type=date", "key", "type=keyword", "_size", "enabled=true")
            );
        }
        var bulk = client().prepareBulk()
            .add(new IndexRequest("sequence-size-a").id("a").source("@timestamp", "2026-01-01T00:00:00Z", "key", "k"))
            .add(new IndexRequest("sequence-size-b").id("b").source("@timestamp", "2026-01-01T00:00:01Z", "key", "k"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertFalse(bulk.get().hasFailures());
        List<List<Object>> expected;
        try (var control = run("FROM sequence-size-b METADATA _size | KEEP _size | LIMIT 10")) {
            assertColumnTypes(control.columns(), List.of("integer"));
            expected = getValuesList(control);
            assertEquals(1, expected.size());
            assertTrue(expected.getFirst().getFirst() instanceof Integer);
        }
        try (var response = run("""
            SEQUENCE BY key WITHIN 1m PRIMARY 2
            (FROM sequence-size-a), (FROM sequence-size-b METADATA _size)
            | KEEP _size | LIMIT 10
            """)) {
            assertColumnTypes(response.columns(), List.of("integer"));
            assertEquals(expected, getValuesList(response));
        }
    }

    static void createAndPopulateIndex(Consumer<String[]> ensureYellow) {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text", "_size", "enabled=true");
        assertAcked(createRequest);
        client().prepareBulk()
            // _size in the 20's
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "short"))
            // _size in the 70's
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "long long long long long long long longlong long long"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ensureYellow.accept(new String[] { indexName });
    }
}

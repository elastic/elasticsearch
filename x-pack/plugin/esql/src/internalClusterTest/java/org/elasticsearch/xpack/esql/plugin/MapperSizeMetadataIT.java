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
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

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

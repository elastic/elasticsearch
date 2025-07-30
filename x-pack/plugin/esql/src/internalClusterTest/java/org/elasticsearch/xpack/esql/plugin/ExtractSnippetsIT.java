/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ExtractSnippetsIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex(this::ensureYellow);
    }

    public void testExtractSnippets() {
        var query = """
            FROM test
            | EVAL x = extract_snippets(content, "fox", 1, 10)
            | KEEP x
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("x"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }



    static void createAndPopulateIndex(Consumer<String[]> ensureYellow) {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(createRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        var lookupIndexName = "test_lookup";
        createAndPopulateLookupIndex(client, lookupIndexName);

        ensureYellow.accept(new String[] { indexName, lookupIndexName });
    }

    static void createAndPopulateLookupIndex(IndicesAdminClient client, String lookupIndexName) {
        var createRequest = client.prepareCreate(lookupIndexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
            .setMapping("id", "type=integer", "lookup_content", "type=text");
        assertAcked(createRequest);
    }
}

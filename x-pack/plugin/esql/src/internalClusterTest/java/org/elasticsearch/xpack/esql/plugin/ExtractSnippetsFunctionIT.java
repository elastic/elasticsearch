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

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ExtractSnippetsFunctionIT extends AbstractEsqlIntegTestCase {

    private static final List<Object> EMPTY_RESULT = Collections.singletonList(null);

    @Before
    public void setupIndex() {
        createAndPopulateIndex(this::ensureYellow);
    }

    public void testExtractSnippets() {
        var query = """
            FROM test
            | EVAL my_snippet = extract_snippets(content, "fox", 1, 15)
            | SORT my_snippet
            | KEEP my_snippet
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("my_snippet"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(
                resp.values(),
                List.of(List.of("The quick brown"), List.of("This is a brown"), EMPTY_RESULT, EMPTY_RESULT, EMPTY_RESULT, EMPTY_RESULT)
            );
        }
    }

    public void testExtractMultipleSnippets() {
        var query = """
            FROM test
            | EVAL my_snippet = extract_snippets(content, "fox", 3, 15)
            | SORT my_snippet
            | KEEP my_snippet
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("my_snippet"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(List.of("The quick brown", "Afterward, the")),
                    List.of(List.of("This is a brown", "Sometimes the b")),
                    EMPTY_RESULT,
                    EMPTY_RESULT,
                    EMPTY_RESULT,
                    EMPTY_RESULT
                )
            );
        }
    }

    public void testExtractSnippetsWithMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE MATCH(content, "fox")
            | EVAL my_snippet = extract_snippets(content, "fox", 1, 15)
            | SORT my_snippet
            | KEEP my_snippet
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("my_snippet"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(resp.values(), List.of(List.of("The quick brown"), List.of("This is a brown")));
        }
    }

    public void testExtractMultipleSnippetsWithMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE MATCH(content, "fox")
            | EVAL my_snippet = extract_snippets(content, "fox", 3, 15)
            | SORT my_snippet
            | KEEP my_snippet
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("my_snippet"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(
                resp.values(),
                List.of(List.of(List.of("The quick brown", "Afterward, the")), List.of(List.of("This is a brown", "Sometimes the b")))
            );
        }
    }

    public void testExtractSnippetDefaults() {
        var query = """
            FROM test
            | EVAL my_snippet = extract_snippets(content, "fox")
            | SORT my_snippet
            | KEEP my_snippet
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("my_snippet"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(
                resp.values(),
                List.of(List.of("is a brown"), List.of("quick brow"), EMPTY_RESULT, EMPTY_RESULT, EMPTY_RESULT, EMPTY_RESULT)
            );
        }
    }

    public void testExtractSnippetDefaultLength() {
        var query = """
            FROM test
            | EVAL my_snippet = extract_snippets(content, "fox", 3)
            | SORT my_snippet
            | KEEP my_snippet
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("my_snippet"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(List.of("is a brown", "the brown")),
                    List.of(List.of("quick brow", "the brown")),
                    EMPTY_RESULT,
                    EMPTY_RESULT,
                    EMPTY_RESULT,
                    EMPTY_RESULT
                )
            );
        }
    }

    static void createAndPopulateIndex(Consumer<String[]> ensureYellow) {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(createRequest);
        client().prepareBulk().add(new IndexRequest(indexName).id("1").source("id", 1, "content", """
            This is a brown fox that likes to run through the meadow.
            Sometimes the brown fox pauses to look around before continuing.
            """)).add(new IndexRequest(indexName).id("2").source("id", 2, "content", """
            This is a brown dog that spends most of the day sleeping in the yard.
            The brown dog occasionally wakes up to bark at the mailman.
            """)).add(new IndexRequest(indexName).id("3").source("id", 3, "content", """
            This dog is really brown and enjoys chasing sticks near the river.
            People often comment on how brown the dog looks in the sunlight.
            """)).add(new IndexRequest(indexName).id("4").source("id", 4, "content", """
            The quick brown fox jumps over the lazy dog whenever it feels playful.
            Afterward, the brown fox runs off into the forest.
            """)).add(new IndexRequest(indexName).id("5").source("id", 5, "content", """
            There is also a white cat that prefers to sit quietly by the window.
            Unlike the other animals, the white cat ignores everything around it.
            """)).add(new IndexRequest(indexName).id("6").source("id", 6, "content", """
            The dog is brown but this document is very very long, filled with many words describing the scene.
            Even so, the brown dog is still the main focus of the story.
            """)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

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

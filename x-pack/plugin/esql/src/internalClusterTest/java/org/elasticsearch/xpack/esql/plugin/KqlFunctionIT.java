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
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;

public class KqlFunctionIT extends AbstractEsqlIntegTestCase {
    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSimpleKqlQuery() {
        var query = """
            FROM test
            | WHERE kql("content: dog")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testMultiFieldKqlQuery() {
        var query = """
            FROM test
            | WHERE kql("dog OR canine")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValuesInAnyOrder(resp.values(), List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testKqlQueryWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = kql("title: fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[KQL] function is only supported in WHERE and STATS commands"));
    }

    public void testInvalidKqlQueryEof() {
        var query = """
            FROM test
            | WHERE kql("content: ((((dog")
            """;

        var error = expectThrows(QueryShardException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Failed to parse KQL query [content: ((((dog]"));
        assertThat(error.getRootCause().getMessage(), containsString("line 1:11: mismatched input '('"));
    }

    public void testInvalidKqlQueryLexicalError() {
        var query = """
            FROM test
            | WHERE kql(":")
            """;

        var error = expectThrows(QueryShardException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Failed to parse KQL query [:]"));
        assertThat(error.getRootCause().getMessage(), containsString("line 1:1: extraneous input ':' "));
    }

    private void createAndPopulateIndex() {
        var indexName = "test";
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(
                new IndexRequest(indexName).id("1")
                    .source("id", 1, "content", "The quick brown animal swiftly jumps over a lazy dog", "title", "A Swift Fox's Journey")
            )
            .add(
                new IndexRequest(indexName).id("2")
                    .source("id", 2, "content", "A speedy brown fox hops effortlessly over a sluggish canine", "title", "The Fox's Leap")
            )
            .add(
                new IndexRequest(indexName).id("3")
                    .source("id", 3, "content", "Quick and nimble, the fox vaults over the lazy dog", "title", "Brown Fox in Action")
            )
            .add(
                new IndexRequest(indexName).id("4")
                    .source(
                        "id",
                        4,
                        "content",
                        "A fox that is quick and brown jumps over a dog that is quite lazy",
                        "title",
                        "Speedy Animals"
                    )
            )
            .add(
                new IndexRequest(indexName).id("5")
                    .source(
                        "id",
                        5,
                        "content",
                        "With agility, a quick brown fox bounds over a slow-moving dog",
                        "title",
                        "Foxes and Canines"
                    )
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), KqlPlugin.class);
    }
}

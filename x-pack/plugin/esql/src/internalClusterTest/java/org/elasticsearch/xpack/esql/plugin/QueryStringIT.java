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
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;

public class QueryStringIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSimpleQueryString() {
        var query = """
            FROM test
            | WHERE qstr("content: dog")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testMultiFieldQueryString() {
        var query = """
            FROM test
            | WHERE qstr("dog OR canine")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValuesInAnyOrder(resp.values(), List.of(List.of(1), List.of(2), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testQueryStringWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = qstr("title: fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[QSTR] function is only supported in WHERE commands"));
    }

    public void testInvalidQueryStringEof() {
        var query = """
            FROM test
            | WHERE qstr("content: ((((dog")
            """;

        var error = expectThrows(QueryShardException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Failed to parse query [content: ((((dog]"));
        assertThat(error.getRootCause().getMessage(), containsString("Encountered \"<EOF>\" at line 1, column 16"));
    }

    public void testInvalidQueryStringLexicalError() {
        var query = """
            FROM test
            | WHERE qstr("/")
            """;

        var error = expectThrows(QueryShardException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Failed to parse query [/]"));
        assertThat(
            error.getRootCause().getMessage(),
            containsString("Lexical error at line 1, column 2.  Encountered: <EOF> (in lexical state 2)")
        );
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

    public void testWhereQstrWithScoring() {
        assumeTrue("'METADATA _score' is disabled", EsqlCapabilities.Cap.METADATA_SCORE.isEnabled());
        var query = """
            FROM test
            METADATA _score
            | WHERE qstr("content: fox")
            | KEEP id, _score
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValuesInAnyOrder(
                resp.values(),
                List.of(
                    List.of(2, 0.3028995096683502),
                    List.of(3, 0.3028995096683502),
                    List.of(4, 0.2547692656517029),
                    List.of(5, 0.28161853551864624)
                )
            );

        }
    }

    public void testWhereQstrWithScoringSorted() {
        assumeTrue("'METADATA _score' is disabled", EsqlCapabilities.Cap.METADATA_SCORE.isEnabled());
        var query = """
            FROM test
            METADATA _score
            | WHERE qstr("content:fox fox")
            | KEEP id, _score
            | SORT _score DESC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(3, 1.5605685710906982),
                    List.of(2, 0.6057990193367004),
                    List.of(5, 0.5632370710372925),
                    List.of(4, 0.5095385313034058)
                )
            );

        }
    }

    public void testWhereQstrWithScoringNoSort() {
        assumeTrue("'METADATA _score' is disabled", EsqlCapabilities.Cap.METADATA_SCORE.isEnabled());
        var query = """
            FROM test
            METADATA _score
            | WHERE qstr("content: fox")
            | KEEP id, _score
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValuesInAnyOrder(
                resp.values(),
                List.of(
                    List.of(2, 0.3028995096683502),
                    List.of(3, 0.3028995096683502),
                    List.of(4, 0.2547692656517029),
                    List.of(5, 0.28161853551864624)
                )
            );
        }
    }

    public void testWhereQstrWithNonPushableAndScoring() {
        assumeTrue("'METADATA _score' is disabled", EsqlCapabilities.Cap.METADATA_SCORE.isEnabled());
        var query = """
            FROM test
            METADATA _score
            | WHERE qstr("content: fox")
              AND abs(id) > 0
            | EVAL c_score = ceil(_score)
            | KEEP id, c_score
            | SORT id DESC
            | LIMIT 2
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "c_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValuesInAnyOrder(resp.values(), List.of(List.of(5, 1.0), List.of(4, 1.0)));
        }
    }
}

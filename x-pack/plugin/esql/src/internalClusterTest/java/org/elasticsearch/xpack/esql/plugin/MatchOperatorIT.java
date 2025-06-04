/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class MatchOperatorIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSimpleWhereMatch() {
        var query = """
            FROM test
            | WHERE content:"fox"
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testCombinedWhereMatch() {
        var query = """
            FROM test
            | WHERE content:"fox" AND id > 5
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(6)));
        }
    }

    public void testMultipleMatch() {
        var query = """
            FROM test
            | WHERE content:"fox" AND content:"brown"
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testMultipleWhereMatch() {
        var query = """
            FROM test
            | WHERE content:"fox" AND content:"brown"
            | EVAL summary = CONCAT("document with id: ", to_str(id), "and content: ", content)
            | SORT summary
            | LIMIT 4
            | WHERE content:"brown fox"
            | KEEP id
            """;

        // TODO: this should not raise an error;
        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[:] operator cannot be used after LIMIT"));
    }

    public void testNotWhereMatch() {
        var query = """
            FROM test
            | WHERE NOT content:"brown fox"
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(5)));
        }
    }

    public void testWhereMatchWithScoring() {
        var query = """
            FROM test
            METADATA _score
            | WHERE content:"fox"
            | KEEP id, _score
            | SORT id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.156558871269226), List.of(6, 0.9114001989364624)));
        }
    }

    /**
     * Test for https://github.com/elastic/elasticsearch/issues/123967
     */
    public void testWhereMatchWithScoring_AndRequestFilter() {
        var query = """
            FROM test METADATA _score
            | WHERE content:"fox"
            | SORT _score DESC
            | KEEP content, _score
            """;

        QueryBuilder filter = boolQuery().must(matchQuery("content", "brown"));

        try (var resp = run(query, randomPragmas(), filter)) {
            assertColumnNames(resp.columns(), List.of("content", "_score"));
            assertColumnTypes(resp.columns(), List.of("text", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of("This is a brown fox", 1.4274532794952393),
                    List.of("The quick brown fox jumps over the lazy dog", 1.1248724460601807)
                )
            );
        }
    }

    public void testWhereMatchWithScoring_AndNoScoreRequestFilter() {
        var query = """
            FROM test METADATA _score
            | WHERE content:"fox"
            | SORT _score DESC
            | KEEP content, _score
            """;

        QueryBuilder filter = boolQuery().filter(matchQuery("content", "brown"));

        try (var resp = run(query, randomPragmas(), filter)) {
            assertColumnNames(resp.columns(), List.of("content", "_score"));
            assertColumnTypes(resp.columns(), List.of("text", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of("This is a brown fox", 1.156558871269226),
                    List.of("The quick brown fox jumps over the lazy dog", 0.9114001989364624)
                )
            );
        }
    }

    public void testWhereMatchWithScoring_And_MatchAllRequestFilter() {
        var query = """
            FROM test METADATA _score
            | WHERE content:"fox"
            | SORT _score DESC
            | KEEP content, _score
            """;

        QueryBuilder filter = QueryBuilders.matchAllQuery();

        try (var resp = run(query, randomPragmas(), filter)) {
            assertColumnNames(resp.columns(), List.of("content", "_score"));
            assertColumnTypes(resp.columns(), List.of("text", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of("This is a brown fox", 2.1565589904785156),
                    List.of("The quick brown fox jumps over the lazy dog", 1.9114001989364624)
                )
            );
        }
    }

    public void testScoringOutsideQuery() {
        var query = """
            FROM test METADATA _score
            | SORT _score DESC
            | KEEP content, _score
            """;

        QueryBuilder filter = boolQuery().must(matchQuery("content", "fox"));

        try (var resp = run(query, randomPragmas(), filter)) {
            assertColumnNames(resp.columns(), List.of("content", "_score"));
            assertColumnTypes(resp.columns(), List.of("text", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of("This is a brown fox", 1.156558871269226),
                    List.of("The quick brown fox jumps over the lazy dog", 0.9114001989364624)
                )
            );
        }
    }

    public void testScoring_Zero_OutsideQuery() {
        var query = """
            FROM test METADATA _score
            | SORT content DESC
            | KEEP content, _score
            """;

        QueryBuilder filter = boolQuery().filter(matchQuery("content", "fox"));

        try (var resp = run(query, randomPragmas(), filter)) {
            assertColumnNames(resp.columns(), List.of("content", "_score"));
            assertColumnTypes(resp.columns(), List.of("text", "double"));
            assertValues(
                resp.values(),
                List.of(List.of("This is a brown fox", 0.0), List.of("The quick brown fox jumps over the lazy dog", 0.0))
            );
        }
    }

    public void testWhereMatchWithScoringDifferentSort() {
        var query = """
            FROM test
            METADATA _score
            | WHERE content:"fox"
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.156558871269226), List.of(6, 0.9114001989364624)));
        }
    }

    public void testWhereMatchWithScoringNoSort() {
        var query = """
            FROM test
            METADATA _score
            | WHERE content:"fox"
            | KEEP id, _score
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValuesInAnyOrder(resp.values(), List.of(List.of(1, 1.156558871269226), List.of(6, 0.9114001989364624)));
        }
    }

    public void testNonExistingColumn() {
        var query = """
            FROM test
            | WHERE something:"fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [something]"));
    }

    public void testWhereMatchEvalColumn() {
        var query = """
            FROM test
            | EVAL upper_content = to_upper(content)
            | WHERE upper_content:"FOX"
            | KEEP id
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[:] operator cannot operate on [upper_content], which is not a field from an index mapping")
        );
    }

    public void testWhereMatchOverWrittenColumn() {
        var query = """
            FROM test
            | DROP content
            | EVAL content = CONCAT("document with ID ", to_str(id))
            | WHERE content:"document"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[:] operator cannot operate on [content], which is not a field from an index mapping")
        );
    }

    public void testWhereMatchAfterStats() {
        var query = """
            FROM test
            | STATS count(*)
            | WHERE content:"fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [content]"));
    }

    public void testWhereMatchNotPushedDown() {
        var query = """
            FROM test
            | WHERE content:"fox" OR length(content) < 20
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(2), List.of(6)));
        }
    }

    public void testWhereMatchWithRow() {
        var query = """
            ROW content = "a brown fox"
            | WHERE content:"fox"
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("line 2:9: [:] operator cannot operate on [content], which is not a field from an index mapping")
        );
    }

    public void testMatchWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = content:"fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[:] operator is only supported in WHERE and STATS commands"));
    }

    public void testMatchWithNonTextField() {
        var query = """
            FROM test
            | WHERE id:3
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(3)));
        }
    }

    private void createAndPopulateIndex() {
        var indexName = "test";
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }
}

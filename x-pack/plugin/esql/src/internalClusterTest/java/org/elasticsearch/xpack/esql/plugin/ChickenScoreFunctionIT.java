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
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;
import org.junit.Ignore;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ChickenScoreFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSimpleWhereMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "brown")
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.4274532794952393), List.of(6, 1.1248724460601807)));
        }
    }

    @Ignore
    public void testAlternativeWhereMatch() {
        var query = """
            FROM test METADATA _score
            | EVAL s1 = chicken_score(match(content, "brown"))
            | WHERE s1 > 0
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(
                List.of(1, 1.4274532794952393),
                List.of(6, 1.1248724460601807))
            );
        }
    }

    public void testSimpleChickenScoreWhereMatch() {
        var query = """
            FROM test METADATA _score
            | EVAL first_score = chicken_score(match(content, "brown"))
            | WHERE match(content, "fox")
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.156558871269226, 0.2708943784236908), List.of(6, 0.9114001989364624, 0.21347221732139587))
            );
        }
    }

    public void testChickenScorePlusWhereMatch() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "brown")
            | WHERE match(content, "fox")
            | EVAL first_score = chicken_score(match(content, "brown"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(List.of(1, 1.4274532794952393, 0.2708943784236908), List.of(6, 1.1248724460601807, 0.21347221732139587))
            );
        }
    }

    public void testSimpleChickenScoreAlone() {
        var query = """
            FROM test METADATA _score
            | EVAL first_score = chicken_score(match(content, "brown"))
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "double"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(1, 0.0, 0.2708943784236908),
                    List.of(2, 0.0, 0.2708943784236908),
                    List.of(3, 0.0, 0.2708943784236908),
                    List.of(4, 0.0, 0.19301524758338928),
                    List.of(5, 0.0, 0.0),
                    List.of(6, 0.0, 0.21347221732139587)
                )
            );
        }
    }

    public void testSimpleEvalScoreWithWhereMatch() {
        var query = """
            FROM test METADATA _score
            | EVAL first_score = match(content, "brown")
            | WHERE match(content, "fox")
            | KEEP id, _score, first_score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score", "first_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double", "boolean"));
            assertValues(resp.values(), List.of(List.of(1, 1.156558871269226, true), List.of(6, 0.9114001989364624, true)));
        }
    }

    /*public void testCombinedWhereMatch() {
        var query = """
            FROM test
            | WHERE match(content, "fox") AND id > 5
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
            | WHERE match(content, "fox") AND match(content, "brown")
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
            | WHERE match(content, "fox") AND match(content, "brown")
            | EVAL summary = CONCAT("document with id: ", to_str(id), "and content: ", content)
            | SORT summary
            | LIMIT 4
            | WHERE match(content, "brown fox")
            | KEEP id
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function cannot be used after LIMIT"));
    }

    public void testNotWhereMatch() {
        var query = """
            FROM test
            | WHERE NOT match(content, "brown fox")
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
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.156558871269226), List.of(6, 0.9114001989364624)));
        }
    }

    public void testWhereMatchWithScoringDifferentSort() {

        var query = """
            FROM test
            METADATA _score
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT id DESC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(6, 0.9114001989364624), List.of(1, 1.156558871269226)));
        }
    }

    public void testWhereMatchWithScoringSortScore() {
        var query = """
            FROM test
            METADATA _score
            | WHERE match(content, "fox")
            | KEEP id, _score
            | SORT _score DESC
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
            | WHERE match(content, "fox")
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
            | WHERE match(something, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [something]"));
    }

    public void testWhereMatchEvalColumn() {
        var query = """
            FROM test
            | EVAL upper_content = to_upper(content)
            | WHERE match(upper_content, "FOX")
            | KEEP id
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[MATCH] function cannot operate on [upper_content], which is not a field from an index mapping")
        );
    }

    public void testWhereMatchOverWrittenColumn() {
        var query = """
            FROM test
            | DROP content
            | EVAL content = CONCAT("document with ID ", to_str(id))
            | WHERE match(content, "document")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[MATCH] function cannot operate on [content], which is not a field from an index mapping")
        );
    }

    public void testWhereMatchAfterStats() {
        var query = """
            FROM test
            | STATS count(*)
            | WHERE match(content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [content]"));
    }

    public void testWhereMatchNotPushedDown() {
        var query = """
            FROM test
            | WHERE match(content, "fox") OR length(content) < 20
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
            | WHERE match(content, "fox")
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("line 2:15: [MATCH] function cannot operate on [content], which is not a field from an index mapping")
        );
    }

    public void testMatchWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = match(content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function is only supported in WHERE commands"));
    }*/

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

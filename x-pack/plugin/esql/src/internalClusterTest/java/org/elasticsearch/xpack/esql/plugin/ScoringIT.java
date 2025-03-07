/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ScoringIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
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
    }

    public void testMatchAllScoring() {
        var query = """
            FROM test
            METADATA _score
            | KEEP id, _score
            """;

        assertZeroScore(query);
    }

    public void testNonPushableFunctionsScoring() {
        var query = """
            FROM test
            METADATA _score
            | WHERE length(content) < 20
            | KEEP id, _score
            """;

        assertZeroScore(query);

        query = """
            FROM test
            METADATA _score
            | WHERE length(content) < 20 OR id > 4
            | KEEP id, _score
            """;

        assertZeroScore(query);

        query = """
            FROM test
            METADATA _score
            | WHERE length(content) < 20 AND id < 4
            | KEEP id, _score
            """;

        assertZeroScore(query);
    }

    public void testPushableFunctionsScoring() {
        var query = """
            FROM test
            METADATA _score
            | WHERE id > 4
            | KEEP id, _score
            | SORT id ASC
            """;

        assertZeroScore(query);

        query = """
            FROM test
            METADATA _score
            | WHERE id > 4 AND id < 7
            | KEEP id, _score
            | SORT id ASC
            """;

        assertZeroScore(query);
    }

    private void assertZeroScore(String query) {
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp.values());
            for (List<Object> value : values) {
                assertThat((Double) value.get(1), equalTo(0.0));
            }
        }
    }

    public void testPushableAndFullTextFunctionsConjunctionScoring() {
        var queryWithoutFilter = """
            FROM test
            METADATA _score
            | WHERE content:"fox"
            | KEEP id, _score
            | SORT id ASC
            """;
        var query = """
            FROM test
            METADATA _score
            | WHERE content:"fox" AND id > 4
            | KEEP id, _score
            | SORT id ASC
            """;
        checkSameScores(queryWithoutFilter, query);

        query= """
            FROM test
            METADATA _score
            | WHERE content:"fox" AND (id > 4 or id < 2)
            | KEEP id, _score
            | SORT id ASC
            """;
        queryWithoutFilter = """
            FROM test
            METADATA _score
            | WHERE content:"fox"
            | KEEP id, _score
            | SORT id ASC
            """;
        checkSameScores(queryWithoutFilter, query);
    }

    private void checkSameScores(String queryWithoutFilter, String query) {
        Map<Integer, Double> expectedScores = new HashMap<>();
        try (var respWithoutFilter = run(queryWithoutFilter)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(respWithoutFilter);
            for (List<Object> result : valuesList) {
                expectedScores.put((Integer) result.get(0), (Double) result.get(1));
            }
        }
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = EsqlTestUtils.getValuesList(resp.values());
            for (List<Object> value : values) {
                Double score = (Double) value.get(1);
                assertThat(score, greaterThan(0.0));
                assertThat(expectedScores.get((Integer)value.get(0)), equalTo(score));
            }
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

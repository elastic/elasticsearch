/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.kql.KqlPlugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ScoringIT extends AbstractEsqlIntegTestCase {

    private final String matchingClause;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), KqlPlugin.class);
    }

    @ParametersFactory
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { "match(content, \"fox\")" });
        params.add(new Object[] { "content:\"fox\"" });
        params.add(new Object[] { "qstr(\"content: fox\")" });
        params.add(new Object[] { "kql(\"content*: fox\")" });
        params.add(new Object[] { "term(content, \"fox\")" });
        return params;
    }

    public ScoringIT(String matchingClause) {
        this.matchingClause = matchingClause;
    }

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testWhereMatchWithScoring() {
        var query = """
            FROM test
            METADATA _score
            | WHERE %s
            | KEEP id, _score
            | SORT id ASC
            """.formatted(matchingClause);

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
            | WHERE %s
            | KEEP id, _score
            | SORT id DESC
            """.formatted(matchingClause);
        ;

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
            | WHERE %s
            | KEEP id, _score
            | SORT _score DESC
            """.formatted(matchingClause);

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
            | WHERE %s
            | KEEP id, _score
            """.formatted(matchingClause);

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValuesInAnyOrder(resp.values(), List.of(List.of(1, 1.156558871269226), List.of(6, 0.9114001989364624)));
        }
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
            | WHERE %s
            | KEEP id, _score
            | SORT id ASC
            """.formatted(matchingClause);
        ;
        var query = """
            FROM test
            METADATA _score
            | WHERE %s AND id > 4
            | KEEP id, _score
            | SORT id ASC
            """.formatted(matchingClause);
        checkSameScores(queryWithoutFilter, query);

        query = """
            FROM test
            METADATA _score
            | WHERE %s AND (id > 4 or id < 2)
            | KEEP id, _score
            | SORT id ASC
            """.formatted(matchingClause);
        queryWithoutFilter = """
            FROM test
            METADATA _score
            | WHERE %s
            | KEEP id, _score
            | SORT id ASC
            """.formatted(matchingClause);
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
                assertThat(expectedScores.get((Integer) value.get(0)), equalTo(score));
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

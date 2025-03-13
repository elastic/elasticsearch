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

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ScoringIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testDefaultScoring() {
        var query = """
            FROM test METADATA _score
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);

            assertThat(values.size(), equalTo(6));

            for (int i = 0; i < 6; i++) {
                assertThat(values.get(0).get(1), equalTo(1.0));
            }
        }
    }

    public void testScoringNonPushableFunctions() {
        var query = """
            FROM test METADATA _score
            | WHERE length(content) < 20
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(2));

            assertThat(values.get(0).get(0), equalTo(1));
            assertThat(values.get(1).get(0), equalTo(2));

            assertThat((Double) values.get(0).get(1), is(1.0));
            assertThat((Double) values.get(1).get(1), is(1.0));
        }
    }

    public void testDisjunctionScoring() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "fox") OR length(content) < 20
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(3));

            assertThat(values.get(0).get(0), equalTo(1));
            assertThat(values.get(1).get(0), equalTo(6));
            assertThat(values.get(2).get(0), equalTo(2));

            // Matches full text query and non pushable query
            assertThat((Double) values.get(0).get(1), greaterThan(1.0));
            assertThat((Double) values.get(1).get(1), greaterThan(1.0));
            // Matches just non pushable query
            assertThat((Double) values.get(2).get(1), equalTo(1.0));
        }
    }

    public void testConjunctionPushableScoring() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "fox") AND id > 4
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(1));

            assertThat(values.get(0).get(0), equalTo(6));

            // Matches full text query and pushable query
            assertThat((Double) values.get(0).get(1), greaterThan(1.0));
        }
    }

    public void testConjunctionNonPushableScoring() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "fox") AND length(content) < 20
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(1));

            assertThat(values.get(0).get(0), equalTo(1));

            // Matches full text query and pushable query
            assertThat((Double) values.get(0).get(1), greaterThan(1.0));
        }
    }

    public void testDisjunctionScoringPushableFunctions() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "fox") OR match(content, "quick")
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(2));

            assertThat(values.get(0).get(0), equalTo(6));
            assertThat(values.get(1).get(0), equalTo(1));

            // Matches both conditions
            assertThat((Double) values.get(0).get(1), greaterThan(2.0));
            // Matches a single condition
            assertThat((Double) values.get(1).get(1), greaterThan(1.0));
        }
    }

    public void testDisjunctionScoringMultipleNonPushableFunctions() {
        var query = """
            FROM test METADATA _score
            | WHERE match(content, "fox") OR length(content) < 20 AND id > 2
            | KEEP id, _score
            | SORT _score DESC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(2));

            assertThat(values.get(0).get(0), equalTo(1));
            assertThat(values.get(1).get(0), equalTo(6));

            // Matches the full text query and the two pushable query
            assertThat((Double) values.get(0).get(1), greaterThan(2.0));
            assertThat((Double) values.get(0).get(1), lessThan(3.0));
            // Matches just the match function
            assertThat((Double) values.get(1).get(1), lessThan(2.0));
            assertThat((Double) values.get(1).get(1), greaterThan(1.0));
        }
    }

    public void testDisjunctionScoringWithNot() {
        var query = """
            FROM test METADATA _score
            | WHERE NOT(match(content, "dog")) OR length(content) > 50
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(3));

            assertThat(values.get(0).get(0), equalTo(1));
            assertThat(values.get(1).get(0), equalTo(4));
            assertThat(values.get(2).get(0), equalTo(5));

            // Matches NOT gets 0.0 and default score is 1.0
            assertThat((Double) values.get(0).get(1), equalTo(1.0));
            assertThat((Double) values.get(1).get(1), equalTo(1.0));
            assertThat((Double) values.get(2).get(1), equalTo(1.0));
        }
    }

    public void testScoringWithNoFullTextFunction() {
        var query = """
            FROM test METADATA _score
            | WHERE length(content) > 50
            | KEEP id, _score
            | SORT _score DESC, id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(1));

            assertThat(values.get(0).get(0), equalTo(4));

            // Non pushable query gets score of 0.0, summed with 1.0 coming from Lucene
            assertThat((Double) values.get(0).get(1), equalTo(1.0));
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

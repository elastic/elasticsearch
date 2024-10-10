/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class MatchOperatorIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    @Override
    protected EsqlQueryResponse run(EsqlQueryRequest request) {
        assumeTrue("match operator available in snapshot builds only", Build.current().isSnapshot());
        return super.run(request);
    }

    public void testSimpleWhereMatch() {
        var query = """
            FROM test
            | WHERE content MATCH "fox"
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(List.of("id")));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::type).map(DataType::toString).toList(), equalTo(List.of("INTEGER")));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertMap(values, matchesList().item(List.of(1)).item(List.of(6)));
        }
    }

    public void testCombinedWhereMatch() {
        var query = """
            FROM test
            | WHERE content MATCH "fox" AND id > 5
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(List.of(("id"))));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::type).map(DataType::toString).toList(), equalTo(List.of(("INTEGER"))));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertMap(values, matchesList().item(List.of(6)));
        }
    }

    public void testMultipleMatch() {
        var query = """
            FROM test
            | WHERE content MATCH "fox" OR content MATCH "brown"
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(List.of(("id"))));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::type).map(DataType::toString).toList(), equalTo(List.of(("INTEGER"))));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.size(), equalTo(5));
            assertMap(values, matchesList().item(List.of(1)).item(List.of(2)).item(List.of(3)).item(List.of(4)).item(List.of(6)));
        }
    }

    public void testMultipleWhereMatch() {
        var query = """
            FROM test
            | WHERE content MATCH "fox" OR content MATCH "brown"
            | EVAL summary = CONCAT("document with id: ", to_str(id), "and content: ", content)
            | SORT summary
            | LIMIT 4
            | WHERE content MATCH "brown fox"
            | KEEP id
            """;

        // TODO: this should not raise an error;
        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unsupported expression [content MATCH \"brown fox\"]"));
    }

    public void testNotWhereMatch() {
        var query = """
            FROM test
            | WHERE NOT content MATCH "brown fox"
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertThat(resp.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(List.of(("id"))));
            assertThat(resp.columns().stream().map(ColumnInfoImpl::type).map(DataType::toString).toList(), equalTo(List.of(("INTEGER"))));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertMap(values, matchesList().item(List.of(5)));
        }
    }

    public void testNonExistingColumn() {
        var query = """
            FROM test
            | WHERE something MATCH "fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [something]"));
    }

    public void testWhereMatchEvalColumn() {
        var query = """
            FROM test
            | EVAL upper_content = to_upper(content)
            | WHERE upper_content MATCH "FOX"
            | KEEP id
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("MATCH requires a mapped index field, found [upper_content]"));
    }

    public void testWhereMatchOverWrittenColumn() {
        var query = """
            FROM test
            | DROP content
            | EVAL content = CONCAT("document with ID ", to_str(id))
            | WHERE content MATCH "document"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("MATCH requires a mapped index field, found [content]"));
    }

    public void testWhereMatchAfterStats() {
        var query = """
            FROM test
            | STATS count(*)
            | WHERE content match "fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [content]"));
    }

    public void testWhereMatchWithFunctions() {
        var query = """
            FROM test
            | WHERE content MATCH "fox" OR to_upper(content) == "FOX"
            """;
        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(error.getMessage(), containsString(" Invalid condition using MATCH"));
    }

    public void testWhereMatchWithRow() {
        var query = """
            ROW content = "a brown fox"
            | WHERE content MATCH "fox"
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("MATCH requires a mapped index field, found [content]"));
    }

    public void testMatchWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = content MATCH "fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("EVAL does not support MATCH expressions"));
    }

    public void testMatchWithNonTextField() {
        var query = """
            FROM test
            | WHERE id MATCH "fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString(" MATCH requires a text or keyword field, but [id] has type [integer]"));
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

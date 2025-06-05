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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.CoreMatchers.containsString;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class MatchPhraseFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex();
    }

    public void testSimpleWhereMatchPhrase() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "brown fox")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testSimpleWhereMatchPhraseNoResults() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "fox brown")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), Collections.emptyList());
        }
    }

    public void testSimpleWhereMatchPhraseAndSlop() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "fox brown", {"slop": 5})
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testCombinedWhereMatchPhrase() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "brown fox") AND id > 5
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(6)));
        }
    }

    public void testMultipleMatchPhrase() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "the quick") AND match_phrase(content, "brown fox")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(6)));
        }
    }

    public void testMultipleWhereMatchPhrase() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "the quick") AND match_phrase(content, "brown fox")
            | EVAL summary = CONCAT("document with id: ", to_str(id), "and content: ", content)
            | SORT summary
            | LIMIT 4
            | WHERE match_phrase(content, "lazy dog")
            | KEEP id
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MatchPhrase] function cannot be used after LIMIT"));
    }

    public void testNotWhereMatchPhrase() {
        var query = """
            FROM test
            | WHERE NOT match_phrase(content, "brown fox")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(2), List.of(3), List.of(4), List.of(5)));
        }
    }

    public void testWhereMatchPhraseWithScoring() {
        var query = """
            FROM test
            METADATA _score
            | WHERE match_phrase(content, "brown fox")
            | KEEP id, _score
            | SORT id ASC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.4274532794952393), List.of(6, 1.1248723268508911)));
        }
    }

    public void testWhereMatchPhraseWithScoringDifferentSort() {

        var query = """
            FROM test
            METADATA _score
            | WHERE match_phrase(content, "brown fox")
            | KEEP id, _score
            | SORT id DESC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(6, 1.1248723268508911), List.of(1, 1.4274532794952393)));
        }
    }

    public void testWhereMatchPhraseWithScoringSortScore() {
        var query = """
            FROM test
            METADATA _score
            | WHERE match_phrase(content, "brown fox")
            | KEEP id, _score
            | SORT _score DESC
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 1.4274532794952393), List.of(6, 1.1248723268508911)));
        }
    }

    public void testWhereMatchPhraseWithScoringNoSort() {
        var query = """
            FROM test
            METADATA _score
            | WHERE match_phrase(content, "brown fox")
            | KEEP id, _score
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValuesInAnyOrder(resp.values(), List.of(List.of(1, 1.4274532794952393), List.of(6, 1.1248723268508911)));
        }
    }

    public void testNonExistingColumn() {
        var query = """
            FROM test
            | WHERE match_phrase(something, "brown fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [something]"));
    }

    public void testWhereMatchPhraseEvalColumn() {
        var query = """
            FROM test
            | EVAL upper_content = to_upper(content)
            | WHERE match_phrase(upper_content, "BROWN FOX")
            | KEEP id
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[MatchPhrase] function cannot operate on [upper_content], which is not a field from an index mapping")
        );
    }

    public void testWhereMatchPhraseOverWrittenColumn() {
        var query = """
            FROM test
            | DROP content
            | EVAL content = CONCAT("document with ID ", to_str(id))
            | WHERE match_phrase(content, "document content")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("[MatchPhrase] function cannot operate on [content], which is not a field from an index mapping")
        );
    }

    public void testWhereMatchPhraseAfterStats() {
        var query = """
            FROM test
            | STATS count(*)
            | WHERE match_phrase(content, "brown fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("Unknown column [content]"));
    }

    public void testWhereMatchPhraseNotPushedDown() {
        var query = """
            FROM test
            | WHERE match_phrase(content, "brown fox") OR length(content) < 20
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(2), List.of(6)));
        }
    }

    public void testWhereMatchPhraseWithRow() {
        var query = """
            ROW content = "a brown fox"
            | WHERE match_phrase(content, "brown fox")
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("line 2:22: [MatchPhrase] function cannot operate on [content], which is not a field from an index mapping")
        );
    }

    public void testMatchPhraseWithStats() {
        var errorQuery = """
            FROM test
            | STATS c = count(*) BY match_phrase(content, "brown fox")
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(errorQuery));
        assertThat(error.getMessage(), containsString("[MatchPhrase] function is only supported in WHERE and STATS commands"));

        var query = """
            FROM test
            | STATS c = count(*) WHERE match_phrase(content, "brown fox"), d = count(*) WHERE match_phrase(content, "lazy dog")
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("c", "d"));
            assertColumnTypes(resp.columns(), List.of("long", "long"));
            assertValues(resp.values(), List.of(List.of(2L, 1L)));
        }

        query = """
            FROM test METADATA _score
            | WHERE match_phrase(content, "brown fox")
            | STATS m = max(_score), n = min(_score)
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("m", "n"));
            assertColumnTypes(resp.columns(), List.of("double", "double"));
            List<List<Object>> valuesList = getValuesList(resp.values());
            assertEquals(1, valuesList.size());
            assertThat((double) valuesList.get(0).get(0), Matchers.greaterThan(1.0));
            assertThat((double) valuesList.get(0).get(1), Matchers.greaterThan(0.0));
        }
    }

    public void testMatchPhraseWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = match_phrase(content, "brown fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MatchPhrase] function is only supported in WHERE and STATS commands"));
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

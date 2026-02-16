/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.CoreMatchers.containsString;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class MatchOperatorIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        MatchFunctionIT.createAndPopulateIndex(this::ensureYellow);
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

        try (var resp = run(syncEsqlQueryRequest(query).pragmas(randomPragmas()).filter(filter))) {
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

        try (var resp = run(syncEsqlQueryRequest(query).pragmas(randomPragmas()).filter(filter))) {
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

        try (var resp = run(syncEsqlQueryRequest(query).pragmas(randomPragmas()).filter(filter))) {
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

        try (var resp = run(syncEsqlQueryRequest(query).pragmas(randomPragmas()).filter(filter))) {
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

        try (var resp = run(syncEsqlQueryRequest(query).pragmas(randomPragmas()).filter(filter))) {
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

    public void testMatchOperatorWithLookupJoin() {
        var query = """
            FROM test
            | LOOKUP JOIN test_lookup ON id
            | WHERE id > 0 AND lookup_content : "fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString(
                "line 3:20: [:] operator cannot operate on [lookup_content], supplied by an index [test_lookup] "
                    + "in non-STANDARD mode [lookup]"
            )
        );
    }
}

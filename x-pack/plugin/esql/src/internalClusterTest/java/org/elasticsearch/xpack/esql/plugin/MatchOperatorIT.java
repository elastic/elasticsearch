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
        MatchFunctionIT.createAndPopulateIndices(this::ensureYellow);
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

    public void testWhereMatchOverWrittenColumn() {
        var query = """
            FROM test
            | DROP content
            | EVAL content = to_text(CONCAT("document with ID ", to_str(id)))
            | WHERE content:"document"
            | KEEP id, content
            | SORT id
            | LIMIT 2
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            assertValues(resp.values(), List.of(List.of(1, "document with ID 1"), List.of(2, "document with ID 2")));
        }
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

    public void testMatchOperatorAfterMvExpand() {
        var query = """
            FROM test
            | MV_EXPAND content
            | EVAL content = to_text(content)
            | WHERE content : "fox"
            | SORT id, content
            | KEEP id, content
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            assertValues(
                resp.values(),
                List.of(List.of(1, "This is a brown fox"), List.of(6, "The quick brown fox jumps over the lazy dog"))
            );
        }
    }

    public void testWhereFalseBeforeInlineStatsWithMatchOperator() {
        var query = """
            FROM test
            | WHERE false
            | INLINE STATS max_id = MAX(id)
            | WHERE content:"fox"
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[:] operator cannot be used after INLINE"));
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

    public void testMatchWithRow() {
        var query = """
            ROW content = to_text(["This is a brown fox", "This is a brown dog", "This dog is really brown"])
            | MV_EXPAND content
            | WHERE content:"dog"
            | SORT content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content"));
            assertColumnTypes(resp.columns(), List.of("text"));
            assertValues(resp.values(), List.of(List.of("This dog is really brown"), List.of("This is a brown dog")));
        }
    }

    public void testMatchRuntimeExpression() {
        var query = """
            FROM test
            | EVAL new_content = to_text(concat(content, " and a white cat"))
            | WHERE new_content:"fox"
            | SORT new_content
            | KEEP new_content
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("new_content"));
            assertColumnTypes(resp.columns(), List.of("text"));
            assertValues(
                resp.values(),
                List.of(
                    List.of("The quick brown fox jumps over the lazy dog and a white cat"),
                    List.of("This is a brown fox and a white cat")
                )
            );
        }
    }
}

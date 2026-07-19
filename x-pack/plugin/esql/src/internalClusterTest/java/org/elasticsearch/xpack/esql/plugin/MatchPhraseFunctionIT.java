/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.plugin.MatchFunctionIT.createAndPopulateIndices;
import static org.hamcrest.CoreMatchers.containsString;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class MatchPhraseFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndices(this::ensureYellow);
    }

    /**
     * Runtime match_phrase is gated behind a snapshot-only capability; in release builds the queries these tests
     * run are rejected by the verifier instead.
     */
    private static void assumeRuntimeMatchPhraseEnabled() {
        assumeTrue("requires runtime match_phrase", MatchPhrase.runtimeSearchEnabled());
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
        assumeRuntimeMatchPhraseEnabled();
        // to_upper produces a keyword, so runtime match_phrase compares the whole value exactly: the phrase-like
        // "BROWN FOX" query matches nothing, only the complete value does.
        var query = """
            FROM test
            | EVAL upper_content = to_upper(content)
            | WHERE match_phrase(upper_content, "BROWN FOX")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), Collections.emptyList());
        }

        query = """
            FROM test
            | EVAL upper_content = to_upper(content)
            | WHERE match_phrase(upper_content, "THIS IS A BROWN FOX")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1)));
        }
    }

    public void testWhereMatchPhraseOverWrittenColumn() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            FROM test
            | DROP content
            | EVAL content = CONCAT("document with ID ", to_str(id))
            | WHERE match_phrase(content, "document content")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), Collections.emptyList());
        }

        query = """
            FROM test
            | DROP content
            | EVAL content = CONCAT("document with ID ", to_str(id))
            | WHERE match_phrase(content, "document with ID 3")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(3)));
        }
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
        assumeRuntimeMatchPhraseEnabled();
        // A ROW string literal is a keyword: runtime match_phrase requires the exact value, not a phrase within it.
        var query = """
            ROW content = "a brown fox"
            | WHERE match_phrase(content, "brown fox")
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(resp.values(), Collections.emptyList());
        }

        query = """
            ROW content = "a brown fox"
            | WHERE match_phrase(content, "a brown fox")
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content"));
            assertColumnTypes(resp.columns(), List.of("keyword"));
            assertValues(resp.values(), List.of(List.of("a brown fox")));
        }
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

    public void testMatchPhraseAfterMvExpand() {
        assumeRuntimeMatchPhraseEnabled();
        // After MV_EXPAND on the searched field, the expanded attribute is no longer a direct index field, so
        // runtime search takes over: the MV_EXPAND restriction is bypassed and the phrase is evaluated per row.
        var query = """
            FROM test
            | MV_EXPAND content
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

    public void testMatchPhraseAfterMvExpandWithIntermediateCommands() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            FROM test
            | MV_EXPAND content
            | EVAL upper_content = to_upper(content)
            | WHERE match_phrase(content, "brown fox")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }

        query = """
            FROM test
            | MV_EXPAND content
            | SORT id
            | KEEP id, content
            | WHERE match_phrase(content, "brown fox")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testWhereFalseBeforeInlineStatsWithMatchPhrase() {
        var query = """
            FROM test
            | WHERE false
            | INLINE STATS max_id = MAX(id)
            | WHERE match_phrase(content, "brown fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MatchPhrase] function cannot be used after INLINE"));
    }

    public void testWhereFalseWithEvalBeforeInlineStatsAndMatchPhrase() {
        var query = """
            FROM test
            | WHERE false
            | EVAL doubled_id = id * 2
            | INLINE STATS avg_id = AVG(id) BY doubled_id
            | WHERE match_phrase(content, "brown fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MatchPhrase] function cannot be used after INLINE"));
    }

    // ---- runtime match_phrase: searching text expressions that are not index-mapped fields ----

    public void testSimpleWhereRuntimeMatchPhrase() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            FROM test
            | WHERE match_phrase(to_text(concat(content, " extra")), "brown fox")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testRuntimeMatchPhraseOrderMatters() {
        assumeRuntimeMatchPhraseEnabled();
        // Both tokens exist in ids 1 and 6, but never adjacent in this order, so a runtime phrase matches nothing.
        var query = """
            FROM test
            | WHERE match_phrase(to_text(concat(content, " extra")), "fox brown")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), Collections.emptyList());
        }
    }

    public void testWhereRuntimeMatchPhraseEvalTextColumn() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            FROM test
            | EVAL text_content = content
            | WHERE match_phrase(text_content, "brown fox")
            | KEEP id
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(1), List.of(6)));
        }
    }

    public void testWhereRuntimeMatchPhraseWithRow() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            ROW content = to_text("a brown fox")
            | WHERE match_phrase(content, "brown fox")
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content"));
            assertColumnTypes(resp.columns(), List.of("text"));
            assertValues(resp.values(), List.of(List.of("a brown fox")));
        }
    }

    public void testWhereRuntimeMatchPhraseKeyword() {
        assumeRuntimeMatchPhraseEnabled();
        // concat produces a keyword: runtime match_phrase preserves the pushed-down term-query semantics and
        // matches on the exact value.
        var query = """
            FROM test
            | EVAL suffixed = concat(content, " extra")
            | WHERE match_phrase(suffixed, "There is also a white cat extra")
            | KEEP id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            assertValues(resp.values(), List.of(List.of(5)));
        }
    }

    public void testSimpleWhereRuntimeMatchPhraseWithScore() {
        assumeRuntimeMatchPhraseEnabled();
        // Runtime match_phrase does not contribute to the score, so matching rows keep a 0.0 score.
        var query = """
            FROM test METADATA _score
            | WHERE match_phrase(to_text(concat(content, " extra")), "brown fox")
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 0.0), List.of(6, 0.0)));
        }
    }

    public void testMatchPhraseRuntimeEvalWithOptionsThrowsError() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            FROM test
            | EVAL new_content = to_text(concat(content, " extra"))
            | WHERE match_phrase(new_content, "brown fox", {"slop": 5})
            | KEEP new_content
            """;
        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("Options are not supported for [MATCH_PHRASE] function call on non-index-mapped field [new_content]")
        );
    }

    public void testMatchPhraseRuntimeRowWithOptionsThrowsError() {
        assumeRuntimeMatchPhraseEnabled();
        var query = """
            ROW content = to_text("a brown fox")
            | WHERE match_phrase(content, "brown fox", {"analyzer": "standard"})
            """;
        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("Options are not supported for [MATCH_PHRASE] function call on non-index-mapped field [content]")
        );
    }

    public void testMatchPhraseWithLookupJoin() {
        var query = """
            FROM test
            | LOOKUP JOIN test_lookup ON id
            | WHERE id > 0 AND MATCH_PHRASE(lookup_content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString(
                "line 3:33: [MatchPhrase] function cannot operate on [lookup_content], supplied by an index [test_lookup] "
                    + "in non-STANDARD mode [lookup]"
            )
        );
    }
}

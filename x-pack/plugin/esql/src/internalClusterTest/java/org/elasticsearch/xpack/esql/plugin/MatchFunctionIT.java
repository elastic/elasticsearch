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
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.CoreMatchers.containsString;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class MatchFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex(this::ensureYellow);
    }

    public void testSimpleWhereMatch() {
        var query = """
            FROM test
            | WHERE match(content, "fox")
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

    public void testWhereMatchOverWrittenColumn() {
        var query = """
            FROM test
            | DROP content
            | EVAL content = to_text(CONCAT("document with ID ", to_str(id)))
            | WHERE match(content, "document")
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

    public void testMatchWithStats() {
        var errorQuery = """
            FROM test
            | STATS c = count(*) BY match(content, "fox")
            """;

        var error = expectThrows(ElasticsearchException.class, () -> run(errorQuery));
        assertThat(error.getMessage(), containsString("[MATCH] function is only supported in WHERE and STATS commands"));

        var query = """
            FROM test
            | STATS c = count(*) WHERE match(content, "fox"), d = count(*) WHERE match(content, "dog")
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("c", "d"));
            assertColumnTypes(resp.columns(), List.of("long", "long"));
            assertValues(resp.values(), List.of(List.of(2L, 4L)));
        }

        query = """
            FROM test METADATA _score
            | WHERE match(content, "fox")
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

    public void testSimpleWhereRuntimeMatchWithScore() {
        var query = """
            FROM test METADATA _score
            | WHERE match(to_text(concat(content, " extra")), "fox")
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "double"));
            assertValues(resp.values(), List.of(List.of(1, 0.0), List.of(6, 0.0)));
        }
    }

    public void testMatchWithinEval() {
        var query = """
            FROM test
            | EVAL matches_query = match(content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function is only supported in WHERE and STATS commands"));
    }

    public void testMatchAfterMvExpand() {
        var query = """
            FROM test
            | MV_EXPAND content
            | EVAL content = to_text(content)
            | WHERE match(content, "fox")
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

    public void testMatchWithLookupJoin() {
        var query = """
            FROM test
            | LOOKUP JOIN test_lookup ON id
            | WHERE id > 0 AND MATCH(lookup_content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString(
                "line 3:26: [MATCH] function cannot operate on [lookup_content], supplied by an index [test_lookup] "
                    + "in non-STANDARD mode [lookup]"
            )
        );
    }

    public void testMatchOnJoinFieldWithLookupJoin() {
        var query = """
            FROM test
            | EVAL x = 123
            | RENAME x AS id
            | LOOKUP JOIN test_lookup ON id
            | WHERE id > 0 AND MATCH(id, "123")
            | KEEP id, content
            | SORT content
            | LIMIT 2
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "text"));
            assertValues(
                resp.values(),
                List.of(
                    List.of(123, "The dog is brown but this document is very very long"),
                    List.of(123, "The quick brown fox jumps over the lazy dog")
                )
            );
        }
    }

    public void testWhereFalseBeforeInlineStatsWithMatch() {
        var query = """
            FROM test
            | WHERE false
            | INLINE STATS max_id = MAX(id)
            | WHERE match(content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function cannot be used after INLINE"));
    }

    public void testImpossibleFilterBeforeInlineStatsWithMatch() {
        var query = """
            FROM test
            | EVAL a = 1, b = a + 1, c = b + a
            | WHERE c > 10
            | INLINE STATS max_id = MAX(id)
            | WHERE match(content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function cannot be used after INLINE"));
    }

    public void testWhereFalseBeforeInlineStatsWithMatchAndStats() {
        var query = """
            FROM test
            | WHERE false
            | INLINE STATS max_id = MAX(id)
            | WHERE match(content, "fox")
            | STATS c = COUNT(*)
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function cannot be used after INLINE"));
    }

    public void testWhereFalseBeforeGroupedInlineStatsWithMatch() {
        var query = """
            FROM test
            | WHERE false
            | INLINE STATS max_id = MAX(id) BY id
            | WHERE match(content, "fox")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(error.getMessage(), containsString("[MATCH] function cannot be used after INLINE"));
    }

    public void testMatchWithLookupJoinOnMatch() {
        var query = """
            FROM test
            | rename id as id_left
            | LOOKUP JOIN test_lookup ON id_left == id and MATCH(lookup_content, "fox")
            | WHERE id > 0
            | SORT id, id_left, content, lookup_content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content", "id_left", "id", "lookup_content"));
            assertColumnTypes(resp.columns(), List.of("text", "integer", "integer", "text"));
            // Should return rows where lookup_content matches "fox" (ids 1 and 6)
            assertValues(
                resp.values(),
                List.of(
                    List.of("This is a brown fox", 1, 1, "This is a brown fox"),
                    List.of("The quick brown fox jumps over the lazy dog", 6, 6, "The quick brown fox jumps over the lazy dog")
                )
            );
        }
    }

    /**
     * Regression for when {@code LOOKUP JOIN} looks like:
     * {@snippet lang="esql" :
     *   | LOOKUP JOIN kw_lookup ON <something> AND MATCH(rhs_field, "fox")
     * }
     * <p>
     *     This has to do with a "bulk" optimization in the lookup join.
     * </p>
     */
    public void testMatchInLookupJoinWithKeywordJoinField() {
        assumeTrue(
            "requires LOOKUP JOIN with full-text function support",
            EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled()
        );

        var client = client().admin().indices();

        // Lookup index: keyword join field + text field for MATCH
        assertAcked(
            client.prepareCreate("kw_lookup")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
                .setMapping("tag", "type=keyword", "description", "type=text")
        );
        client().prepareBulk()
            .add(new IndexRequest("kw_lookup").source("tag", "fox_tag", "description", "This is a brown fox"))
            .add(new IndexRequest("kw_lookup").source("tag", "dog_tag", "description", "This is a brown dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Main index: keyword field matching the lookup join field
        assertAcked(
            client.prepareCreate("kw_main")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("left_tag", "type=keyword", "value", "type=integer")
        );
        client().prepareBulk()
            .add(new IndexRequest("kw_main").source("left_tag", "fox_tag", "value", 1))
            .add(new IndexRequest("kw_main").source("left_tag", "dog_tag", "value", 2))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ensureYellow("kw_main", "kw_lookup");

        // keyword join triggers LuceneBulkLookup; after that, PushFiltersToSource cannot push
        // MATCH to Lucene (plan pattern mismatch), so MATCH stays in FilterExec.
        // Before the fix this caused: IndexOutOfBoundsException: no shards on LuceneQueryEvaluator
        var query = """
            FROM kw_main
            | LOOKUP JOIN kw_lookup ON left_tag == tag AND MATCH(description, "fox")
            | WHERE description IS NOT NULL
            | KEEP left_tag, value, description
            | SORT left_tag
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("left_tag", "value", "description"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            // Only fox_tag's description matches "fox"; dog_tag has null description and is filtered out
            assertValues(resp.values(), List.of(List.of("fox_tag", 1, "This is a brown fox")));
        }
    }

    public void testMatchRuntimeEvalWithOptionsThrowsError() {
        var query = """
            FROM test
            | EVAL new_content = to_text(concat(content, " extra"))
            | WHERE match(new_content, "fox", {"analyzer": "standard"})
            | KEEP new_content
            """;
        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("Options are not supported for [MATCH] function call on non-index-mapped field [new_content]")
        );
    }

    public void testMatchRuntimeRowWithOptionsThrowsError() {
        var query = """
            ROW content = to_text("This is a brown fox")
            | WHERE match(content, "fox AND brown", {"operator": "AND"})
            """;
        var error = expectThrows(VerificationException.class, () -> run(query));
        assertThat(
            error.getMessage(),
            containsString("Options are not supported for [MATCH] function call on non-index-mapped field [content]")
        );
    }

    public void testMatchRuntimeEvalWithIncompatibleLongValueThrowsError() {
        var query = """
            FROM test
            | EVAL new_id = to_long(id)
            | WHERE match(new_id, "not_a_number")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 3:23: [MATCH] query value [\"not_a_number\"] does not match the type ([long]) of non-index-mapped field [new_id]",
            error.getMessage()
        );
    }

    public void testMatchRuntimeRowWithIncompatibleIpValueThrowsError() {
        var query = """
            ROW my_ip = to_ip("192.168.1.1")
            | WHERE match(my_ip, "not_an_ip")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 2:22: [MATCH] query value [\"not_an_ip\"] does not match the type ([ip]) of non-index-mapped field [my_ip]",
            error.getMessage()
        );
    }

    public void testMatchRuntimeEvalWithIncompatibleIntegerValueThrowsError() {
        var query = """
            FROM test
            | EVAL new_id = to_integer(id)
            | WHERE match(new_id, "not_a_number")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 3:23: [MATCH] query value [\"not_a_number\"] does not match the type ([integer]) of non-index-mapped field "
                + "[new_id]",
            error.getMessage()
        );
    }

    public void testMatchRuntimeEvalWithIncompatibleDoubleValueThrowsError() {
        var query = """
            FROM test
            | EVAL new_id = to_double(id)
            | WHERE match(new_id, "not_a_number")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 3:23: [MATCH] query value [\"not_a_number\"] does not match the type ([double]) of non-index-mapped field [new_id]",
            error.getMessage()
        );
    }

    public void testMatchRuntimeEvalWithIncompatibleUnsignedLongValueThrowsError() {
        var query = """
            FROM test
            | EVAL new_id = to_unsigned_long(id)
            | WHERE match(new_id, "not_a_number")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 3:23: [MATCH] query value [\"not_a_number\"] does not match the type ([unsigned_long]) of non-index-mapped field "
                + "[new_id]",
            error.getMessage()
        );
    }

    public void testMatchRuntimeRowWithIncompatibleDatetimeValueThrowsError() {
        var query = """
            ROW my_date = to_datetime("2024-01-01")
            | WHERE match(my_date, "not_a_date")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 2:24: [MATCH] query value [\"not_a_date\"] does not match the type ([datetime]) of non-index-mapped field "
                + "[my_date]",
            error.getMessage()
        );
    }

    public void testMatchRuntimeRowWithIncompatibleDateNanosValueThrowsError() {
        var query = """
            ROW my_date = to_date_nanos("2024-01-01")
            | WHERE match(my_date, "not_a_date")
            """;

        var error = expectThrows(VerificationException.class, () -> run(query));
        assertEquals(
            "Found 1 problem\n"
                + "line 2:24: [MATCH] query value [\"not_a_date\"] does not match the type ([date_nanos]) of non-index-mapped field "
                + "[my_date]",
            error.getMessage()
        );
    }

    static void createAndPopulateIndex(Consumer<String[]> ensureYellow) {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(createRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        var lookupIndexName = "test_lookup";
        createAndPopulateLookupIndex(client, lookupIndexName);

        ensureYellow.accept(new String[] { indexName, lookupIndexName });
    }

    static void createAndPopulateLookupIndex(IndicesAdminClient client, String lookupIndexName) {
        var createRequest = client.prepareCreate(lookupIndexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
            .setMapping("id", "type=integer", "lookup_content", "type=text");
        assertAcked(createRequest);

        // Populate the lookup index with test data
        client().prepareBulk()
            .add(new IndexRequest(lookupIndexName).id("1").source("id", 1, "lookup_content", "This is a brown fox"))
            .add(new IndexRequest(lookupIndexName).id("2").source("id", 2, "lookup_content", "This is a brown dog"))
            .add(new IndexRequest(lookupIndexName).id("3").source("id", 3, "lookup_content", "This dog is really brown"))
            .add(
                new IndexRequest(lookupIndexName).id("4")
                    .source("id", 4, "lookup_content", "The dog is brown but this document is very very long")
            )
            .add(new IndexRequest(lookupIndexName).id("5").source("id", 5, "lookup_content", "There is also a white cat"))
            .add(new IndexRequest(lookupIndexName).id("6").source("id", 6, "lookup_content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }
}

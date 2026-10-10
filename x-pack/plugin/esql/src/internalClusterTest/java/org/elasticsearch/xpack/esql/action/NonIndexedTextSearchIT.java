/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * In a strictly columnar index a {@code text} field that indexes no terms answers a search like one that does,
 * reading its values instead of an index. Every test asks the same question of both mappings and expects the same
 * answer; {@link #testScoreCountsMatchedTerms} covers the one thing that differs, the score.
 */
public class NonIndexedTextSearchIT extends AbstractEsqlIntegTestCase {

    private static final String INDEXED = "with_index";
    private static final String NOT_INDEXED = "without_index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    /** The field type the indices are built with; {@code match_only_text} is the same family and takes the same path. */
    private String fieldType = "text";
    /** Whether the mapping asks for doc values, which a strictly columnar index gives every field anyway. */
    private boolean docValues = false;

    private void createIndices(String analyzer) {
        createIndices(analyzer, true);
    }

    /**
     * @param columnar whether the indices are strictly columnar, which is what keeps every field's values in a
     *                 column for a row-by-row search to read
     */
    private void createIndices(String analyzer, boolean columnar) {
        for (String index : List.of(INDEXED, NOT_INDEXED)) {
            final String body = "{\"type\":\""
                + fieldType
                + "\",\"index\":"
                + index.equals(INDEXED)
                + (analyzer == null ? "" : ",\"analyzer\":\"" + analyzer + "\"")
                + "}";
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(
                        columnar
                            ? Settings.builder()
                                .put("index.number_of_shards", 1)
                                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                            : Settings.builder().put("index.number_of_shards", 1)
                    )
                    .setMapping("{\"properties\":{\"body\":" + body + ",\"tag\":{\"type\":\"keyword\"}}}")
            );
        }
        for (int i = 0; i < 20; i++) {
            for (String index : List.of(INDEXED, NOT_INDEXED)) {
                indexDoc(index, "the quick brown foxes jumped " + i, i < 5 ? "a" : "b");
            }
        }
        client().admin().indices().prepareRefresh(INDEXED, NOT_INDEXED).get();
    }

    private void createIndex(String index, String analyzer, boolean standardMode, boolean indexed) {
        final String body = "{\"type\":\""
            + fieldType
            + "\",\"index\":"
            + indexed
            + (docValues ? ",\"doc_values\":true" : "")
            + (analyzer == null ? "" : ",\"analyzer\":\"" + analyzer + "\"")
            + "}";
        final Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1);
        if (standardMode == false) {
            settings.put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName());
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping("{\"properties\":{\"body\":" + body + ",\"tag\":{\"type\":\"keyword\"}}}")
        );
    }

    private void indexDoc(String index, String body, String tag) {
        // Through a bulk request, which is how a columnar index writes its columns in a batch.
        client().prepareBulk()
            .add(client().prepareIndex(index).setSource("{\"body\":\"" + body + "\",\"tag\":\"" + tag + "\"}", XContentType.JSON))
            .get();
    }

    /** What the indexed field answers is what the field without an index has to answer. */
    private void assertSameAsIndexed(String tail) {
        final List<List<Object>> indexed = rowsOf("FROM " + INDEXED + " | " + tail, null);
        final List<List<Object>> notIndexed = rowsOf("FROM " + NOT_INDEXED + " | " + tail, null);
        assertThat(tail, notIndexed, equalTo(indexed));
    }

    private List<List<Object>> rowsOf(String query, long[] documentsFound) {
        try (var response = run(syncEsqlQueryRequest(query).profile(true), DEFAULT_REQUEST_TIMEOUT)) {
            if (documentsFound != null) {
                documentsFound[0] = response.documentsFound();
            }
            return getValuesList(response);
        }
    }

    public void testMatchAndPhraseAnswerAsIndexed() {
        createIndices(null);
        assertSameAsIndexed("WHERE match(body, \"quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"quick nothing\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"quick brown\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"brown quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"quick\") | SORT body | KEEP body | LIMIT 3");
        // LIKE and RLIKE reach the field as a wildcard and a regexp, which read the same tokens a match does
        assertSameAsIndexed("WHERE body LIKE \"qu*ck\" | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE body RLIKE \"qu.*k\" | STATS c = COUNT(*)");
    }

    /** The whole condition is a query, so the search is answered without loading the documents it reads. */
    public void testTheWholeConditionIsPushed() {
        createIndices(null);
        final long[] documentsFound = new long[1];
        final List<List<Object>> rows = rowsOf(
            "FROM " + NOT_INDEXED + " | WHERE tag == \"a\" AND match(body, \"quick\") | STATS c = COUNT(*)",
            documentsFound
        );
        assertThat(rows, equalTo(List.of(List.of(5L))));
        assertThat("nothing was loaded to answer it", documentsFound[0], lessThanOrEqualTo(1L));
    }

    /** Any of a document's values can match, and a document holding none matches nothing. */
    public void testMultipleValuesAndMissingValues() {
        createIndices(null);
        for (String index : List.of(INDEXED, NOT_INDEXED)) {
            client().prepareIndex(index)
                .setSource("{\"body\":[\"nothing here\",\"a quick remark\"],\"tag\":\"c\"}", XContentType.JSON)
                .get();
            client().prepareIndex(index).setSource("{\"tag\":\"c\"}", XContentType.JSON).get();
        }
        client().admin().indices().prepareRefresh(INDEXED, NOT_INDEXED).get();
        assertSameAsIndexed("WHERE match(body, \"remark\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"quick remark\") | STATS c = COUNT(*)");
        // a phrase may not span two values
        assertSameAsIndexed("WHERE match_phrase(body, \"here a\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE tag == \"c\" AND match(body, \"nothing\") | STATS c = COUNT(*)");
    }

    /** With a disjunction nothing is pushed, so the whole condition is answered row by row. */
    public void testDisjunction() {
        createIndices(null);
        assertSameAsIndexed("WHERE tag == \"a\" OR match(body, \"quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"nothing\") OR match_phrase(body, \"quick brown\") | STATS c = COUNT(*)");
    }

    /** Options are answered by a query rather than the token matchers, and answer the same. */
    public void testOptions() {
        createIndices(null);
        assertSameAsIndexed("WHERE match(body, \"quick missing\", {\"operator\": \"AND\"}) | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"quikc\", {\"fuzziness\": \"1\"}) | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"quick foxes\", {\"slop\": 1}) | STATS c = COUNT(*)");
    }

    /** An {@code analyzer} option reads the query string; the field's own analyzer still reads its values. */
    public void testAnalyzerOptionReadsTheQueryOnly() {
        createIndices(null);
        // read whole, the query matches no single token of a value
        assertSameAsIndexed("WHERE match(body, \"quick brown\", {\"analyzer\": \"keyword\"}) | STATS c = COUNT(*)");
        // read as words, it matches again
        assertSameAsIndexed("WHERE match(body, \"quick brown\", {\"analyzer\": \"standard\"}) | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"quick brown\", {\"analyzer\": \"standard\"}) | STATS c = COUNT(*)");
        assertThat(
            "a query read whole matches no value token",
            rowsOf("FROM " + NOT_INDEXED + " | WHERE match(body, \"quick brown\", {\"analyzer\": \"keyword\"}) | STATS c = COUNT(*)", null),
            equalTo(List.of(List.of(0L)))
        );
    }

    /** A phrase under an analyzer that leaves gaps is answered by a query rather than the token matcher. */
    public void testPhraseWithAnalyzerLeavingGaps() {
        createIndices("stop");
        assertSameAsIndexed("WHERE match_phrase(body, \"quick brown\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"the quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"the\") | STATS c = COUNT(*)");
    }

    /**
     * The score is the one answer that differs: a search over values scores each query term it matches, the boolean
     * similarity a runtime search uses, where an index scores BM25 from statistics a column does not hold.
     */
    public void testScoreCountsMatchedTerms() {
        createIndices(null);
        assertThat(scoreOf(NOT_INDEXED, "match(body, \"quick\")"), equalTo(1.0));
        assertThat(scoreOf(NOT_INDEXED, "match(body, \"quick brown\")"), equalTo(2.0));
        assertThat(scoreOf(NOT_INDEXED, "match(body, \"quick nothing\")"), equalTo(1.0));
        assertThat(scoreOf(NOT_INDEXED, "match_phrase(body, \"quick brown\")"), equalTo(1.0));

        final double indexed = scoreOf(INDEXED, "match(body, \"quick brown\")");
        assertThat(indexed, greaterThan(0.0));
        assertThat("BM25, not a term count", indexed, not(equalTo(2.0)));
    }

    private double scoreOf(String index, String condition) {
        final List<List<Object>> rows = rowsOf(
            "FROM " + index + " METADATA _score | WHERE " + condition + " | KEEP _score | LIMIT 1",
            null
        );
        assertThat(condition, rows, hasSize(1));
        return (Double) rows.get(0).get(0);
    }

    /**
     * One query over both mappings at once: the field's analyzer reads its values where it has no terms and its
     * index where it has them, so every document is asked the same question.
     */
    public void testIndexedAndNotIndexedTogether() {
        createIndices("keyword");
        final String both = INDEXED + "," + NOT_INDEXED;
        // the keyword analyzer keeps a value whole on both sides, so one word of it matches nothing anywhere
        assertThat(rowsOf("FROM " + both + " | WHERE match(body, \"quick\") | STATS c = COUNT(*)", null), equalTo(List.of(List.of(0L))));
        assertThat(
            "every document of both indices",
            rowsOf("FROM " + both + " | WHERE match(body, \"the quick brown foxes jumped 3\") | STATS c = COUNT(*)", null),
            equalTo(List.of(List.of(2L)))
        );
        // each index scores its own way: the one with terms by BM25, the one without by counting matched terms
        final List<List<Object>> scores = rowsOf(
            "FROM " + both + " METADATA _score | WHERE match(body, \"the quick brown foxes jumped 3\") | KEEP _score | SORT _score",
            null
        );
        assertThat(scores, hasSize(2));
        assertThat("the two indices score the same document differently", scores.get(0), not(equalTo(scores.get(1))));
    }

    /** Each index reads its own values with its own analyzer, so two mappings that differ answer differently. */
    public void testIndicesDisagreeingOnTheAnalyzer() {
        createIndex(INDEXED, "keyword", false, false);
        createIndex(NOT_INDEXED, "standard", false, false);
        indexDoc(INDEXED, "the quick brown fox", "a");
        indexDoc(NOT_INDEXED, "the quick brown fox", "a");
        client().admin().indices().prepareRefresh(INDEXED, NOT_INDEXED).get();

        assertThat(
            "only the index whose analyzer splits the value into words matches one of them",
            rowsOf("FROM " + INDEXED + "," + NOT_INDEXED + " | WHERE match(body, \"quick\") | STATS c = COUNT(*)", null),
            equalTo(List.of(List.of(1L)))
        );
    }

    /** The DSL answers the same field the same way, which is where this capability lives. */
    /** {@code match_only_text} is the same family and takes the same path, term family included. */
    public void testMatchOnlyText() {
        fieldType = "match_only_text";
        createIndices(null);
        assertSameAsIndexed("WHERE body LIKE \"qu*ck\" | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE body RLIKE \"qu.*k\" | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"quick nothing\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"quick brown\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match_phrase(body, \"brown quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE tag == \"a\" AND match(body, \"quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"quick\") | SORT body | KEEP body | LIMIT 3");
    }

    /**
     * Only the strictly columnar modes are taken to keep every field's values in a column, so an index outside them
     * answers as it did before - even one whose field does keep doc values.
     */
    public void testOutsideColumnarNothingChanges() {
        docValues = true;
        createIndices(null, false);
        assertThat(
            rowsOf("FROM " + NOT_INDEXED + " | WHERE match(body, \"quick\") | STATS c = COUNT(*)", null),
            equalTo(List.of(List.of(0L)))
        );
    }

    /**
     * The field's own analyzer reads its values: under the {@code keyword} analyzer a value stays whole, so one word
     * of it matches nothing - as it would not, had the standard analyzer been used instead.
     */
    public void testMappedAnalyzerIsUsed() {
        createIndices("keyword");
        assertSameAsIndexed("WHERE match(body, \"quick\") | STATS c = COUNT(*)");
        assertSameAsIndexed("WHERE match(body, \"the quick brown foxes jumped 3\") | STATS c = COUNT(*)");
        assertThat(
            "one word of a value the keyword analyzer keeps whole",
            rowsOf("FROM " + NOT_INDEXED + " | WHERE match(body, \"quick\") | STATS c = COUNT(*)", null),
            equalTo(List.of(List.of(0L)))
        );
        assertThat(
            "the whole value",
            rowsOf("FROM " + NOT_INDEXED + " | WHERE match(body, \"the quick brown foxes jumped 3\") | STATS c = COUNT(*)", null),
            equalTo(List.of(List.of(1L)))
        );
    }
}

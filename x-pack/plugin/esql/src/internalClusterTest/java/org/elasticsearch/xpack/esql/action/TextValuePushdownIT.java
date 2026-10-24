/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * A predicate over a {@code text} field's value is answered from the values the field keeps, in Lucene, rather than by
 * loading every row into the compute engine. Each predicate here is asked of two indices holding the same documents:
 * one whose field keeps its values, where the predicate is pushed, and one whose field keeps none, where it is not.
 * The two have to answer the same.
 */
public class TextValuePushdownIT extends AbstractEsqlIntegTestCase {

    private static final String PUSHED = "keeps_values";
    private static final String NOT_PUSHED = "keeps_none";
    private static final String EXACT_SUBFIELD = "keeps_a_subfield";

    private static final List<String> DOCS = List.of("the quick brown fox", "quick", "jumps over the lazy dog", "The Quick Brown Fox", "");

    @Before
    public void setUpIndices() {
        // A columnar index keeps every field's values; a standard one keeps none for a text field.
        create(PUSHED, Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()));
        create(NOT_PUSHED, Settings.builder());
        createWithSubfield(EXACT_SUBFIELD, Settings.builder());
        for (String index : List.of(PUSHED, NOT_PUSHED, EXACT_SUBFIELD)) {
            for (int i = 0; i < DOCS.size(); i++) {
                client().prepareIndex(index)
                    .setId(Integer.toString(i))
                    .setSource("{\"body\":\"" + DOCS.get(i) + "\",\"id\":" + i + "}", XContentType.JSON)
                    .get();
            }
            // A document holding no value at all, and one holding several.
            client().prepareIndex(index).setId("absent").setSource("{\"id\":99}", XContentType.JSON).get();
            client().prepareIndex(index).setId("several").setSource("{\"body\":[\"quick\",\"brown\"],\"id\":98}", XContentType.JSON).get();
        }
        client().admin().indices().prepareRefresh(PUSHED, NOT_PUSHED, EXACT_SUBFIELD).get();
    }

    private void createWithSubfield(String index, Settings.Builder settings) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings.put("index.number_of_shards", 1))
                .setMapping(
                    "{\"properties\":{\"body\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
                        + "\"id\":{\"type\":\"long\"}}}"
                )
        );
    }

    private void create(String index, Settings.Builder settings) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings.put("index.number_of_shards", 1))
                .setMapping("{\"properties\":{\"body\":{\"type\":\"text\"},\"id\":{\"type\":\"long\"}}}")
        );
    }

    public void testPatternsAnswerTheSame() {
        assertSame("WHERE body LIKE \"the quick*\"");
        assertSame("WHERE body LIKE \"*quick*\"");
        assertSame("WHERE body LIKE \"quick\"");
        assertSame("WHERE body RLIKE \"the quick.*\"");
        assertSame("WHERE starts_with(body, \"the\")");
        assertSame("WHERE ends_with(body, \"fox\")");
        assertSame("WHERE contains(body, \"brown\")");
    }

    public void testEqualityAndRangesAnswerTheSame() {
        assertSame("WHERE body == \"quick\"");
        assertSame("WHERE body != \"quick\"");
        assertSame("WHERE body IN (\"quick\", \"the quick brown fox\")");
        assertSame("WHERE body > \"q\"");
        assertSame("WHERE body <= \"quick\"");
    }

    /** The case of the pattern is the case of the value, which is not what the field's analyzer would fold. */
    public void testTheValueKeepsItsCase() {
        assertSame("WHERE body LIKE \"The Quick*\"");
        assertSame("WHERE body == \"The Quick Brown Fox\"");
    }

    /**
     * In a strictly columnar index the predicate is answered in Lucene, so the source emits only the documents that
     * answer it. Without the values it emits every document and the compute engine does the answering, which is the
     * regression this guards: a predicate that stops being pushed emits more rows than it answers.
     */
    public void testTheColumnarIndexAnswersInLucene() {
        for (String tail : List.of(
            "WHERE body LIKE \"the quick*\"",
            "WHERE body RLIKE \"the quick.*\"",
            "WHERE body == \"quick\"",
            "WHERE body IN (\"quick\", \"the quick brown fox\")",
            "WHERE body > \"q\"",
            "WHERE starts_with(body, \"the\")"
        )) {
            final long answered = rowsOf(PUSHED, tail);
            assertThat(tail + ": emitted only what it answered", rowsEmitted(PUSHED, tail), equalTo(answered));
            assertThat(tail + ": the other index emitted more", rowsEmitted(NOT_PUSHED, tail), greaterThan(answered));
        }
    }

    /**
     * Outside the columnar modes a text field keeps no values, so nothing here is answered from them: a field with an
     * exact sub-field keeps being pushed to that sub-field, and one without keeps being answered by the compute
     * engine. Equality names the sub-field; a pattern does not, because a sub-field's {@code ignore_above} can hold
     * no term for a long value and a pattern cannot be checked against it the way a value can.
     */
    public void testOutsideTheColumnarModesNothingChanges() {
        final String equality = "WHERE body == \"quick\"";
        assertThat("equality is pushed to the sub-field", rowsEmitted(EXACT_SUBFIELD, equality), equalTo(rowsOf(EXACT_SUBFIELD, equality)));

        final String pattern = "WHERE body LIKE \"the quick*\"";
        assertThat(
            "a pattern is answered by the compute engine",
            rowsEmitted(EXACT_SUBFIELD, pattern),
            greaterThan(rowsOf(EXACT_SUBFIELD, pattern))
        );

        // And whichever way it is answered, it is answered the same.
        for (String tail : List.of(equality, pattern, "WHERE body RLIKE \"the quick.*\"", "WHERE starts_with(body, \"the\")")) {
            assertThat(tail, rowsOf(EXACT_SUBFIELD, tail), equalTo(rowsOf(PUSHED, tail)));
        }
    }

    private long rowsOf(String index, String tail) {
        try (var response = run(syncEsqlQueryRequest("FROM " + index + " | " + tail + " | KEEP id"))) {
            return getValuesList(response).size();
        }
    }

    /** The rows the Lucene source handed to the compute engine, which a pushed predicate has already narrowed. */
    private long rowsEmitted(String index, String tail) {
        try (var response = run(syncEsqlQueryRequest("FROM " + index + " | " + tail + " | KEEP id").profile(true))) {
            assertThat(response.profile(), notNullValue());
            long rows = 0;
            for (var driver : response.profile().drivers()) {
                for (var operator : driver.operators()) {
                    if (operator.status() instanceof LuceneOperator.Status lucene) {
                        rows += lucene.rowsEmitted();
                    }
                }
            }
            return rows;
        }
    }

    private void assertSame(String tail) {
        final String query = " | " + tail + " | KEEP id | SORT id";
        try (
            var pushed = run(syncEsqlQueryRequest("FROM " + PUSHED + query));
            var notPushed = run(syncEsqlQueryRequest("FROM " + NOT_PUSHED + query))
        ) {
            assertThat(tail, getValuesList(pushed), equalTo(getValuesList(notPushed)));
        }
    }
}

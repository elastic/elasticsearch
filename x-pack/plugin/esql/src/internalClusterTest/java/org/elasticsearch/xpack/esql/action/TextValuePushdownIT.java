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
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * A predicate over a {@code text} field's value is answered from the values the field keeps, in Lucene, rather than by
 * loading every row into the compute engine. Each predicate here is asked of two indices holding the same documents:
 * one whose field keeps its values, where the predicate is pushed, and one whose field keeps none, where it is not.
 * The two have to answer the same.
 */
public class TextValuePushdownIT extends AbstractEsqlIntegTestCase {

    private static final String PUSHED = "keeps_values";
    private static final String NOT_PUSHED = "keeps_none";

    private static final List<String> DOCS = List.of("the quick brown fox", "quick", "jumps over the lazy dog", "The Quick Brown Fox", "");

    @Before
    public void setUpIndices() {
        // A columnar index keeps every field's values; a standard one keeps none for a text field.
        create(PUSHED, Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()));
        create(NOT_PUSHED, Settings.builder());
        for (String index : List.of(PUSHED, NOT_PUSHED)) {
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
        client().admin().indices().prepareRefresh(PUSHED, NOT_PUSHED).get();
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class IndexSortIT extends ESIntegTestCase {
    private static final XContentBuilder TEST_MAPPING = createTestMapping();

    private static XContentBuilder createTestMapping() {
        try {
            return jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("date")
                        .field("type", "date")
                    .endObject()
                    .startObject("numeric")
                        .field("type", "integer")
                    .field("doc_values", false)
                    .endObject()
                    .startObject("numeric_dv")
                        .field("type", "integer")
                        .field("doc_values", true)
                    .endObject()
                    .startObject("keyword_dv")
                        .field("type", "keyword")
                        .field("doc_values", true)
                    .endObject()
                    .startObject("keyword")
                        .field("type", "keyword")
                        .field("doc_values", false)
                    .endObject()
                .endObject().endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void testIndexSort() {
        SortField dateSort = new SortedNumericSortField("date", SortField.Type.LONG, false);
        dateSort.setMissingValue(Long.MAX_VALUE);
        SortField numericSort = new SortedNumericSortField("numeric_dv", SortField.Type.LONG, false);
        numericSort.setMissingValue(Long.MAX_VALUE);
        SortField keywordSort = new SortedSetSortField("keyword_dv", false);
        keywordSort.setMissingValue(SortField.STRING_LAST);
        Sort indexSort = new Sort(dateSort, numericSort, keywordSort);
        prepareCreate("test")
            .setSettings(Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "1")
                .putList("index.sort.field", "date", "numeric_dv", "keyword_dv")
            )
            .setMapping(TEST_MAPPING)
            .get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("test").setId(Integer.toString(i))
                .setSource("numeric_dv", randomInt(), "keyword_dv", randomAlphaOfLengthBetween(10, 20))
                .get();
        }
        flushAndRefresh();
        ensureYellow();
        assertSortedSegments("test", indexSort);
    }

    public void testInvalidIndexSort() {
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> prepareCreate("test")
                .setSettings(Settings.builder()
                    .put(indexSettings())
                    .putList("index.sort.field", "invalid_field")
                )
                .setMapping(TEST_MAPPING)
                .get()
        );
        assertThat(exc.getMessage(), containsString("unknown index sort field:[invalid_field]"));

        exc = expectThrows(IllegalArgumentException.class,
            () -> prepareCreate("test")
                .setSettings(Settings.builder()
                    .put(indexSettings())
                    .putList("index.sort.field", "numeric")
                )
                .setMapping(TEST_MAPPING)
                .get()
        );
        assertThat(exc.getMessage(), containsString("docvalues not found for index sort field:[numeric]"));

        exc = expectThrows(IllegalArgumentException.class,
            () -> prepareCreate("test")
                .setSettings(Settings.builder()
                    .put(indexSettings())
                    .putList("index.sort.field", "keyword")
                )
                .setMapping(TEST_MAPPING)
                .get()
        );
        assertThat(exc.getMessage(), containsString("docvalues not found for index sort field:[keyword]"));
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

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

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class FetchFieldsPhaseTests extends ESSingleNodeTestCase {

    private IndexService indexService;

    @Before
    public void setupIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field").field("type", "keyword").endObject()
                .startObject("integer_field").field("type", "integer").endObject()
                .startObject("float_range").field("type", "float_range").endObject()
                .startObject("object")
                    .startObject("properties")
                        .startObject("field").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
                .startObject("field_that_does_not_match").field("type", "keyword").endObject()
            .endObject()
        .endObject();
        indexService = createIndex("index", Settings.EMPTY, mapping);
    }

    public void testLeafValues() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .startObject("object")
                .field("field", "third")
            .endObject()
        .endObject();

        SearchHit hit = fetchFields(indexService, source, List.of("field", "object.field"));
        assertThat(hit.getFields().size(), equalTo(2));

        DocumentField field = hit.getFields().get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField objectField = hit.getFields().get("object.field");
        assertNotNull(objectField);
        assertThat(objectField.getValues().size(), equalTo(1));
        assertThat(objectField.getValues(), hasItems("third"));
    }

    public void testObjectValues() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .startObject("float_range")
                .field("gte", 0.0)
                .field("lte", 2.718)
            .endObject()
        .endObject();

        SearchHit hit = fetchFields(indexService, source, "float_range");
        assertThat(hit.getFields().size(), equalTo(1));

        DocumentField rangeField = hit.getFields().get("float_range");
        assertNotNull(rangeField);
        assertThat(rangeField.getValues().size(), equalTo(1));
        assertThat(rangeField.getValue(), equalTo(Map.of("gte", 0.0, "lte", 2.718)));
    }

    public void testFieldNamesWithWildcard() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .field("integer_field", "third")
            .startObject("object")
                .field("field", "fourth")
            .endObject()
        .endObject();

        SearchHit hit = fetchFields(indexService, source, "*field");
        assertThat(hit.getFields().size(), equalTo(3));

        DocumentField field = hit.getFields().get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField otherField = hit.getFields().get("integer_field");
        assertNotNull(otherField);
        assertThat(otherField.getValues().size(), equalTo(1));
        assertThat(otherField.getValues(), hasItems("third"));

        DocumentField objectField = hit.getFields().get("object.field");
        assertNotNull(objectField);
        assertThat(objectField.getValues().size(), equalTo(1));
        assertThat(objectField.getValues(), hasItems("fourth"));
    }

    public void testSourceDisabled() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_source").field("enabled", false).endObject()
            .startObject("properties")
                .startObject("field").field("type", "keyword").endObject()
            .endObject()
        .endObject();

        IndexService sourceDisabledIndexService = createIndex("disabled-index", Settings.EMPTY, mapping);
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .endObject();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> fetchFields(sourceDisabledIndexService, source, "field"));
        assertThat(e.getMessage(), containsString("Unable to retrieve the requested [fields] since _source is disabled in the mapping"));
    }

    public void testObjectFields() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .startObject("object")
                .field("field", "third")
            .endObject()
        .endObject();

        SearchHit hit = fetchFields(indexService, source, "object");
        assertFalse(hit.getFields().containsKey("object"));
    }

    private SearchHit fetchFields(IndexService indexService, XContentBuilder source, String field) {
        return fetchFields(indexService, source, List.of(field));
    }

    private SearchHit fetchFields(IndexService indexService, XContentBuilder source, List<String> fields) {
        FetchFieldsContext fetchFieldsContext = new FetchFieldsContext(fields);
        SearchContext searchContext = new FetchFieldsTestSearchContext(indexService, fetchFieldsContext);

        SourceLookup sourceLookup = new SourceLookup();
        sourceLookup.setSource(BytesReference.bytes(source));

        FetchFieldsPhase phase = new FetchFieldsPhase();
        SearchHit searchHit = new SearchHit(1, null, null, null, null);
        phase.hitsExecute(searchContext, hit -> sourceLookup, new SearchHit[]{ searchHit });
        return searchHit;
    }

    private static class FetchFieldsTestSearchContext extends TestSearchContext {
        private final FetchFieldsContext context;

        FetchFieldsTestSearchContext(IndexService indexService,
                                     FetchFieldsContext context) {
            super(indexService.getBigArrays(), indexService);
            this.context = context;
        }

        @Override
        public FetchFieldsContext fetchFieldsContext() {
            return context;
        }
    }
}

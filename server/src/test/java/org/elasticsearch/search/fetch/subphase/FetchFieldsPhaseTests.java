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
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
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

        FetchSubPhase.HitContext hitContext = hitExecute(indexService, source, List.of("field", "object.field"));
        assertThat(hitContext.hit().getFields().size(), equalTo(2));

        DocumentField field = hitContext.hit().getFields().get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField objectField = hitContext.hit().getFields().get("object.field");
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

        FetchSubPhase.HitContext hitContext = hitExecute(indexService, source, "float_range");
        assertThat(hitContext.hit().getFields().size(), equalTo(1));

        DocumentField rangeField = hitContext.hit().getFields().get("float_range");
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

        FetchSubPhase.HitContext hitContext = hitExecute(indexService, source, "*field");
        assertThat(hitContext.hit().getFields().size(), equalTo(3));

        DocumentField field = hitContext.hit().getFields().get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField otherField = hitContext.hit().getFields().get("integer_field");
        assertNotNull(otherField);
        assertThat(otherField.getValues().size(), equalTo(1));
        assertThat(otherField.getValues(), hasItems("third"));

        DocumentField objectField = hitContext.hit().getFields().get("object.field");
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
            () -> hitExecute(sourceDisabledIndexService, source, "field"));
        assertThat(e.getMessage(), containsString("Unable to retrieve the requested [fields] since _source is disabled in the mapping"));
    }

    public void testNonExistentFields() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject()
            .field("field", "value")
            .endObject();

        FetchSubPhase.HitContext hitContext = hitExecute(indexService, source, "non_existent");
        assertFalse(hitContext.hit().getFields().containsKey("non_existent"));
    }

    private FetchSubPhase.HitContext hitExecute(IndexService indexService, XContentBuilder source, String field) {
        return hitExecute(indexService, source, List.of(field));
    }

    private FetchSubPhase.HitContext hitExecute(IndexService indexService, XContentBuilder source, List<String> fields) {
        FetchFieldsContext fetchFieldsContext = new FetchFieldsContext(fields);
        BytesReference sourceAsBytes = source != null ? BytesReference.bytes(source) : null;
        SearchContext searchContext = new FetchFieldsPhaseTestSearchContext(indexService, fetchFieldsContext, sourceAsBytes);

        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
        SearchHit searchHit = new SearchHit(1, null, null, null, null);
        hitContext.reset(searchHit, null, 1, null);

        FetchFieldsPhase phase = new FetchFieldsPhase();
        phase.hitExecute(searchContext, hitContext);
        return hitContext;
    }

    private static class FetchFieldsPhaseTestSearchContext extends TestSearchContext {
        private final FetchFieldsContext context;
        private final BytesReference source;

        FetchFieldsPhaseTestSearchContext(IndexService indexService,
                                          FetchFieldsContext context,
                                          BytesReference source) {
            super(indexService.getBigArrays(), indexService);
            this.context = context;
            this.source = source;
        }

        @Override
        public FetchFieldsContext fetchFieldsContext() {
            return context;
        }

        @Override
        public SearchLookup lookup() {
            SearchLookup lookup = new SearchLookup(this.mapperService(), this::getForField);
            lookup.source().setSource(source);
            return lookup;
        }
    }
}

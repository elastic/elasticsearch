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

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class FetchFieldsPhaseTests extends ESSingleNodeTestCase {

    @Before
    public void createIndex() throws IOException {
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

        client().admin().indices().prepareCreate("index").setMapping(mapping).get();
    }

    public void testLeafValues() throws IOException {
        indexDocument(XContentFactory.jsonBuilder().startObject()
                .array("field", "first", "second")
                .startObject("object")
                    .field("field", "third")
                .endObject()
            .endObject());

        SearchHit hit = fetchFields("field", "object.field");
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
        indexDocument(XContentFactory.jsonBuilder().startObject()
                .startObject("float_range")
                    .field("gte", 0.0)
                    .field("lte", 2.718)
                .endObject()
            .endObject());

        SearchHit hit = fetchFields("float_range");
        assertThat(hit.getFields().size(), equalTo(1));

        DocumentField rangeField = hit.getFields().get("float_range");
        assertNotNull(rangeField);
        assertThat(rangeField.getValues().size(), equalTo(1));
        assertThat(rangeField.getValue(), equalTo(Map.of("gte", 0.0, "lte", 2.718)));
    }

    public void testFieldNamesWithWildcard() throws IOException {
        indexDocument(XContentFactory.jsonBuilder().startObject()
                .array("field", "first", "second")
                .field("integer_field", 42)
                .startObject("object")
                    .field("field", "fourth")
                .endObject()
            .endObject());

        SearchHit hit = fetchFields("*field");
        assertThat(hit.getFields().size(), equalTo(3));

        DocumentField field = hit.getFields().get("field");
        assertNotNull(field);
        assertThat(field.getValues().size(), equalTo(2));
        assertThat(field.getValues(), hasItems("first", "second"));

        DocumentField otherField = hit.getFields().get("integer_field");
        assertNotNull(otherField);
        assertThat(otherField.getValues().size(), equalTo(1));
        assertThat(otherField.getValues(), hasItems(42));

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

        client().admin().indices().prepareCreate("source-disabled")
            .setMapping(mapping)
            .get();

        SearchPhaseExecutionException e = expectThrows(SearchPhaseExecutionException.class, () ->
            client().prepareSearch("source-disabled").addFetchField("field").get());
        assertThat(e.getCause().getMessage(), containsString(
            "Unable to retrieve the requested [fields] since _source is disabled in the mapping"));
    }

    public void testObjectFields() throws IOException {
        indexDocument(XContentFactory.jsonBuilder().startObject()
            .array("field", "first", "second")
            .startObject("object")
                .field("field", "third")
            .endObject()
        .endObject());

        SearchHit hit = fetchFields("object");
        assertFalse(hit.getFields().containsKey("object"));
    }

    private void indexDocument(XContentBuilder source) {
        client().prepareIndex("index")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource(source)
            .get();
    }

    private SearchHit fetchFields(String... fields) {
        SearchRequestBuilder builder = client().prepareSearch("index");
        for (String field : fields) {
            builder.addFetchField(field);
        }
        SearchResponse response = builder.get();

        assertEquals(1, response.getHits().getHits().length);
        return response.getHits().getHits()[0];
    }
}

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

package org.elasticsearch.index.mapper.core;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class BaseTypeMapperTests extends ElasticsearchSingleNodeTest {

    public static final String INDEX = "text";
    public static final String TYPE = "type";
    public static final String[] BASE_TYPES = {"byte", "short", "integer", "long", "float", "double", "string"};

    @Test
    public void testNumberWithBoostAppearInAll() throws IOException {
        String type = getRandomBaseType();
        logger.info("TYPE is " + type);
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field")
                .field("type", type).field("omit_norms", "false")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);

        XContentBuilder doc = jsonBuilder().startObject()
                .startObject("field").field("value", "2").field("boost", 10).endObject()
                .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
        doc = jsonBuilder().startObject()
                .startObject("field").field("value", "2").field("boost", 5).endObject()
                .endObject();
        client().prepareIndex(INDEX, TYPE, "2").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        String termInAllField = "2" + ((type.equals("float") || type.equals("double")) ? ".0" : "");
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("_all", termInAllField)).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
    }

    @Test
    public void testMultiFieldsWithArray() throws IOException {

        String type = getRandomBaseType();
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject(TYPE)
                .startObject("properties")
                .startObject("field")
                .field("type", type)
                .startObject("fields")
                .startObject("same_base_type")
                .field("type", type)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);

        XContentBuilder doc = jsonBuilder().startObject()
                .field("field", "1", "2", "3")
                .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("field.same_base_type", "1")).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
    }

    // test that multi fields and boost works for number and String
    @Test
    public void testMultiFieldsWithArrayAndBoost() throws IOException {
        String type = getRandomBaseType();
        logger.info("TYPE is " + type);
        XContentBuilder mapping =
                XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("properties")
                        .startObject("field")
                        .field("type", type).field("omit_norms", "false")
                        .startObject("fields")
                        .startObject("string")
                        .field("omit_norms", "false")
                                // boost only has an effect for string types so to check if it is transported to the multi fields
                                // the multi field has to be a string
                        .field("type", "string")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);
        XContentBuilder doc = jsonBuilder().startObject()
                .startArray("field")
                .startObject().field("value", "1").field("boost", 1).endObject()
                .startObject().field("value", "2").field("boost", 3).endObject()
                .startObject().field("value", "3").field("boost", 1).endObject()
                .endArray()
                .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
        doc = jsonBuilder().startObject()
                .array("field", "1", "2", "3")
                .endObject();
        client().prepareIndex(INDEX, TYPE, "2").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        String termInField = "2" + ((type.equals("float") || type.equals("double")) ? ".0" : "");
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("field.string", termInField)).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).getScore(), equalTo(searchResponse.getHits().getAt(1).getScore() * 3.0f));
    }

    @Test
    public void testFieldBoostForMultiField() throws IOException {

        String type = getRandomBaseType();
        XContentBuilder mapping =
                XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("properties")
                        .startObject("field")
                        .field("type", type).field("omit_norms", "false")
                        .startObject("fields")
                        .startObject("string")
                        .field("omit_norms", "false")
                                // boost only has an effect for string types so to check if it is transported to the multi fields
                                // the multi field has to be a string
                        .field("type", "string")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);
        XContentBuilder doc = jsonBuilder().startObject()
                .startObject("field")
                .field("value", "1").field("boost", 4)
                .endObject()
                .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
        doc = jsonBuilder().startObject()
                .startObject("field")
                .field("value", "1").field("boost", 2)
                .endObject()
                .endObject();
        logger.info("TYPE is " + type);
        client().prepareIndex(INDEX, TYPE, "2").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        String termInField = "1" + ((type.equals("float") || type.equals("double")) ? ".0" : "");
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("field.string", termInField)).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().getAt(0).getScore(), equalTo(searchResponse.getHits().getAt(1).getScore() * 2.0f));
    }

    @Test
    public void testTokenCountWithBoost() throws IOException {

        String type = getRandomBaseType();
        XContentBuilder mapping =
                XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("properties")
                        .startObject("field")
                        .field("type", "string")
                        .field("store", "yes")
                        .startObject("fields")
                        .startObject("token_count")
                        .field("type", "token_count")
                        .field("analyzer", "standard")
                        .field("store", "yes")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);
        XContentBuilder doc = jsonBuilder().startObject()
                .startObject("field")
                .field("value", "in einen hering jung und schlank, zwo, drei, vier,...").field("boost", 4)
                .endObject()
                .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
        doc = jsonBuilder().startObject()
                .field("field", "in einen hering jung und schlank, zwo, drei, vier,...")
                .endObject();
        logger.info("TYPE is " + type);
        client().prepareIndex(INDEX, TYPE, "2").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(matchAllQuery()).addField("field.token_count").addField("field").get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat((int) searchResponse.getHits().getAt(0).getFields().get("field.token_count").getValue(), equalTo(9));
        assertThat((int) searchResponse.getHits().getAt(1).getFields().get("field.token_count").getValue(), equalTo(9));
        assertThat(searchResponse.getHits().getAt(0).getFields().get("field").getValue().toString(), equalTo("in einen hering jung und schlank, zwo, drei, vier,..."));
        assertThat(searchResponse.getHits().getAt(1).getFields().get("field").getValue().toString(), equalTo("in einen hering jung und schlank, zwo, drei, vier,..."));
    }

    @Test
    public void testBoostWithWrongArrayRepresentation() throws IOException {
        String type = getRandomBaseType();
        logger.info("TYPE is " + type);
        XContentBuilder mapping =
                XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("properties")
                        .startObject("field")
                        .field("type", type).field("omit_norms", "false")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);
        boolean additionalDoc = randomBoolean();

        XContentBuilder doc;
        if (additionalDoc) {
            doc = jsonBuilder().startObject()
                    .array("field", "1", "2", "3")
                    .endObject();
            client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
            client().admin().indices().prepareRefresh(INDEX).get();
        }

        doc = jsonBuilder().startObject()
                .startObject("field")
                .array("value", "1", "2", "3").field("boost", 1)
                .endObject()
                .endObject();
        try {
            client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("Boost for field in array must be given like this: {field_name: [{\"value\": value}, \"boost\": boost},{...},...]}"));
        }

        doc = jsonBuilder().startObject()
                .array("field", "1", "2", "3")
                .endObject();
        client().prepareIndex(INDEX, TYPE, "2").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        String termInField = "2" + ((type.equals("float") || type.equals("double")) ? ".0" : "");
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("field", termInField)).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l + (additionalDoc ? 1l : 0l)));
    }

    @Test
    public void testGeoPointArrayMultiField() throws IOException {
        XContentBuilder mapping =
                XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("properties")
                        .startObject("field")
                        .field("type", "geo_point")
                        .startObject("fields")
                        .startObject("string_point")
                        .field("type", "string")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();
        createIndex(INDEX, ImmutableSettings.EMPTY, TYPE, mapping);

        Map<String, Float> geoPoint1 = new HashMap<>();
        geoPoint1.put("lat", 10f);
        geoPoint1.put("lon", 20f);
        Map<String, Float> geoPoint2 = new HashMap<>();
        geoPoint2.put("lat", 11f);
        geoPoint2.put("lon", 22f);
        XContentBuilder doc = jsonBuilder().startObject()
                .array("field", geoPoint1, geoPoint2)
                .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(doc).get();

        doc = jsonBuilder().startObject()
                .field("field", geoPoint2)
                .endObject();
        client().prepareIndex(INDEX, TYPE, "2").setSource(doc).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery("string_point", "11.0,22.0")).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
    }


    public String getRandomBaseType() {
        return BASE_TYPES[randomInt(BASE_TYPES.length - 1)];
    }
}

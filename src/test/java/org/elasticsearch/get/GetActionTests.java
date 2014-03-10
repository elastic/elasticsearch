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

package org.elasticsearch.get;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

public class GetActionTests extends ElasticsearchIntegrationTest {

    @Test
    public void simpleGetTests() {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();

        ensureGreen();

        GetResponse response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").get();

        logger.info("--> realtime get 1");
        response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime get 1 (no source, implicit)");
        response = client().prepareGet("test", "type1", "1").setFields(Strings.EMPTY_ARRAY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getFields().size(), equalTo(0));
        assertThat(response.getSourceAsBytes(), nullValue());

        logger.info("--> realtime get 1 (no source, explicit)");
        response = client().prepareGet("test", "type1", "1").setFetchSource(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getFields().size(), equalTo(0));
        assertThat(response.getSourceAsBytes(), nullValue());

        logger.info("--> realtime get 1 (no type)");
        response = client().prepareGet("test", null, "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> non realtime get 1");
        response = client().prepareGet("test", "type1", "1").setRealtime(false).execute().actionGet();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> realtime fetch of field (requires fetching parsing source)");
        response = client().prepareGet("test", "type1", "1").setFields("field1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsBytes(), nullValue());
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime fetch of field & source (requires fetching parsing source)");
        response = client().prepareGet("test", "type1", "1").setFields("field1").setFetchSource("field1", null).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap(), hasKey("field1"));
        assertThat(response.getSourceAsMap(), not(hasKey("field2")));
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> flush the index, so we load it from it");
        client().admin().indices().prepareFlush().execute().actionGet();

        logger.info("--> realtime get 1 (loaded from index)");
        response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> non realtime get 1 (loaded from index)");
        response = client().prepareGet("test", "type1", "1").setRealtime(false).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field (loaded from index)");
        response = client().prepareGet("test", "type1", "1").setFields("field1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsBytes(), nullValue());
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime fetch of field & source (loaded from index)");
        response = client().prepareGet("test", "type1", "1").setFields("field1").setFetchSource(true).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsBytes(), not(nullValue()));
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> update doc 1");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").execute().actionGet();

        logger.info("--> realtime get 1");
        response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1_1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2_1"));

        logger.info("--> update doc 1 again");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1_2", "field2", "value2_2").execute().actionGet();

        response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1_2"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2_2"));

        DeleteResponse deleteResponse = client().prepareDelete("test", "type1", "1").execute().actionGet();
        assertThat(deleteResponse.isFound(), equalTo(true));

        response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
    }

    @Test
    public void simpleMultiGetTests() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // fine
        }
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();

        ensureGreen();

        MultiGetResponse response = client().prepareMultiGet().add("test", "type1", "1").execute().actionGet();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .add("test", "type1", "15")
                .add("test", "type1", "3")
                .add("test", "type1", "9")
                .add("test", "type1", "11")
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(5));
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("15"));
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(false));
        assertThat(response.getResponses()[2].getId(), equalTo("3"));
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[3].getId(), equalTo("9"));
        assertThat(response.getResponses()[3].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[4].getId(), equalTo("11"));
        assertThat(response.getResponses()[4].getResponse().isExists(), equalTo(false));

        // multi get with specific field
        response = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "type1", "1").fields("field"))
                .add(new MultiGetRequest.Item("test", "type1", "3").fields("field"))
                .execute().actionGet();

        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSourceAsBytes(), nullValue());
        assertThat(response.getResponses()[0].getResponse().getField("field").getValues().get(0).toString(), equalTo("value1"));
    }

    @Test
    public void realtimeGetWithCompress() throws Exception {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("_source").field("compress", true).endObject().endObject().endObject())
                .execute().actionGet();

        ensureGreen();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append((char) i);
        }
        String fieldValue = sb.toString();
        client().prepareIndex("test", "type", "1").setSource("field", fieldValue).execute().actionGet();

        // realtime get
        GetResponse getResponse = client().prepareGet("test", "type", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getSourceAsMap().get("field").toString(), equalTo(fieldValue));
    }

    @Test
    public void getFieldsWithDifferentTypes() throws Exception {

        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("_source").field("enabled", true).endObject().endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type2")
                        .startObject("_source").field("enabled", false).endObject()
                        .startObject("properties")
                        .startObject("str").field("type", "string").field("store", "yes").endObject()
                        .startObject("strs").field("type", "string").field("store", "yes").endObject()
                        .startObject("int").field("type", "integer").field("store", "yes").endObject()
                        .startObject("ints").field("type", "integer").field("store", "yes").endObject()
                        .startObject("date").field("type", "date").field("store", "yes").endObject()
                        .startObject("binary").field("type", "binary").field("store", "yes").endObject()
                        .endObject()
                        .endObject().endObject())
                .execute().actionGet();

        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(
                jsonBuilder().startObject()
                        .field("str", "test")
                        .field("strs", new String[]{"A", "B", "C"})
                        .field("int", 42)
                        .field("ints", new int[]{1, 2, 3, 4})
                        .field("date", "2012-11-13T15:26:14.000Z")
                        .field("binary", Base64.encodeBytes(new byte[]{1, 2, 3}))
                        .endObject()).execute().actionGet();

        client().prepareIndex("test", "type2", "1").setSource(
                jsonBuilder().startObject()
                        .field("str", "test")
                        .field("strs", new String[]{"A", "B", "C"})
                        .field("int", 42)
                        .field("ints", new int[]{1, 2, 3, 4})
                        .field("date", "2012-11-13T15:26:14.000Z")
                        .field("binary", Base64.encodeBytes(new byte[]{1, 2, 3}))
                        .endObject()).execute().actionGet();

        // realtime get with stored source
        logger.info("--> realtime get (from source)");
        GetResponse getResponse = client().prepareGet("test", "type1", "1").setFields("str", "strs", "int", "ints", "date", "binary").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat((String) getResponse.getField("str").getValue(), equalTo("test"));
        assertThat(getResponse.getField("strs").getValues(), contains((Object) "A", "B", "C"));
        assertThat((Long) getResponse.getField("int").getValue(), equalTo(42l));
        assertThat(getResponse.getField("ints").getValues(), contains((Object) 1L, 2L, 3L, 4L));
        assertThat((String) getResponse.getField("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat(getResponse.getField("binary").getValue(), instanceOf(String.class)); // its a String..., not binary mapped

        logger.info("--> realtime get (from stored fields)");
        getResponse = client().prepareGet("test", "type2", "1").setFields("str", "strs", "int", "ints", "date", "binary").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat((String) getResponse.getField("str").getValue(), equalTo("test"));
        assertThat(getResponse.getField("strs").getValues(), contains((Object) "A", "B", "C"));
        assertThat((Integer) getResponse.getField("int").getValue(), equalTo(42));
        assertThat(getResponse.getField("ints").getValues(), contains((Object) 1, 2, 3, 4));
        assertThat((String) getResponse.getField("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat((BytesReference) getResponse.getField("binary").getValue(), equalTo((BytesReference) new BytesArray(new byte[]{1, 2, 3})));

        logger.info("--> flush the index, so we load it from it");
        client().admin().indices().prepareFlush().execute().actionGet();

        logger.info("--> non realtime get (from source)");
        getResponse = client().prepareGet("test", "type1", "1").setFields("str", "strs", "int", "ints", "date", "binary").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat((String) getResponse.getField("str").getValue(), equalTo("test"));
        assertThat(getResponse.getField("strs").getValues(), contains((Object) "A", "B", "C"));
        assertThat((Long) getResponse.getField("int").getValue(), equalTo(42l));
        assertThat(getResponse.getField("ints").getValues(), contains((Object) 1L, 2L, 3L, 4L));
        assertThat((String) getResponse.getField("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat(getResponse.getField("binary").getValue(), instanceOf(String.class)); // its a String..., not binary mapped

        logger.info("--> non realtime get (from stored fields)");
        getResponse = client().prepareGet("test", "type2", "1").setFields("str", "strs", "int", "ints", "date", "binary").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat((String) getResponse.getField("str").getValue(), equalTo("test"));
        assertThat(getResponse.getField("strs").getValues(), contains((Object) "A", "B", "C"));
        assertThat((Integer) getResponse.getField("int").getValue(), equalTo(42));
        assertThat(getResponse.getField("ints").getValues(), contains((Object) 1, 2, 3, 4));
        assertThat((String) getResponse.getField("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat((BytesReference) getResponse.getField("binary").getValue(), equalTo((BytesReference) new BytesArray(new byte[]{1, 2, 3})));
    }

    @Test
    public void testGetDocWithMultivaluedFields() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // fine
        }
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                .startObject("field").field("type", "string").field("store", "yes").endObject()
                .endObject()
                .endObject().endObject().string();
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("properties")
                .startObject("field").field("type", "string").field("store", "yes").endObject()
                .endObject()
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test")
                .addMapping("type1", mapping1)
                .addMapping("type2", mapping2)
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .execute().actionGet();

        ensureGreen();

        GetResponse response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));
        response = client().prepareGet("test", "type2", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));

        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("field", "1", "2").endObject())
                .execute().actionGet();

        client().prepareIndex("test", "type2", "1")
                .setSource(jsonBuilder().startObject().field("field", "1", "2").endObject())
                .execute().actionGet();

        response = client().prepareGet("test", "type1", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getType(), equalTo("type1"));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));


        response = client().prepareGet("test", "type2", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getType(), equalTo("type2"));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));

        // Now test values being fetched from stored fields.
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        response = client().prepareGet("test", "type1", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));


        response = client().prepareGet("test", "type2", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));
    }

    @Test
    public void testThatGetFromTranslogShouldWorkWithExclude() throws Exception {
        String index = "test";
        String type = "type1";

        String mapping = jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("_source")
                .array("excludes", "excluded")
                .endObject()
                .endObject()
                .endObject()
                .string();

        client().admin().indices().prepareCreate(index)
                .addMapping(type, mapping)
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .execute().actionGet();

        client().prepareIndex(index, type, "1")
                .setSource(jsonBuilder().startObject().field("field", "1", "2").field("excluded", "should not be seen").endObject())
                .execute().actionGet();

        GetResponse responseBeforeFlush = client().prepareGet(index, type, "1").execute().actionGet();
        client().admin().indices().prepareFlush(index).execute().actionGet();
        GetResponse responseAfterFlush = client().prepareGet(index, type, "1").execute().actionGet();

        assertThat(responseBeforeFlush.isExists(), is(true));
        assertThat(responseAfterFlush.isExists(), is(true));
        assertThat(responseBeforeFlush.getSourceAsMap(), hasKey("field"));
        assertThat(responseBeforeFlush.getSourceAsMap(), not(hasKey("excluded")));
        assertThat(responseBeforeFlush.getSourceAsString(), is(responseAfterFlush.getSourceAsString()));
    }

    @Test
    public void testThatGetFromTranslogShouldWorkWithInclude() throws Exception {
        String index = "test";
        String type = "type1";

        String mapping = jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("_source")
                .array("includes", "included")
                .endObject()
                .endObject()
                .endObject()
                .string();

        client().admin().indices().prepareCreate(index)
                .addMapping(type, mapping)
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .execute().actionGet();

        client().prepareIndex(index, type, "1")
                .setSource(jsonBuilder().startObject().field("field", "1", "2").field("included", "should be seen").endObject())
                .execute().actionGet();

        GetResponse responseBeforeFlush = client().prepareGet(index, type, "1").execute().actionGet();
        client().admin().indices().prepareFlush(index).execute().actionGet();
        GetResponse responseAfterFlush = client().prepareGet(index, type, "1").execute().actionGet();

        assertThat(responseBeforeFlush.isExists(), is(true));
        assertThat(responseAfterFlush.isExists(), is(true));
        assertThat(responseBeforeFlush.getSourceAsMap(), not(hasKey("field")));
        assertThat(responseBeforeFlush.getSourceAsMap(), hasKey("included"));
        assertThat(responseBeforeFlush.getSourceAsString(), is(responseAfterFlush.getSourceAsString()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testThatGetFromTranslogShouldWorkWithIncludeExcludeAndFields() throws Exception {
        String index = "test";
        String type = "type1";

        String mapping = jsonBuilder()
                .startObject()
                .startObject(type)
                .startObject("_source")
                .array("includes", "included")
                .array("exlcudes", "excluded")
                .endObject()
                .endObject()
                .endObject()
                .string();

        client().admin().indices().prepareCreate(index)
                .addMapping(type, mapping)
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .execute().actionGet();

        client().prepareIndex(index, type, "1")
                .setSource(jsonBuilder().startObject()
                        .field("field", "1", "2")
                        .startObject("included").field("field", "should be seen").field("field2", "extra field to remove").endObject()
                        .startObject("excluded").field("field", "should not be seen").field("field2", "should not be seen").endObject()
                        .endObject())
                .execute().actionGet();

        GetResponse responseBeforeFlush = client().prepareGet(index, type, "1").setFields("_source", "included.field", "excluded.field").execute().actionGet();
        assertThat(responseBeforeFlush.isExists(), is(true));
        assertThat(responseBeforeFlush.getSourceAsMap(), not(hasKey("excluded")));
        assertThat(responseBeforeFlush.getSourceAsMap(), not(hasKey("field")));
        assertThat(responseBeforeFlush.getSourceAsMap(), hasKey("included"));

        // now tests that extra source filtering works as expected
        GetResponse responseBeforeFlushWithExtraFilters = client().prepareGet(index, type, "1").setFields("included.field", "excluded.field")
                .setFetchSource(new String[]{"field", "*.field"}, new String[]{"*.field2"}).get();
        assertThat(responseBeforeFlushWithExtraFilters.isExists(), is(true));
        assertThat(responseBeforeFlushWithExtraFilters.getSourceAsMap(), not(hasKey("excluded")));
        assertThat(responseBeforeFlushWithExtraFilters.getSourceAsMap(), not(hasKey("field")));
        assertThat(responseBeforeFlushWithExtraFilters.getSourceAsMap(), hasKey("included"));
        assertThat((Map<String, Object>) responseBeforeFlushWithExtraFilters.getSourceAsMap().get("included"), hasKey("field"));
        assertThat((Map<String, Object>) responseBeforeFlushWithExtraFilters.getSourceAsMap().get("included"), not(hasKey("field2")));

        client().admin().indices().prepareFlush(index).execute().actionGet();
        GetResponse responseAfterFlush = client().prepareGet(index, type, "1").setFields("_source", "included.field", "excluded.field").execute().actionGet();
        GetResponse responseAfterFlushWithExtraFilters = client().prepareGet(index, type, "1").setFields("included.field", "excluded.field")
                .setFetchSource("*.field", "*.field2").get();

        assertThat(responseAfterFlush.isExists(), is(true));
        assertThat(responseBeforeFlush.getSourceAsString(), is(responseAfterFlush.getSourceAsString()));

        assertThat(responseAfterFlushWithExtraFilters.isExists(), is(true));
        assertThat(responseBeforeFlushWithExtraFilters.getSourceAsString(), is(responseAfterFlushWithExtraFilters.getSourceAsString()));
    }

    @Test
    public void testGetWithVersion() {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        GetResponse response = client().prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").execute().actionGet();

        // From translog:

        // version 0 means ignore version, which is the default
        response = client().prepareGet("test", "type1", "1").setVersion(0).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1l));

        response = client().prepareGet("test", "type1", "1").setVersion(1).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1l));

        try {
            client().prepareGet("test", "type1", "1").setVersion(2).execute().actionGet();
            fail();
        } catch (VersionConflictEngineException e) {
        }

        // From Lucene index:
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        // version 0 means ignore version, which is the default
        response = client().prepareGet("test", "type1", "1").setVersion(0).setRealtime(false).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1l));

        response = client().prepareGet("test", "type1", "1").setVersion(1).setRealtime(false).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1l));

        try {
            client().prepareGet("test", "type1", "1").setVersion(2).setRealtime(false).execute().actionGet();
            fail();
        } catch (VersionConflictEngineException e) {
        }

        logger.info("--> index doc 1 again, so increasing the version");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").execute().actionGet();

        // From translog:

        // version 0 means ignore version, which is the default
        response = client().prepareGet("test", "type1", "1").setVersion(0).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(2l));

        try {
            client().prepareGet("test", "type1", "1").setVersion(1).execute().actionGet();
            fail();
        } catch (VersionConflictEngineException e) {
        }

        response = client().prepareGet("test", "type1", "1").setVersion(2).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(2l));

        // From Lucene index:
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        // version 0 means ignore version, which is the default
        response = client().prepareGet("test", "type1", "1").setVersion(0).setRealtime(false).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(2l));

        try {
            client().prepareGet("test", "type1", "1").setVersion(1).setRealtime(false).execute().actionGet();
            fail();
        } catch (VersionConflictEngineException e) {
        }

        response = client().prepareGet("test", "type1", "1").setVersion(2).setRealtime(false).execute().actionGet();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(2l));
    }

    @Test
    public void testMultiGetWithVersion() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // fine
        }
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();

        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        MultiGetResponse response = client().prepareMultiGet().add("test", "type1", "1").execute().actionGet();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 3; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        // Version from translog
        response = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "type1", "1").version(0))
                .add(new MultiGetRequest.Item("test", "type1", "1").version(1))
                .add(new MultiGetRequest.Item("test", "type1", "1").version(2))
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[1].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("VersionConflictEngineException"));

        //Version from Lucene index
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        response = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "type1", "1").version(0))
                .add(new MultiGetRequest.Item("test", "type1", "1").version(1))
                .add(new MultiGetRequest.Item("test", "type1", "1").version(2))
                .setRealtime(false)
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[1].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("VersionConflictEngineException"));


        for (int i = 0; i < 3; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        // Version from translog
        response = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "type1", "2").version(0))
                .add(new MultiGetRequest.Item("test", "type1", "2").version(1))
                .add(new MultiGetRequest.Item("test", "type1", "2").version(2))
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("VersionConflictEngineException"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[2].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));


        //Version from Lucene index
        client().admin().indices().prepareRefresh("test").execute().actionGet();
        response = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "type1", "2").version(0))
                .add(new MultiGetRequest.Item("test", "type1", "2").version(1))
                .add(new MultiGetRequest.Item("test", "type1", "2").version(2))
                .setRealtime(false)
                .execute().actionGet();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("VersionConflictEngineException"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[2].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
    }

    @Test
    public void testGetFields_metaData() throws Exception {
        client().admin().indices().prepareCreate("my-index")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .get();

        client().prepareIndex("my-index", "my-type1", "1")
                .setRouting("1")
                .setSource(jsonBuilder().startObject().field("field1", "value").endObject())
                .get();

        GetResponse getResponse = client().prepareGet("my-index", "my-type1", "1")
                .setRouting("1")
                .setFields("field1", "_routing")
                .get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
        assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
        assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
        assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));

        client().admin().indices().prepareFlush("my-index").get();

        client().prepareGet("my-index", "my-type1", "1")
                .setFields("field1", "_routing")
                .setRouting("1")
                .get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
        assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
        assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
        assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
    }

    @Test
    public void testGetFields_nonLeafField() throws Exception {
        client().admin().indices().prepareCreate("my-index")
                .addMapping("my-type1", jsonBuilder().startObject().startObject("my-type1").startObject("properties")
                        .startObject("field1").startObject("properties")
                        .startObject("field2").field("type", "string").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .get();

        client().prepareIndex("my-index", "my-type1", "1")
                .setSource(jsonBuilder().startObject().startObject("field1").field("field2", "value1").endObject().endObject())
                .get();

        try {
            client().prepareGet("my-index", "my-type1", "1").setFields("field1").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}

        client().admin().indices().prepareFlush("my-index").get();

        try {
            client().prepareGet("my-index", "my-type1", "1").setFields("field1").get();
            fail();
        } catch (ElasticsearchIllegalArgumentException e) {}
    }

    @Test
    public void testGetFields_complexField() throws Exception {
        client().admin().indices().prepareCreate("my-index")
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .addMapping("my-type2", jsonBuilder().startObject().startObject("my-type2").startObject("properties")
                        .startObject("field1").field("type", "object")
                            .startObject("field2").field("type", "object")
                                .startObject("field3").field("type", "object")
                                    .startObject("field4").field("type", "string").field("store", "yes")
                                .endObject()
                            .endObject()
                        .endObject()
                        .endObject().endObject().endObject())
                .get();

        BytesReference source = jsonBuilder().startObject()
                    .startArray("field1")
                        .startObject()
                            .startObject("field2")
                                .startArray("field3")
                                    .startObject()
                                        .field("field4", "value1")
                                    .endObject()
                                .endArray()
                            .endObject()
                        .endObject()
                        .startObject()
                            .startObject("field2")
                                .startArray("field3")
                                    .startObject()
                                        .field("field4", "value2")
                                    .endObject()
                                .endArray()
                            .endObject()
                        .endObject()
                    .endArray()
                .endObject().bytes();

        client().prepareIndex("my-index", "my-type1", "1").setSource(source).get();
        client().prepareIndex("my-index", "my-type2", "1").setSource(source).get();


        String field = "field1.field2.field3.field4";
        GetResponse getResponse = client().prepareGet("my-index", "my-type1", "1").setFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        getResponse = client().prepareGet("my-index", "my-type2", "1").setFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        client().admin().indices().prepareFlush("my-index").get();

        getResponse = client().prepareGet("my-index", "my-type1", "1").setFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        getResponse = client().prepareGet("my-index", "my-type2", "1").setFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));
    }

}

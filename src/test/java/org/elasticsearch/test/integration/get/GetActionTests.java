/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.get;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class GetActionTests extends AbstractNodesTests {

    protected Client client;

    @BeforeClass
    public void startNodes() {
        startNode("node1");
        startNode("node2");
        client = client("node1");
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    @Test
    public void simpleGetTests() {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();

        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        GetResponse response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(false));

        logger.info("--> index doc 1");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").execute().actionGet();

        logger.info("--> realtime get 1");
        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime get 1 (no source)");
        response = client.prepareGet("test", "type1", "1").setFields(Strings.EMPTY_ARRAY).execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.source(), nullValue());

        logger.info("--> realtime get 1 (no type)");
        response = client.prepareGet("test", null, "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> non realtime get 1");
        response = client.prepareGet("test", "type1", "1").setRealtime(false).execute().actionGet();
        assertThat(response.exists(), equalTo(false));

        logger.info("--> realtime fetch of field (requires fetching parsing source)");
        response = client.prepareGet("test", "type1", "1").setFields("field1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.source(), nullValue());
        assertThat(response.field("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.field("field2"), nullValue());

        logger.info("--> flush the index, so we load it from it");
        client.admin().indices().prepareFlush().execute().actionGet();

        logger.info("--> realtime get 1 (loaded from index)");
        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> non realtime get 1 (loaded from index)");
        response = client.prepareGet("test", "type1", "1").setRealtime(false).execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field (loaded from index)");
        response = client.prepareGet("test", "type1", "1").setFields("field1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.source(), nullValue());
        assertThat(response.field("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.field("field2"), nullValue());

        logger.info("--> update doc 1");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_1", "field2", "value2_1").execute().actionGet();

        logger.info("--> realtime get 1");
        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1_1"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2_1"));

        logger.info("--> update doc 1 again");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1_2", "field2", "value2_2").execute().actionGet();

        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.sourceAsMap().get("field1").toString(), equalTo("value1_2"));
        assertThat(response.sourceAsMap().get("field2").toString(), equalTo("value2_2"));

        DeleteResponse deleteResponse = client.prepareDelete("test", "type1", "1").execute().actionGet();
        assertThat(deleteResponse.notFound(), equalTo(false));

        response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(false));
    }

    @Test
    public void simpleMultiGetTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // fine
        }
        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)).execute().actionGet();

        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        MultiGetResponse response = client.prepareMultiGet().add("test", "type1", "1").execute().actionGet();
        assertThat(response.responses().length, equalTo(1));
        assertThat(response.responses()[0].response().exists(), equalTo(false));

        for (int i = 0; i < 10; i++) {
            client.prepareIndex("test", "type1", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        response = client.prepareMultiGet()
                .add("test", "type1", "1")
                .add("test", "type1", "15")
                .add("test", "type1", "3")
                .add("test", "type1", "9")
                .add("test", "type1", "11")
                .execute().actionGet();
        assertThat(response.responses().length, equalTo(5));
        assertThat(response.responses()[0].id(), equalTo("1"));
        assertThat(response.responses()[0].response().exists(), equalTo(true));
        assertThat(response.responses()[0].response().sourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.responses()[1].id(), equalTo("15"));
        assertThat(response.responses()[1].response().exists(), equalTo(false));
        assertThat(response.responses()[2].id(), equalTo("3"));
        assertThat(response.responses()[2].response().exists(), equalTo(true));
        assertThat(response.responses()[3].id(), equalTo("9"));
        assertThat(response.responses()[3].response().exists(), equalTo(true));
        assertThat(response.responses()[4].id(), equalTo("11"));
        assertThat(response.responses()[4].response().exists(), equalTo(false));

        // multi get with specific field
        response = client.prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "type1", "1").fields("field"))
                .add(new MultiGetRequest.Item("test", "type1", "3").fields("field"))
                .execute().actionGet();

        assertThat(response.responses().length, equalTo(2));
        assertThat(response.responses()[0].response().source(), nullValue());
        assertThat(response.responses()[0].response().field("field").getValues().get(0).toString(), equalTo("value1"));
    }

    @Test
    public void realtimeGetWithCompress() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("_source").field("compress", true).endObject().endObject().endObject())
                .execute().actionGet();

        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append((char) i);
        }
        String fieldValue = sb.toString();
        client.prepareIndex("test", "type", "1").setSource("field", fieldValue).execute().actionGet();

        // realtime get
        GetResponse getResponse = client.prepareGet("test", "type", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));
        assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo(fieldValue));
    }

    @Test
    public void getFieldsWithDifferentTypes() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .addMapping("type1", jsonBuilder().startObject().startObject("type").startObject("_source").field("enabled", true).endObject().endObject().endObject())
                .addMapping("type2", jsonBuilder().startObject().startObject("type")
                        .startObject("_source").field("enabled", false).endObject()
                        .startObject("properties")
                        .startObject("str").field("type", "string").field("store", "yes").endObject()
                        .startObject("int").field("type", "integer").field("store", "yes").endObject()
                        .startObject("date").field("type", "date").field("store", "yes").endObject()
                        .startObject("binary").field("type", "binary").field("store", "yes").endObject()
                        .endObject()
                        .endObject().endObject())
                .execute().actionGet();

        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        client.prepareIndex("test", "type1", "1").setSource("str", "test", "int", 42, "date", "2012-11-13T15:26:14.000Z", "binary", Base64.encodeBytes(new byte[]{1, 2, 3})).execute().actionGet();
        client.prepareIndex("test", "type2", "1").setSource("str", "test", "int", 42, "date", "2012-11-13T15:26:14.000Z", "binary", Base64.encodeBytes(new byte[]{1, 2, 3})).execute().actionGet();

        // realtime get with stored source
        logger.info("--> realtime get (from source)");
        GetResponse getResponse = client.prepareGet("test", "type1", "1").setFields("str", "int", "date", "binary").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));
        assertThat((String) getResponse.field("str").getValue(), equalTo("test"));
        assertThat((Long) getResponse.field("int").getValue(), equalTo(42l));
        assertThat((String) getResponse.field("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat(getResponse.field("binary").getValue(), instanceOf(String.class)); // its a String..., not binary mapped

        logger.info("--> realtime get (from stored fields)");
        getResponse = client.prepareGet("test", "type2", "1").setFields("str", "int", "date", "binary").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));
        assertThat((String) getResponse.field("str").getValue(), equalTo("test"));
        assertThat((Integer) getResponse.field("int").getValue(), equalTo(42));
        assertThat((String) getResponse.field("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat((BytesReference) getResponse.field("binary").getValue(), equalTo((BytesReference) new BytesArray(new byte[]{1, 2, 3})));

        logger.info("--> flush the index, so we load it from it");
        client.admin().indices().prepareFlush().execute().actionGet();

        logger.info("--> non realtime get (from source)");
        getResponse = client.prepareGet("test", "type1", "1").setFields("str", "int", "date", "binary").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));
        assertThat((String) getResponse.field("str").getValue(), equalTo("test"));
        assertThat((Long) getResponse.field("int").getValue(), equalTo(42l));
        assertThat((String) getResponse.field("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat(getResponse.field("binary").getValue(), instanceOf(String.class)); // its a String..., not binary mapped

        logger.info("--> non realtime get (from stored fields)");
        getResponse = client.prepareGet("test", "type2", "1").setFields("str", "int", "date", "binary").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));
        assertThat((String) getResponse.field("str").getValue(), equalTo("test"));
        assertThat((Integer) getResponse.field("int").getValue(), equalTo(42));
        assertThat((String) getResponse.field("date").getValue(), equalTo("2012-11-13T15:26:14.000Z"));
        assertThat((BytesReference) getResponse.field("binary").getValue(), equalTo((BytesReference) new BytesArray(new byte[]{1, 2, 3})));
    }

    @Test
    public void testGetDocWithMultivaluedFields() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
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
        client.admin().indices().prepareCreate("test")
                .addMapping("type1", mapping1)
                .addMapping("type2", mapping2)
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1))
                .execute().actionGet();

        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        GetResponse response = client.prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(false));
        response = client.prepareGet("test", "type2", "1").execute().actionGet();
        assertThat(response.exists(), equalTo(false));

        client.prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("field", "1", "2").endObject())
                .execute().actionGet();

        client.prepareIndex("test", "type2", "1")
                .setSource(jsonBuilder().startObject().field("field", "1", "2").endObject())
                .execute().actionGet();

        response = client.prepareGet("test", "type1", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getType(), equalTo("type1"));
        assertThat(response.fields().size(), equalTo(1));
        assertThat(response.fields().get("field").getValues().size(), equalTo(1));
        assertThat(((List) response.fields().get("field").getValues().get(0)).size(), equalTo(2));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(0).toString(), equalTo("1"));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(1).toString(), equalTo("2"));


        response = client.prepareGet("test", "type2", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.getType(), equalTo("type2"));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.fields().size(), equalTo(1));
        assertThat(response.fields().get("field").getValues().size(), equalTo(1));
        assertThat(((List) response.fields().get("field").getValues().get(0)).size(), equalTo(2));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(0).toString(), equalTo("1"));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(1).toString(), equalTo("2"));

        // Now test values being fetched from stored fields.
        client.admin().indices().prepareRefresh("test").execute().actionGet();
        response = client.prepareGet("test", "type1", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.fields().size(), equalTo(1));
        assertThat(response.fields().get("field").getValues().size(), equalTo(1));
        assertThat(((List) response.fields().get("field").getValues().get(0)).size(), equalTo(2));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(0).toString(), equalTo("1"));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(1).toString(), equalTo("2"));


        response = client.prepareGet("test", "type2", "1")
                .setFields("field")
                .execute().actionGet();
        assertThat(response.exists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.fields().size(), equalTo(1));
        assertThat(response.fields().get("field").getValues().size(), equalTo(1));
        assertThat(((List) response.fields().get("field").getValues().get(0)).size(), equalTo(2));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(0).toString(), equalTo("1"));
        assertThat(((List) response.fields().get("field").getValues().get(0)).get(1).toString(), equalTo("2"));
    }

}
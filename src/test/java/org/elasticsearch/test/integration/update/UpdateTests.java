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

package org.elasticsearch.test.integration.update;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class UpdateTests extends AbstractNodesTests {
    private Client client;

    @BeforeClass
    public void startNodes() throws Exception {
        startNode("node1", nodeSettings());
        startNode("node2", nodeSettings());
        client = getClient();
    }

    protected void createIndex() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        logger.info("--> creating index test");
        client.admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                        .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();
    }

    protected Settings nodeSettings() {
        return ImmutableSettings.Builder.EMPTY_SETTINGS;
    }

    protected String getConcreteIndexName() {
        return "test";
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void testUpdateRequest() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "type", "1");
        // simple script
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));

        // script with params
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .field("script", "script1")
                .startObject("params").field("param1", "value1").endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));

        // script with params and upsert
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        Map<String, Object> upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .startObject("params").field("param1", "value1").endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("params").field("param1", "value1").endObject()
                .startObject("upsert").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .field("script", "script1")
                .endObject());
        assertThat(request.script(), equalTo("script1"));
        assertThat(request.scriptParams().get("param1").toString(), equalTo("value1"));
        upsertDoc = XContentHelper.convertToMap(request.upsertRequest().source(), true).v2();
        assertThat(upsertDoc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) upsertDoc.get("compound")).get("field2").toString(), equalTo("value2"));

        // script with doc
        request = new UpdateRequest("test", "type", "1");
        request.source(XContentFactory.jsonBuilder().startObject()
                .startObject("doc").field("field1", "value1").startObject("compound").field("field2", "value2").endObject().endObject()
                .endObject());
        Map<String, Object> doc = request.doc().sourceAsMap();
        assertThat(doc.get("field1").toString(), equalTo("value1"));
        assertThat(((Map) doc.get("compound")).get("field2").toString(), equalTo("value2"));
    }

    @Test
    public void testUpsert() throws Exception {
        createIndex();
        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        client.prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript("ctx._source.field += 1")
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("1"));
        }

        client.prepareUpdate("test", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setScript("ctx._source.field += 1")
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("2"));
        }
    }

    @Test
    public void testUpdate() throws Exception {
        createIndex();
        ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        try {
            client.prepareUpdate("test", "type1", "1").setScript("ctx._source.field++").execute().actionGet();
            assert false;
        } catch (DocumentMissingException e) {
            // all is well
        }

        client.prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();

        UpdateResponse updateResponse = client.prepareUpdate("test", "type1", "1").setScript("ctx._source.field += 1").execute().actionGet();
        assertThat(updateResponse.version(), equalTo(2L));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("2"));
        }

        updateResponse = client.prepareUpdate("test", "type1", "1").setScript("ctx._source.field += count").addScriptParam("count", 3).execute().actionGet();
        assertThat(updateResponse.version(), equalTo(3L));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check noop
        updateResponse = client.prepareUpdate("test", "type1", "1").setScript("ctx.op = 'none'").execute().actionGet();
        assertThat(updateResponse.version(), equalTo(3L));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("5"));
        }

        // check delete
        updateResponse = client.prepareUpdate("test", "type1", "1").setScript("ctx.op = 'delete'").execute().actionGet();
        assertThat(updateResponse.version(), equalTo(4L));

        for (int i = 0; i < 5; i++) {
            GetResponse getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.exists(), equalTo(false));
        }

        // check percolation
        client.prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        logger.info("--> register a query");
        client.prepareIndex("_percolator", "test", "1")
                .setSource(jsonBuilder().startObject()
                        .field("query", termQuery("field", 2))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
        updateResponse = client.prepareUpdate("test", "type1", "1").setScript("ctx._source.field += 1").setPercolate("*").execute().actionGet();
        assertThat(updateResponse.matches().size(), equalTo(1));

        // check TTL is kept after an update without TTL
        client.prepareIndex("test", "type1", "2").setSource("field", 1).setTTL(86400000L).setRefresh(true).execute().actionGet();
        GetResponse getResponse = client.prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        long ttl = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl, greaterThan(0L));
        client.prepareUpdate("test", "type1", "2").setScript("ctx._source.field += 1").execute().actionGet();
        getResponse = client.prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl, greaterThan(0L));

        // check TTL update
        client.prepareUpdate("test", "type1", "2").setScript("ctx._ttl = 3600000").execute().actionGet();
        getResponse = client.prepareGet("test", "type1", "2").setFields("_ttl").execute().actionGet();
        ttl = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl, greaterThan(0L));
        assertThat(ttl, lessThan(3600000L));

        // check timestamp update
        client.prepareIndex("test", "type1", "3").setSource("field", 1).setRefresh(true).execute().actionGet();
        client.prepareUpdate("test", "type1", "3").setScript("ctx._timestamp = \"2009-11-15T14:12:12\"").execute().actionGet();
        getResponse = client.prepareGet("test", "type1", "3").setFields("_timestamp").execute().actionGet();
        long timestamp = ((Number) getResponse.field("_timestamp").value()).longValue();
        assertThat(timestamp, equalTo(1258294332000L));

        // check fields parameter
        client.prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client.prepareUpdate("test", "type1", "1").setScript("ctx._source.field += 1").setFields("_source", "field").execute().actionGet();
        assertThat(updateResponse.getResult(), notNullValue());
        assertThat(updateResponse.getResult().sourceRef(), notNullValue());
        assertThat(updateResponse.getResult().field("field").value(), notNullValue());

        // check updates without script
        // add new field
        client.prepareIndex("test", "type1", "1").setSource("field", 1).execute().actionGet();
        updateResponse = client.prepareUpdate("test", "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field2", 2).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("1"));
            assertThat(getResponse.sourceAsMap().get("field2").toString(), equalTo("2"));
        }

        // change existing field
        updateResponse = client.prepareUpdate("test", "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("field", 3).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsMap().get("field").toString(), equalTo("3"));
            assertThat(getResponse.sourceAsMap().get("field2").toString(), equalTo("2"));
        }

        // recursive map
        Map<String, Object> testMap = new HashMap<String, Object>();
        Map<String, Object> testMap2 = new HashMap<String, Object>();
        Map<String, Object> testMap3 = new HashMap<String, Object>();
        testMap3.put("commonkey", testMap);
        testMap3.put("map3", 5);
        testMap2.put("map2", 6);
        testMap.put("commonkey", testMap2);
        testMap.put("map1", 8);

        client.prepareIndex("test", "type1", "1").setSource("map", testMap).execute().actionGet();
        updateResponse = client.prepareUpdate("test", "type1", "1").setDoc(XContentFactory.jsonBuilder().startObject().field("map", testMap3).endObject()).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            getResponse = client.prepareGet("test", "type1", "1").execute().actionGet();
            Map map1 = (Map) getResponse.sourceAsMap().get("map");
            assertThat(map1.size(), equalTo(3));
            assertThat(map1.containsKey("map1"), equalTo(true));
            assertThat(map1.containsKey("map3"), equalTo(true));
            assertThat(map1.containsKey("commonkey"), equalTo(true));
            Map map2 = (Map) map1.get("commonkey");
            assertThat(map2.size(), equalTo(3));
            assertThat(map2.containsKey("map1"), equalTo(true));
            assertThat(map2.containsKey("map2"), equalTo(true));
            assertThat(map2.containsKey("commonkey"), equalTo(true));
        }
    }
}

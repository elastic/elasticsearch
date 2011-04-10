/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.test.integration.search.scriptfield;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class ScriptFieldSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("server1", settingsBuilder().put("number_of_shards", 1).put("number_of_replicas", 0));
        startNode("client1", settingsBuilder().put("node.client", true).build());
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeNode("client1");
        closeAllNodes();
    }

    protected Client getClient() {
        return client("client1");
    }

    @Test public void testDocAndFields() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("num1").field("type", "double").field("store", "yes").endObject()
                .endObject().endObject().endObject().string();

        client.admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        client.prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).field("date", "1970-01-01T00:00:00").endObject())
                .execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).field("date", "1970-01-01T00:00:25").endObject())
                .execute().actionGet();
        client.admin().indices().prepareFlush().execute().actionGet();
        client.prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).field("date", "1970-01-01T00:02:00").endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("running doc['num1'].value");
        SearchResponse response = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "doc['num1'].value")
                .addScriptField("sNum1_field", "_fields['num1'].value")
                .addScriptField("date1", "doc['date'].date.millis")
                .execute().actionGet();

        assertThat("Failures " + Arrays.toString(response.shardFailures()), response.shardFailures().length, equalTo(0));

        assertThat(response.hits().totalHits(), equalTo(3l));
        assertThat(response.hits().getAt(0).isSourceEmpty(), equalTo(true));
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(1.0));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1_field").values().get(0), equalTo(1.0));
        assertThat((Long) response.hits().getAt(0).fields().get("date1").values().get(0), equalTo(0l));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1_field").values().get(0), equalTo(2.0));
        assertThat((Long) response.hits().getAt(1).fields().get("date1").values().get(0), equalTo(25000l));
        assertThat(response.hits().getAt(2).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(2).fields().get("sNum1").values().get(0), equalTo(3.0));
        assertThat((Double) response.hits().getAt(2).fields().get("sNum1_field").values().get(0), equalTo(3.0));
        assertThat((Long) response.hits().getAt(2).fields().get("date1").values().get(0), equalTo(120000l));

        logger.info("running doc['num1'].value * factor");
        Map<String, Object> params = MapBuilder.<String, Object>newMapBuilder().put("factor", 2.0).map();
        response = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addSort("num1", SortOrder.ASC)
                .addScriptField("sNum1", "doc['num1'].value * factor", params)
                .execute().actionGet();

        assertThat(response.hits().totalHits(), equalTo(3l));
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat((Double) response.hits().getAt(0).fields().get("sNum1").values().get(0), equalTo(2.0));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));
        assertThat((Double) response.hits().getAt(1).fields().get("sNum1").values().get(0), equalTo(4.0));
        assertThat(response.hits().getAt(2).id(), equalTo("3"));
        assertThat((Double) response.hits().getAt(2).fields().get("sNum1").values().get(0), equalTo(6.0));
    }

    @Test public void testScriptFieldUsingSource() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // its ok
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                        .startObject("obj1").field("test", "something").endObject()
                        .startObject("obj2").startArray("arr2").value("arr_value1").value("arr_value2").endArray().endObject()
                        .startArray("arr3").startObject().field("arr3_field1", "arr3_value1").endObject().endArray()
                        .endObject())
                .execute().actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        SearchResponse response = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addField("_source.obj1") // we also automatically detect _source in fields
                .addScriptField("s_obj1", "_source.obj1")
                .addScriptField("s_obj1_test", "_source.obj1.test")
                .addScriptField("s_obj2", "_source.obj2")
                .addScriptField("s_obj2_arr2", "_source.obj2.arr2")
                .addScriptField("s_arr3", "_source.arr3")
                .execute().actionGet();

        assertThat("Failures " + Arrays.toString(response.shardFailures()), response.shardFailures().length, equalTo(0));

        Map<String, Object> sObj1 = (Map<String, Object>) response.hits().getAt(0).field("_source.obj1").value();
        assertThat(sObj1.get("test").toString(), equalTo("something"));
        assertThat(response.hits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        sObj1 = (Map<String, Object>) response.hits().getAt(0).field("s_obj1").value();
        assertThat(sObj1.get("test").toString(), equalTo("something"));
        assertThat(response.hits().getAt(0).field("s_obj1_test").value().toString(), equalTo("something"));

        Map<String, Object> sObj2 = (Map<String, Object>) response.hits().getAt(0).field("s_obj2").value();
        List sObj2Arr2 = (List) sObj2.get("arr2");
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

        sObj2Arr2 = (List) response.hits().getAt(0).field("s_obj2_arr2").value();
        assertThat(sObj2Arr2.size(), equalTo(2));
        assertThat(sObj2Arr2.get(0).toString(), equalTo("arr_value1"));
        assertThat(sObj2Arr2.get(1).toString(), equalTo("arr_value2"));

        List sObj2Arr3 = (List) response.hits().getAt(0).field("s_arr3").value();
        assertThat(((Map) sObj2Arr3.get(0)).get("arr3_field1").toString(), equalTo("arr3_value1"));
    }
}
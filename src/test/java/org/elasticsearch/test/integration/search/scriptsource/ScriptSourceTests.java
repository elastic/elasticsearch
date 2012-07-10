/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.search.scriptsource;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.textQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class ScriptSourceTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1", settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        startNode("client1", settingsBuilder().put("node.client", true).build());
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("client1");
    }

    @Test
    public void testWithSourceDisabled() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_source").field("enabled", false).endObject()
                .startObject("_id").field("store", "yes").endObject()
                .startObject("properties")
                .startObject("text").field("type", "string").field("store", "no").endObject()
                .startObject("num").field("type", "integer").field("store", "no").endObject()
                .endObject().endObject().endObject().string();

        client.admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();

        for (int i = 0; i < 10; i++) {
            String id = String.valueOf(i);
            client.prepareIndex("test", "type1", id).setSource(jsonBuilder().startObject()
                    .field("text", "Test value " + id)
                    .field("num", i + 10)
                    .endObject()).execute().actionGet();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        String sourceScript = "{" +
                "\"text\" : \"Test value \" + _fields._id.value + \" from script\"," +
                "\"num\" : 10 + (int)_fields._id.value" +
                "}";

        // Make sure that fields cannot be retrieved when source script is not set
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery()).addField("text").addField("num")
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(0));

        // Fields can be retrieved from script generated source
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery()).addField("text").addField("num")
                .setScriptSource(sourceScript)
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).fields().size(), equalTo(2));
            assertThat(searchResponse.hits().getAt(i).fields().get("text").value().toString(), equalTo("Test value " + i + " from script"));
            assertThat(searchResponse.hits().getAt(i).fields().get("num").value().toString(), equalTo(String.valueOf(i + 10)));
        }

        // Check that highlighting works with generated source
        searchResponse = client.prepareSearch()
                .setQuery(textQuery("text", "value"))
                .setScriptSource(sourceScript)
                .addHighlightedField("text")
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).highlightFields().get("text").fragments().length, equalTo(1));
            assertThat(searchResponse.hits().getAt(i).highlightFields().get("text").fragments()[0].string(), startsWith("Test <em>value</em> " + i + " from script"));
        }

        // Check that script_fields work with generated source
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addScriptField("script_test", "_source.text.replace(\"script\", \"script_field\")")
                .setScriptSource(sourceScript)
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).fields().size(), equalTo(1));
            assertThat(searchResponse.hits().getAt(i).fields().get("script_test").value().toString(), equalTo("Test value " + i + " from script_field"));
        }

        // Source returned as a string
        searchResponse = client.prepareSearch()
                .setQuery(textQuery("text", "value"))
                .addField("text")
                .setScriptSource("\"{\\\"text\\\":\\\"Test value 0 from script returning text\\\"}\"")
                .addHighlightedField("text")
                .addSort("num", SortOrder.ASC)
                .setSize(1)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().get("text").value().toString(), equalTo("Test value 0 from script returning text"));
        assertThat(searchResponse.hits().getAt(0).highlightFields().get("text").fragments().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).highlightFields().get("text").fragments()[0].string(), startsWith("Test <em>value</em> 0 from script returning text"));

    }


    @Test
    public void testWithSourceEnabled() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_source").field("enabled", true).endObject()
                .startObject("_id").field("store", "yes").endObject()
                .startObject("properties")
                .startObject("text").field("type", "string").field("store", "no").endObject()
                .startObject("num").field("type", "integer").field("store", "no").endObject()
                .endObject().endObject().endObject().string();

        client.admin().indices().preparePutMapping().setType("type1").setSource(mapping).execute().actionGet();


        String[] originalSource = new String[10];
        for (int i = 0; i < 10; i++) {
            String id = String.valueOf(i);
            originalSource[i] = jsonBuilder().startObject()
                    .field("text", "Test value " + id)
                    .field("num", i + 10)
                    .endObject().string()
                    + "\n\n"; // Add \n\n to later make sure that original source wasn't reformatted.
            client.prepareIndex("test", "type1", id).setSource(originalSource[i]).execute().actionGet();
        }

        client.admin().indices().prepareRefresh().execute().actionGet();

        String sourceScript = "{" +
                "\"text\" : \"Test value \" + _fields._id.value + \" from script\"," +
                "\"num\" : 20 + (int)_fields._id.value" +
                "}";

        // Make sure that fields can be retrieved when source script is not set
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery()).addField("text").addField("num")
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(2));
        assertThat(searchResponse.hits().getAt(0).fields().get("text").value().toString(), equalTo("Test value 0"));
        assertThat(searchResponse.hits().getAt(0).fields().get("num").value().toString(), equalTo("10"));

        // Test that script generated source can be returned
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setScriptSource(sourceScript)
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).fields().size(), equalTo(0));
            assertThat(searchResponse.hits().getAt(i).sourceAsString(), containsString("Test value " + i + " from script"));
            assertThat(searchResponse.hits().getAt(i).sourceAsString(), containsString(String.valueOf(20 + i)));
        }

        // Test that script generated source can replace stored source for fields
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery()).addField("text").addField("num")
                .setScriptSource(sourceScript)
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).fields().size(), equalTo(2));
            assertThat(searchResponse.hits().getAt(i).fields().get("text").value().toString(), equalTo("Test value " + i + " from script"));
            assertThat(searchResponse.hits().getAt(i).fields().get("num").value().toString(), equalTo(String.valueOf(i + 20)));
            // We didn't ask for source so it shouldn't be returned
            assertThat(searchResponse.hits().getAt(i).sourceAsString(), nullValue());
        }

        // Check that highlighting works with generated source
        searchResponse = client.prepareSearch()
                .setQuery(textQuery("text", "value"))
                .setScriptSource(sourceScript)
                .addHighlightedField("text")
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).highlightFields().get("text").fragments().length, equalTo(1));
            assertThat(searchResponse.hits().getAt(i).highlightFields().get("text").fragments()[0].string(), startsWith("Test <em>value</em> " + i + " from script"));
        }

        // Check that script_fields work with generated source
        searchResponse = client.prepareSearch()
                .setQuery(textQuery("text", "value"))
                .addScriptField("script_test", "_source.text.replace(\"script\", \"script_field\")")
                .addHighlightedField("text")
                .setScriptSource(sourceScript)
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).fields().size(), equalTo(1));
            assertThat(searchResponse.hits().getAt(i).fields().get("script_test").value().toString(), equalTo("Test value " + i + " from script_field"));
        }

        // Test that script can turn off
        searchResponse = client.prepareSearch()
                .setQuery(textQuery("text", "value"))
                .addField("text")
                .addField("num")
                .setScriptSource("null")
                .addSort("num", SortOrder.ASC)
                .setSize(1)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(1));
        assertThat(searchResponse.hits().getAt(0).fields().size(), equalTo(0));

        // Make sure that original source can be left intact
        searchResponse = client.prepareSearch()
                .setQuery(matchAllQuery())
                .addHighlightedField("text")
                .setScriptSource("_source")
                .addSort("num", SortOrder.ASC)
                .execute().actionGet();
        assertThat(searchResponse.hits().getTotalHits(), equalTo(10l));
        assertThat(searchResponse.hits().hits().length, equalTo(10));
        for (int i = 0; i < 10; i++) {
            assertThat(searchResponse.hits().getAt(i).sourceAsString(), equalTo(originalSource[i]));
        }


    }
}

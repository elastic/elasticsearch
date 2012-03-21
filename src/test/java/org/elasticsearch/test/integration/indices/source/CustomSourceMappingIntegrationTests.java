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

package org.elasticsearch.test.integration.indices.source;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.elasticsearch.test.unit.index.mapper.source.TestSourceProviderParser;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.deleteIndexRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class CustomSourceMappingIntegrationTests extends AbstractNodesTests {


    protected Settings indexSettings(int numOfReplicas, int numOfShards) {
        return settingsBuilder()
                .put("index.number_of_replicas", numOfReplicas)
                .put("index.number_of_shards", numOfShards)
                .build();

    }

    protected void deleteIndex(String index) {
        try {
            client("node1").admin().indices().delete(deleteIndexRequest(index)).actionGet();
        } catch (ElasticSearchException ex) {
            // Ignore
        }
    }


    protected void createIndex(String index, Settings indexSettings) throws IOException {

        logger.info("Creating index test");
        client("node1").admin().indices().create(createIndexRequest(index)
                .settings(
                        settingsBuilder()
                                .put(indexSettings)
                                .put("index.source.provider.test.type", TestSourceProviderParser.class.getName())
                )
                .mapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .field("provider", "test")
                        .endObject()
                        .endObject().endObject().string())).actionGet();
    }

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testSearchWithCustomSource() throws Exception {
        startNode("node1");
        deleteIndex("test");
        createIndex("test", indexSettings(0, 1));

        for (int i = 0; i < 10; i++) {
            client("node1").prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).execute().actionGet();
        }

        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        client("node1").admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client("node1").prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
        SearchHits hits = searchResponse.hits();
        assertThat(hits.totalHits(), equalTo(10l));
        for (int i = 0; i < 10; i++) {
            assertThat(hits.getAt(i).sourceAsString(), equalTo("--id:" + hits.getAt(i).id() + "--"));
        }
    }

    @Test
    public void testGetWithCustomSource() throws Exception {
        startNode("node1");
        deleteIndex("test");
        createIndex("test", indexSettings(0, 1));

        for (int i = 0; i < 10; i++) {
            client("node1").prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).execute().actionGet();
        }

        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        client("node1").admin().indices().prepareFlush().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            GetResponse getResponse = client("node1").prepareGet("test", "type1", Integer.toString(i)).execute().actionGet();
            assertThat(getResponse.sourceAsString(), equalTo("--id:" + i + "--"));
        }
    }

    @Test
    public void testReplication() throws Exception {
        startNode("node1");
        startNode("node2");
        deleteIndex("test");
        createIndex("test", indexSettings(1, 4));

        client("node1").prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("value", "test1")
                .endObject()).execute().actionGet();

        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));
        client("node1").admin().indices().prepareFlush().execute().actionGet();
        for (int i = 0; i < 10; i++) {
            GetResponse getResponse = client("node2").prepareGet("test", "type1", "1").execute().actionGet();
            assertThat(getResponse.sourceAsString(), equalTo("--id:1--"));
        }
    }

    @Test
    public void testMappingSettings() throws Exception {
        startNode("node1");
        startNode("node2");
        deleteIndex("test");
        createIndex("test", indexSettings(1, 4));

        ClusterStateResponse clusterStateResponse = client("node2").admin().cluster().prepareState()
                .setFilterAll().setFilterIndices("test").setFilterMetaData(false)
                .execute().actionGet();

        MetaData metaData = clusterStateResponse.state().metaData();
        MappingMetaData mappingMetaData = metaData.getIndices().get("test").getMappings().get("type1");
        String mapping = mappingMetaData.source().string();
        assertThat(mapping, containsString("\"provider\":\"test\""));

    }

    @Test
    public void testIdBasedSourceProvider() throws Exception {
        startNode("node1");
        try {
            client("node1").admin().indices().delete(deleteIndexRequest("test")).actionGet();
        } catch (ElasticSearchException ex) {
            // Ignore
        }
        logger.info("Creating index test");
        client("node1").admin().indices().create(createIndexRequest("test")
                .settings(
                        settingsBuilder()
                                .put("index.number_of_replicas", 0)
                                .put("index.number_of_shards", 1)
                                .put("index.source.provider.idbased.type", IdBasedSourceProviderParser.class.getName())
                )
                .mapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .field("provider", "idbased")
                        .field("source_pattern", "{\"_id\":\"%1$s\", \"_type\":\"%2$s\", "
                                + "\"body\":\"This record has id %1$s and type %2$s\", \"generated\":true}")
                        .endObject()
                        .startObject("properties")

                        .startObject("body")
                        .field("type", "string")
                        .field("store", "no")
                        .endObject()

                        .endObject()
                        .endObject().endObject().string())).actionGet();

        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));

        for (int i = 0; i < 10; i++) {
            client("node1").prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("_id", i)
                    .field("body", "This record has id " + i + " and type type1")
                    .endObject()).execute().actionGet();
        }

        // Test get before flush
        GetResponse getResponse = client("node1").prepareGet("test", "type1", "1")
                .setFields("body", "_source")
                .setRealtime(true)
                .execute().actionGet();
        assertThat(getResponse.sourceAsString(), containsString("This record has id 1 and type type1"));
        assertThat((String) getResponse.field("body").value(), equalTo("This record has id 1 and type type1"));

        client("node1").admin().indices().prepareFlush().execute().actionGet();

        SearchResponse searchResponse = client("node1").prepareSearch()
                .setQuery(queryString("body:\"has id 3\""))
                .addField("generated")
                .addHighlightedField("body", 100, 1)
                .setHighlighterPreTags("!--")
                .setHighlighterPostTags("--!")
                .addScriptField("script_body", "'(' + _source.body + ')'")
                .execute().actionGet();
        SearchHits hits = searchResponse.hits();
        // Check that body field was indexed
        assertThat(hits.totalHits(), equalTo(1l));
        // Check that generated sources is accessible
        assertThat(hits.getAt(0).field("generated").<Boolean>value(), equalTo(true));
        String[] fragments = hits.getAt(0).highlightFields().get("body").fragments();
        assertThat(fragments.length, equalTo(1));
        assertThat(fragments[0], containsString("!--has--! !--id--! !--3--!"));
        assertThat(hits.getAt(0).field("script_body").<String>value(), equalTo("(This record has id 3 and type type1)"));

        // Test get after flush
        getResponse = client("node1").prepareGet("test", "type1", "2")
                .setFields("body", "_source", "generated", "'[' + _source.body + ']'")
                .execute().actionGet();
        assertThat(getResponse.sourceAsString(), containsString("This record has id 2 and type type1"));
        assertThat((String) getResponse.field("body").value(), equalTo("This record has id 2 and type type1"));
        assertThat((String) getResponse.field("'[' + _source.body + ']'").value(), equalTo("[This record has id 2 and type type1]"));
        assertThat((Boolean) getResponse.field("generated").value(), equalTo(true));
    }

    @Test
    public void testTransformingSourceProvider() throws Exception {
        startNode("node1");
        try {
            client("node1").admin().indices().delete(deleteIndexRequest("test")).actionGet();
        } catch (ElasticSearchException ex) {
            // Ignore
        }
        logger.info("Creating index test");
        client("node1").admin().indices().create(createIndexRequest("test")
                .settings(
                        settingsBuilder()
                                .put("index.number_of_replicas", 0)
                                .put("index.number_of_shards", 1)
                                .put("index.source.provider.transforming.type", TransformingSourceProviderParser.class.getName())
                )
                .mapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type")
                        .startObject("_source")
                        .field("provider", "transforming")
                        .endObject()
                        .startObject("properties")

                        .startObject("body")
                        .field("type", "string")
                        .field("store", "no")
                        .endObject()

                        .endObject()
                        .endObject().endObject().string())).actionGet();

        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealth.timedOut(), equalTo(false));

        for (int i = 0; i < 10; i++) {
            client("node1").prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("_id", i)
                    .field("body", "Content of a file file" + i + ".txt")
                    .field("file", "file" + i + ".txt")
                    .endObject()).execute().actionGet();
        }

        // Test get before flush
        GetResponse getResponse = client("node1").prepareGet("test", "type1", "1")
                .setFields("body", "_source")
                .setRealtime(true)
                .execute().actionGet();
        assertThat(getResponse.sourceAsString(), containsString("Content of a file file1.txt"));
        assertThat((String) getResponse.field("body").value(), equalTo("Content of a file file1.txt"));

        client("node1").admin().indices().prepareFlush().execute().actionGet();

        SearchResponse searchResponse = client("node1").prepareSearch()
                .setQuery(queryString("body:\"file file3.txt\""))
                .addField("generated")
                .addHighlightedField("body", 100, 1)
                .setHighlighterPreTags("!--")
                .setHighlighterPostTags("--!")
                .addScriptField("script_body", "'(' + _source.body + ')'")
                .execute().actionGet();
        SearchHits hits = searchResponse.hits();
        // Check that body field was indexed
        assertThat(hits.totalHits(), equalTo(1l));
        // Check that generated sources is accessible
        assertThat(hits.getAt(0).field("generated").<Boolean>value(), equalTo(true));
        String[] fragments = hits.getAt(0).highlightFields().get("body").fragments();
        assertThat(fragments.length, equalTo(1));
        assertThat(fragments[0], containsString("!--file--! !--file3--!.!--txt--!"));
        assertThat(hits.getAt(0).field("script_body").<String>value(), equalTo("(Content of a file file3.txt)"));

        // Test get after flush
        getResponse = client("node1").prepareGet("test", "type1", "2")
                .setFields("body", "_source", "generated", "'[' + _source.body + ']'")
                .execute().actionGet();
        assertThat(getResponse.sourceAsString(), containsString("Content of a file file2.txt"));
        assertThat((String) getResponse.field("body").value(), equalTo("Content of a file file2.txt"));
        assertThat((String) getResponse.field("'[' + _source.body + ']'").value(), equalTo("[Content of a file file2.txt]"));
        assertThat((Boolean) getResponse.field("generated").value(), equalTo(true));

        // Test get after flush without requesting source
        getResponse = client("node1").prepareGet("test", "type1", "2")
                .setFields("body", "generated", "'[' + _source.body + ']'")
                .execute().actionGet();
        assertThat((String) getResponse.field("body").value(), equalTo("Content of a file file2.txt"));
        assertThat((String) getResponse.field("'[' + _source.body + ']'").value(), equalTo("[Content of a file file2.txt]"));
        assertThat((Boolean) getResponse.field("generated").value(), equalTo(true));

    }

}

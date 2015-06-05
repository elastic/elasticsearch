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

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class MetaDataWriteDataNodesTests extends ElasticsearchIntegrationTest {

    @Test
    public void testMetaWrittenAlsoOnDataNode() throws Exception {
        // this test checks that index state is written on data only nodes
        String masterNodeName = startMasterNode();
        String redNode = startDataNode("red");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.number_of_replicas", 0)));
        index("test", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        waitForConcreteMappingsOnAll("test", "doc", "text");
        ensureGreen("test");
        assertIndexInMetaState(redNode, "test");
        assertIndexInMetaState(masterNodeName, "test");
        //stop master node and start again with an empty data folder
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        String newMasterNode = startMasterNode();
        ensureGreen("test");
        // wait for mapping also on master becasue then we can be sure the state was written
        waitForConcreteMappingsOnAll("test", "doc", "text");
        // check for meta data
        assertIndexInMetaState(redNode, "test");
        assertIndexInMetaState(newMasterNode, "test");
        // check if index and doc is still there
        ensureGreen("test");
        assertTrue(client().prepareGet("test", "doc", "1").get().isExists());
    }

    @Test
    public void testMetaWrittenOnlyForIndicesOnNodesThatHaveAShard() throws Exception {
        // this test checks that the index state is only written to a data only node if they have a shard of that index allocated on the node
        String masterNode = startMasterNode();
        String blueNode = startDataNode("blue");
        String redNode = startDataNode("red");

        assertAcked(prepareCreate("blue_index").setSettings(Settings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")));
        index("blue_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("red_index").setSettings(Settings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        ensureGreen();
        waitForConcreteMappingsOnAll("blue_index", "doc", "text");
        waitForConcreteMappingsOnAll("red_index", "doc", "text");
        assertIndexNotInMetaState(blueNode, "red_index");
        assertIndexNotInMetaState(redNode, "blue_index");
        assertIndexInMetaState(blueNode, "blue_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");
        assertIndexInMetaState(masterNode, "blue_index");

        // not the index state for blue_index should only be written on blue_node and the for red_index only on red_node
        // we restart red node and master but with empty data folders
        stopNode(redNode);
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        masterNode = startMasterNode();
        redNode = startDataNode("red");

        ensureGreen();
        assertIndexNotInMetaState(blueNode, "red_index");
        assertIndexInMetaState(blueNode, "blue_index");
        assertIndexNotInMetaState(redNode, "red_index");
        assertIndexNotInMetaState(redNode, "blue_index");
        assertIndexNotInMetaState(masterNode, "red_index");
        assertIndexInMetaState(masterNode, "blue_index");
        // check that blue index is still there
        assertFalse(client().admin().indices().prepareExists("red_index").get().isExists());
        assertTrue(client().prepareGet("blue_index", "doc", "1").get().isExists());
        // red index should be gone
        // if the blue node had stored the index state then cluster health would be red and red_index would exist
        assertFalse(client().admin().indices().prepareExists("red_index").get().isExists());

    }

    @Test
    public void testMetaIsRemovedIfAllShardsFromIndexRemoved() throws Exception {
        // this test checks that the index state is removed from a data only node once all shards have been allocated away from it
        String masterNode = startMasterNode();
        String blueNode = startDataNode("blue");
        String redNode = startDataNode("red");

        // create blue_index on blue_node and same for red
        client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("3")).get();
        assertAcked(prepareCreate("blue_index").setSettings(Settings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")));
        index("blue_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("red_index").setSettings(Settings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());

        ensureGreen();
        assertIndexNotInMetaState(redNode, "blue_index");
        assertIndexNotInMetaState(blueNode, "red_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(blueNode, "blue_index");
        assertIndexInMetaState(masterNode, "red_index");
        assertIndexInMetaState(masterNode, "blue_index");

        // now relocate blue_index to red_node and red_index to blue_node
        logger.debug("relocating indices...");
        client().admin().indices().prepareUpdateSettings("blue_index").setSettings(Settings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")).get();
        client().admin().indices().prepareUpdateSettings("red_index").setSettings(Settings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")).get();
        client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).get();
        ensureGreen();
        assertIndexNotInMetaState(redNode, "red_index");
        assertIndexNotInMetaState(blueNode, "blue_index");
        assertIndexInMetaState(redNode, "blue_index");
        assertIndexInMetaState(blueNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");
        assertIndexInMetaState(masterNode, "blue_index");
        waitForConcreteMappingsOnAll("blue_index", "doc", "text");
        waitForConcreteMappingsOnAll("red_index", "doc", "text");

        //at this point the blue_index is on red node and the red_index on blue node
        // now, when we start red and master node again but without data folder, the red index should be gone but the blue index should initialize fine
        stopNode(redNode);
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        masterNode = startMasterNode();
        redNode = startDataNode("red");
        ensureGreen();
        assertIndexNotInMetaState(redNode, "blue_index");
        assertIndexNotInMetaState(blueNode, "blue_index");
        assertIndexNotInMetaState(redNode, "red_index");
        assertIndexInMetaState(blueNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");
        assertIndexNotInMetaState(masterNode, "blue_index");
        assertTrue(client().prepareGet("red_index", "doc", "1").get().isExists());
        // if the red_node had stored the index state then cluster health would be red and blue_index would exist
        assertFalse(client().admin().indices().prepareExists("blue_index").get().isExists());
    }

    @Test
    public void testMetaWrittenWhenIndexIsClosed() throws Exception {
        String masterNode = startMasterNode();
        String redNodeDataPath = createTempDir().toString();
        String redNode = startDataNode("red", redNodeDataPath);
        String blueNode = startDataNode("blue");
        // create red_index on red_node and same for red
        client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("3")).get();
        assertAcked(prepareCreate("red_index").setSettings(Settings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());

        ensureGreen();
        assertIndexNotInMetaState(blueNode, "red_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");

        waitForConcreteMappingsOnAll("red_index", "doc", "text");
        client().admin().indices().prepareClose("red_index").get();
        // close the index
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponse.getState().getMetaData().index("red_index").getState().name(), equalTo(IndexMetaData.State.CLOSE.name()));

        // restart master with empty data folder and maybe red node
        boolean restartRedNode = randomBoolean();
        //at this point the red_index on red node
        if (restartRedNode) {
            stopNode(redNode);
        }
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        masterNode = startMasterNode();
        if (restartRedNode) {
            redNode = startDataNode("red", redNodeDataPath);
        }

        ensureGreen("red_index");
        assertIndexNotInMetaState(blueNode, "red_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");
        clusterStateResponse = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponse.getState().getMetaData().index("red_index").getState().name(), equalTo(IndexMetaData.State.CLOSE.name()));

        // open the index again
        client().admin().indices().prepareOpen("red_index").get();
        clusterStateResponse = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponse.getState().getMetaData().index("red_index").getState().name(), equalTo(IndexMetaData.State.OPEN.name()));
        // restart again
        ensureGreen();
        if (restartRedNode) {
            stopNode(redNode);
        }
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        masterNode = startMasterNode();
        if (restartRedNode) {
            redNode = startDataNode("red", redNodeDataPath);
        }
        ensureGreen("red_index");
        assertIndexNotInMetaState(blueNode, "red_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");
        clusterStateResponse = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponse.getState().getMetaData().index("red_index").getState().name(), equalTo(IndexMetaData.State.OPEN.name()));
        assertTrue(client().prepareGet("red_index", "doc", "1").get().isExists());
    }

    @Test
    public void testMetaWrittenWhenIndexIsClosedAndMetaUpdated() throws Exception {
        String masterNode = startMasterNode();
        String redNodeDataPath = createTempDir().toString();
        String redNode = startDataNode("red", redNodeDataPath);
        // create red_index on red_node and same for red
        client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("2")).get();
        assertAcked(prepareCreate("red_index").setSettings(Settings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());

        logger.info("--> wait for green red_index");
        ensureGreen();
        logger.info("--> wait for meta state written for red_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");

        waitForConcreteMappingsOnAll("red_index", "doc", "text");

        logger.info("--> close red_index");
        client().admin().indices().prepareClose("red_index").get();
        // close the index
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponse.getState().getMetaData().index("red_index").getState().name(), equalTo(IndexMetaData.State.CLOSE.name()));

        logger.info("--> restart red node");
        stopNode(redNode);
        redNode = startDataNode("red", redNodeDataPath);
        client().admin().indices().preparePutMapping("red_index").setType("doc").setSource(jsonBuilder().startObject()
                .startObject("properties")
                .startObject("integer_field")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()).get();

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("red_index").addTypes("doc").get();
        assertNotNull(((LinkedHashMap) (getMappingsResponse.getMappings().get("red_index").get("doc").getSourceAsMap().get("properties"))).get("integer_field"));
        // restart master with empty data folder and maybe red node
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        masterNode = startMasterNode();

        ensureGreen("red_index");
        assertIndexInMetaState(redNode, "red_index");
        assertIndexInMetaState(masterNode, "red_index");
        clusterStateResponse = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponse.getState().getMetaData().index("red_index").getState().name(), equalTo(IndexMetaData.State.CLOSE.name()));
        getMappingsResponse = client().admin().indices().prepareGetMappings("red_index").addTypes("doc").get();
        assertNotNull(((LinkedHashMap) (getMappingsResponse.getMappings().get("red_index").get("doc").getSourceAsMap().get("properties"))).get("integer_field"));

    }

    private String startDataNode(String color) {
        return startDataNode(color, createTempDir().toString());
    }

    private String startDataNode(String color, String newDataPath) {
        Settings.Builder settingsBuilder = Settings.builder()
                .put("node.data", true)
                .put("node.master", false)
                .put("node.color", color)
                .put("path.data", newDataPath);
        return internalCluster().startNode(settingsBuilder.build());
    }

    private String startMasterNode() {
        Settings.Builder settingsBuilder = Settings.builder()
                .put("node.data", false)
                .put("node.master", true)
                .put("path.data", createTempDir().toString());
        return internalCluster().startNode(settingsBuilder.build());
    }

    private void stopNode(String name) throws IOException {
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(name));
    }

    protected void assertIndexNotInMetaState(String nodeName, String indexName) throws Exception {
        assertMetaState(nodeName, indexName, false);
    }

    protected void assertIndexInMetaState(String nodeName, String indexName) throws Exception {
        assertMetaState(nodeName, indexName, true);
    }


    private void assertMetaState(final String nodeName, final String indexName, final boolean shouldBe) throws Exception {
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                logger.info("checking if meta state exists...");
                try {
                    return shouldBe == metaStateExists(nodeName, indexName);
                } catch (Throwable t) {
                    logger.info("failed to load meta state", t);
                    // TODO: loading of meta state fails rarely if the state is deleted while we try to load it
                    // this here is a hack, would be much better to use for example a WatchService
                    return false;
                }
            }
        });
        boolean inMetaSate = metaStateExists(nodeName, indexName);
        if (shouldBe) {
            assertTrue("expected " + indexName + " in meta state of node " + nodeName, inMetaSate);
        } else {
            assertFalse("expected " + indexName + " to not be in meta state of node " + nodeName, inMetaSate);
        }
    }

    private boolean metaStateExists(String nodeName, String indexName) throws Exception {
        GatewayMetaState nodeMetaState = ((InternalTestCluster) cluster()).getInstance(GatewayMetaState.class, nodeName);
        MetaData nodeMetaData = null;
        nodeMetaData = nodeMetaState.loadMetaState();
        ImmutableOpenMap<String, IndexMetaData> indices = nodeMetaData.getIndices();
        boolean inMetaSate = false;
        for (ObjectObjectCursor<String, IndexMetaData> index : indices) {
            inMetaSate = inMetaSate || index.key.equals(indexName);
        }
        return inMetaSate;
    }
}

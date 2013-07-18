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

package org.elasticsearch.test.integration.indices.state;

import junit.framework.Assert;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SimpleIndexStateTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(SimpleIndexStateTests.class);

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testSimpleOpenClose() {
        logger.info("--> starting two nodes....");
        startNode("node1");
        startNode("node2");

        logger.info("--> creating test index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(5));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> testing indices status api...");
        IndicesStatusResponse indicesStatusResponse = client("node1").admin().indices().prepareStatus().execute().actionGet();

        logger.info("--> trying to index into a closed index ...");
        try {
            client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> opening index...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(5));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
    }

    @Test
    public void testFastCloseAfterCreateDoesNotClose() {
        logger.info("--> starting two nodes....");
        startNode("node1");
        startNode("node2");

        logger.info("--> creating test index that cannot be allocated");
        client("node1").admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.routing.allocation.include.tag", "no_such_node")
                .put("index.number_of_replicas", 1).build()).execute().actionGet();

        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth("test").setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));

        try {
            client().admin().indices().prepareClose("test").execute().actionGet();
            Assert.fail("Exception should have been thrown");
        } catch(IndexPrimaryShardNotAllocatedException e) {
            // expected
        }

        logger.info("--> updating test index settings to allow allocation");
        client("node1").admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.routing.allocation.include.tag", "").build()).execute().actionGet();

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(5));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
    }

    @Test
    public void testConsistencyAfterIndexCreationFailure() {
        logger.info("--> starting one node....");
        startNode("node1");

        logger.info("--> deleting test index....");
        try {
            client("node1").admin().indices().prepareDelete("test").execute().actionGet();
        } catch (IndexMissingException ex) {
            // Ignore
        }

        logger.info("--> creating test index with invalid settings ");
        try {
            client("node1").admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("number_of_shards", "bad")).execute().actionGet();
            assert false;
        } catch (SettingsException ex) {
            // Expected
        }

        logger.info("--> creating test index with valid settings ");
        CreateIndexResponse response = client("node1").admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("number_of_shards", 1)).execute().actionGet();
        assertThat(response.isAcknowledged(), equalTo(true));
    }

}

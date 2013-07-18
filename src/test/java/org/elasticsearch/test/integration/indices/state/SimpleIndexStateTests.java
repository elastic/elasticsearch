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

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SimpleIndexStateTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(SimpleIndexStateTests.class);

    @AfterMethod
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

        waitForGreen();
        assertOpen(5, 10);

        closeIndex();
        assertClosed();

        openIndex();
        waitForGreen();
        assertOpen(5, 10);
    }

    /**
     * Verify that attempts to close an index right after it is created are rejected.
     * Also verifies that the index can later be closed and properly reopened.
     * Since this test relies on things taking time, it might fail spuriously.  Sorry.
     */
    @Test
    public void testFastCloseAfterCreateDoesNotClose() {
        logger.info("--> starting two nodes....");
        startNode("node1");
        startNode("node2");

        int shards = 2;
        while (true) {
            logger.info("--> creating test index with {} shards", shards);
            client("node1").admin().indices().prepareCreate("test").setSettings(
                    "index.number_of_shards", shards, "index.number_of_replicas", 1).execute().actionGet();

            logger.info("--> triggering a fast close");
            boolean caughtFastClose = true;
            try {
                closeIndex();
                caughtFastClose = false;
            } catch(IndexPrimaryShardNotAllocatedException e) {
                //expected
            }
            logger.info("--> making sure the fast close occured in the expected state: cluster status = red");
            if (getStatus() != ClusterHealthStatus.RED) {
                caughtFastClose = false;
            }

            if (caughtFastClose) {
                logger.info("--> caught a fast close with {} shards", shards);
                break;
            } else {
                logger.info("--> didn't get a fast close with {} shards so trying more", shards);
                assertThat("We run out of attempts to catch a fast close.", shards <= 1024);
                waitForGreen();
                client("node1").admin().indices().prepareDelete("test").execute();
                ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForActiveShards(0).execute().actionGet();
                assertThat(health.isTimedOut(), equalTo(false));
                shards *= 2;
            }
        }

        waitForGreen();
        assertOpen(shards, shards * 2);

        closeIndex();
        assertClosed();

        openIndex();
        waitForGreen();
        assertOpen(shards, shards * 2);
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

    private void waitForGreen() {
        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setTimeout("30s").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));
    }

    private ClusterHealthStatus getStatus() {
        logger.info("--> should still be red");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().execute().actionGet();
        return health.getStatus();
    }

    private void openIndex() {
        logger.info("--> opening index...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();
    }

    private void assertOpen(int expectedPrimaryShards, int expectedTotalShards) {
        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(expectedPrimaryShards));
        assertThat(stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(expectedTotalShards));
        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
    }

    private void closeIndex() {
        logger.info("--> closing test index...");
        CloseIndexResponse closeResponse = client("node1").admin().indices().prepareClose("test").execute().actionGet();
        assertThat(closeResponse.isAcknowledged(), equalTo(true));
    }

    private void assertClosed() {
        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.getState().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), nullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }
    }
}

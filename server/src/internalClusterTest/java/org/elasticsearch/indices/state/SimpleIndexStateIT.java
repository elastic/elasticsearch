/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SimpleIndexStateIT extends ESIntegTestCase {
    private final Logger logger = LogManager.getLogger(SimpleIndexStateIT.class);

    public void testSimpleOpenClose() {
        logger.info("--> creating test index");
        createIndex("test");

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = clusterAdmin().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").size(), equalTo(numShards.numPrimaries));
        assertEquals(
            stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards
        );

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        assertAcked(indicesAdmin().prepareClose("test"));

        stateResponse = clusterAdmin().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> opening index...");
        OpenIndexResponse openIndexResponse = indicesAdmin().prepareOpen("test").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = clusterAdmin().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));

        assertThat(stateResponse.getState().routingTable().index("test").size(), equalTo(numShards.numPrimaries));
        assertEquals(
            stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards
        );

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
    }

    public void testFastCloseAfterCreateContinuesCreateAfterOpen() {
        logger.info("--> creating test index that cannot be allocated");
        indicesAdmin().prepareCreate("test")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder().put("index.routing.allocation.include.tag", "no_such_node").build())
            .get();

        ClusterHealthResponse health = clusterAdmin().prepareHealth("test").setWaitForNodes(">=2").get();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));

        assertAcked(indicesAdmin().prepareClose("test").setWaitForActiveShards(ActiveShardCount.NONE));

        logger.info("--> updating test index settings to allow allocation");
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include.tag", ""), "test");

        indicesAdmin().prepareOpen("test").get();

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = clusterAdmin().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").size(), equalTo(numShards.numPrimaries));
        assertEquals(
            stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards
        );

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
    }

    public void testConsistencyAfterIndexCreationFailure() {
        logger.info("--> deleting test index....");
        try {
            indicesAdmin().prepareDelete("test").get();
        } catch (IndexNotFoundException ex) {
            // Ignore
        }

        logger.info("--> creating test index with invalid settings ");
        try {
            indicesAdmin().prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", "bad")).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [bad] for setting [index.number_of_shards]", ex.getMessage());
            // Expected
        }

        logger.info("--> creating test index with valid settings ");
        CreateIndexResponse response = indicesAdmin().prepareCreate("test")
            .setSettings(Settings.builder().put("number_of_shards", 1))
            .get();
        assertThat(response.isAcknowledged(), equalTo(true));
    }
}

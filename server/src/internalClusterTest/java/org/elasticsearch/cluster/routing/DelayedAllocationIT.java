/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DelayedAllocationIT extends ESIntegTestCase {

    /**
     * Verifies that when there is no delay timeout, a 1/1 index shard will immediately
     * get allocated to a free node when the node hosting it leaves the cluster.
     */
    public void testNoDelayedTimeout() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0))
            .get();
        ensureGreen("test");
        indexRandomData();
        internalCluster().stopNode(findNodeWithShard());
        assertThat(clusterAdmin().prepareHealth().get().getDelayedUnassignedShards(), equalTo(0));
        ensureGreen("test");
    }

    /**
     * When we do have delayed allocation set, verifies that even though there is a node
     * free to allocate the unassigned shard when the node hosting it leaves, it doesn't
     * get allocated. Once we bring the node back, it gets allocated since it existed
     * on it before.
     */
    public void testDelayedAllocationNodeLeavesAndComesBack() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1))
        ).get();
        ensureGreen("test");
        indexRandomData();
        String nodeWithShard = findNodeWithShard();
        Settings nodeWithShardDataPathSettings = internalCluster().dataPathSettings(nodeWithShard);
        internalCluster().stopNode(nodeWithShard);
        assertBusy(
            () -> assertThat(clusterAdmin().prepareState().all().get().getState().getRoutingNodes().unassigned().size() > 0, equalTo(true))
        );
        assertThat(clusterAdmin().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1));
        internalCluster().startNode(nodeWithShardDataPathSettings); // this will use the same data location as the stopped node
        ensureGreen("test");
    }

    /**
     * With a very small delay timeout, verify that it expires and we get to green even
     * though the node hosting the shard is not coming back.
     */
    public void testDelayedAllocationTimesOut() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(100))
        ).get();
        ensureGreen("test");
        indexRandomData();
        internalCluster().stopNode(findNodeWithShard());
        ensureGreen("test");
        internalCluster().startNode();
        // do a second round with longer delay to make sure it happens
        updateIndexSettings(
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(100)),
            "test"
        );
        internalCluster().stopNode(findNodeWithShard());
        ensureGreen("test");
    }

    /**
     * Verify that when explicitly changing the value of the index setting for the delayed
     * allocation to a very small value, it kicks the allocation of the unassigned shard
     * even though the node it was hosted on will not come back.
     */
    public void testDelayedAllocationChangeWithSettingTo100ms() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1))
        ).get();
        ensureGreen("test");
        indexRandomData();
        internalCluster().stopNode(findNodeWithShard());
        assertBusy(
            () -> assertThat(clusterAdmin().prepareState().all().get().getState().getRoutingNodes().unassigned().size() > 0, equalTo(true))
        );
        assertThat(clusterAdmin().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1));
        logger.info("Setting shorter allocation delay");
        updateIndexSettings(
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(100)),
            "test"
        );
        ensureGreen("test");
        assertThat(clusterAdmin().prepareHealth().get().getDelayedUnassignedShards(), equalTo(0));
    }

    /**
     * Verify that when explicitly changing the value of the index setting for the delayed
     * allocation to 0, it kicks the allocation of the unassigned shard
     * even though the node it was hosted on will not come back.
     */
    public void testDelayedAllocationChangeWithSettingTo0() throws Exception {
        internalCluster().startNodes(3);
        prepareCreate("test").setSettings(
            indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1))
        ).get();
        ensureGreen("test");
        indexRandomData();
        internalCluster().stopNode(findNodeWithShard());
        assertBusy(
            () -> assertThat(clusterAdmin().prepareState().all().get().getState().getRoutingNodes().unassigned().size() > 0, equalTo(true))
        );
        assertThat(clusterAdmin().prepareHealth().get().getDelayedUnassignedShards(), equalTo(1));
        updateIndexSettings(
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(0)),
            "test"
        );
        ensureGreen("test");
        assertThat(clusterAdmin().prepareHealth().get().getDelayedUnassignedShards(), equalTo(0));
    }

    private void indexRandomData() throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex("test").setSource("field", "value");
        }
        // we want to test both full divergent copies of the shard in terms of segments, and
        // a case where they are the same (using sync flush), index Random does all this goodness
        // already
        indexRandom(true, builders);
    }

    private String findNodeWithShard() {
        ClusterState state = clusterAdmin().prepareState().get().getState();
        List<ShardRouting> startedShards = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED);
        return state.nodes().get(randomFrom(startedShards).currentNodeId()).getName();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchRedStateIndexIT extends ESIntegTestCase {

    public void testAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes() + 2;
        buildRedIndex(numShards);

        SearchResponse searchResponse = client().prepareSearch().setSize(0).setAllowPartialSearchResults(true).get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect some shards failed", searchResponse.getFailedShards(), allOf(greaterThan(0), lessThanOrEqualTo(numShards)));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            assertThat(failure.getCause(), instanceOf(NoShardAvailableActionException.class));
        }
    }

    public void testClusterAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes() + 2;
        buildRedIndex(numShards);

        setClusterDefaultAllowPartialResults(true);

        SearchResponse searchResponse = client().prepareSearch().setSize(0).get();
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect some shards failed", searchResponse.getFailedShards(), allOf(greaterThan(0), lessThanOrEqualTo(numShards)));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            assertThat(failure.getCause(), instanceOf(NoShardAvailableActionException.class));
        }
    }

    public void testDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes() + 2);

        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setSize(0).setAllowPartialSearchResults(false).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    public void testClusterDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes() + 2);

        setClusterDefaultAllowPartialResults(false);
        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client().prepareSearch().setSize(0).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    private void setClusterDefaultAllowPartialResults(boolean allowPartialResults) {
        String key = SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey();

        Settings persistentSettings = Settings.builder().put(key, allowPartialResults).build();

        ClusterUpdateSettingsResponse response1 = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(persistentSettings)
            .get();

        assertAcked(response1);
        assertEquals(response1.getPersistentSettings().getAsBoolean(key, null), allowPartialResults);
    }

    private void buildRedIndex(int numShards) throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0)
            )
        );
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field1", "value1").get();
        }
        refresh();

        internalCluster().stopRandomDataNode();

        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            List<ShardRouting> unassigneds = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.UNASSIGNED);
            assertThat(unassigneds.size(), greaterThan(0));
        });

    }

    @After
    public void cleanup() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey()))
        );
    }
}

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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
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

        assertResponse(prepareSearch().setSize(0).setAllowPartialSearchResults(true), response -> {
            assertThat(RestStatus.OK, equalTo(response.status()));
            assertThat("Expect some shards failed", response.getFailedShards(), allOf(greaterThan(0), lessThanOrEqualTo(numShards)));
            assertThat("Expect no shards skipped", response.getSkippedShards(), equalTo(0));
            assertThat("Expect subset of shards successful", response.getSuccessfulShards(), lessThan(numShards));
            assertThat("Expected total shards", response.getTotalShards(), equalTo(numShards));
            for (ShardSearchFailure failure : response.getShardFailures()) {
                assertThat(failure.getCause(), instanceOf(NoShardAvailableActionException.class));
            }
        });
    }

    public void testClusterAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes() + 2;
        buildRedIndex(numShards);

        setClusterDefaultAllowPartialResults(true);

        assertResponse(prepareSearch().setSize(0), response -> {
            assertThat(RestStatus.OK, equalTo(response.status()));
            assertThat("Expect some shards failed", response.getFailedShards(), allOf(greaterThan(0), lessThanOrEqualTo(numShards)));
            assertThat("Expect no shards skipped", response.getSkippedShards(), equalTo(0));
            assertThat("Expect subset of shards successful", response.getSuccessfulShards(), lessThan(numShards));
            assertThat("Expected total shards", response.getTotalShards(), equalTo(numShards));
            for (ShardSearchFailure failure : response.getShardFailures()) {
                assertThat(failure.getCause(), instanceOf(NoShardAvailableActionException.class));
                assertThat(failure.getCause().getStackTrace(), emptyArray());
                // We don't write out the entire, repetitive stacktrace in the reason
                assertThat(failure.reason(), equalTo("org.elasticsearch.action.NoShardAvailableActionException" + System.lineSeparator()));
            }
        });
    }

    public void testDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes() + 2);

        SearchPhaseExecutionException ex = expectThrows(
            SearchPhaseExecutionException.class,
            prepareSearch().setSize(0).setAllowPartialSearchResults(false)
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    public void testClusterDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes() + 2);

        setClusterDefaultAllowPartialResults(false);
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, prepareSearch().setSize(0));
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));
    }

    private void setClusterDefaultAllowPartialResults(boolean allowPartialResults) {
        String key = SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey();

        Settings persistentSettings = Settings.builder().put(key, allowPartialResults).build();

        ClusterUpdateSettingsResponse response1 = clusterAdmin().prepareUpdateSettings().setPersistentSettings(persistentSettings).get();

        assertAcked(response1);
        assertEquals(response1.getPersistentSettings().getAsBoolean(key, null), allowPartialResults);
    }

    private void buildRedIndex(int numShards) throws Exception {
        assertAcked(prepareCreate("test").setSettings(indexSettings(numShards, 0)));
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId("" + i).setSource("field1", "value1").get();
        }
        refresh();

        internalCluster().stopRandomDataNode();

        clusterAdmin().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();

        assertBusy(() -> {
            ClusterState state = clusterAdmin().prepareState().get().getState();
            List<ShardRouting> unassigneds = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.UNASSIGNED);
            assertThat(unassigneds.size(), greaterThan(0));
        });

    }

    @After
    public void cleanup() throws Exception {
        updateClusterSettings(Settings.builder().putNull(SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey()));
    }
}

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

package org.elasticsearch.search.basic;


import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchRedStateIndexIT extends ESIntegTestCase {

    public void testAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes()+2;
        buildRedIndex(numShards);
                        
        SearchResponse searchResponse = client().prepareSearch().setSize(0).setAllowPartialSearchResults(true)
                .get();        
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect no shards failed", searchResponse.getFailedShards(), equalTo(0));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
    }

    public void testClusterAllowPartialsWithRedState() throws Exception {
        final int numShards = cluster().numDataNodes()+2;
        buildRedIndex(numShards);
        
        setClusterDefaultAllowPartialResults(true);
                        
        SearchResponse searchResponse = client().prepareSearch().setSize(0).get();        
        assertThat(RestStatus.OK, equalTo(searchResponse.status()));
        assertThat("Expect no shards failed", searchResponse.getFailedShards(), equalTo(0));
        assertThat("Expect no shards skipped", searchResponse.getSkippedShards(), equalTo(0));
        assertThat("Expect subset of shards successful", searchResponse.getSuccessfulShards(), lessThan(numShards));
        assertThat("Expected total shards", searchResponse.getTotalShards(), equalTo(numShards));
    }
    
    
    public void testDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes()+2);        
        
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class,
                () -> 
            client().prepareSearch().setSize(0).setAllowPartialSearchResults(false).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));        
    }
    
    
    public void testClusterDisallowPartialsWithRedState() throws Exception {
        buildRedIndex(cluster().numDataNodes()+2);
        
        setClusterDefaultAllowPartialResults(false);
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class,
                () -> 
            client().prepareSearch().setSize(0).get()
        );
        assertThat(ex.getDetailedMessage(), containsString("Search rejected due to missing shard"));        
    }

    public void testDisallowPartialsWithRedStateRecovering() throws Exception {
        int docCount = scaledRandomIntBetween(10, 1000);
        logger.info("Using docCount [{}]", docCount);
        buildIndex(cluster().numDataNodes() + 2, 1, docCount);

        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> searchThreads = new ArrayList<>();
        // this is a little extreme, but necessary to make this test fail reasonably often (half the runs on my machine).
        for (int i = 0; i < 100; ++i) {
            Thread searchThread = new Thread() {
                {
                    setDaemon(true);
                }

                @Override
                public void run() {
                    while (stop.get() == false) {
                        // todo: the timeouts below should not be necessary, but this test sometimes hangs without them and that is not
                        // the immediate purpose of the test.
                        verify(() -> client().prepareSearch("test").setQuery(new RangeQueryBuilder("field1").gte(0))
                            .setSize(100).setAllowPartialSearchResults(false).get(TimeValue.timeValueSeconds(10)));
                        verify(() -> client().prepareSearch("test")
                            .setSize(100).setAllowPartialSearchResults(false).get(TimeValue.timeValueSeconds(10)));
                    }
                }

                void verify(Supplier<SearchResponse> call) {
                    try {
                        SearchResponse response = call.get();
                        assertThat(response.getHits().getHits().length, equalTo(Math.min(100, docCount)));
                        assertThat(response.getHits().getTotalHits().value, equalTo((long) docCount));
                    } catch (Exception e) {
                        // this is OK.
                        logger.info("Failed with : " + e);
                    }
                }
            };
            searchThreads.add(searchThread);
            searchThread.start();
        }
        try {
            Thread restartThread = new Thread() {
                {
                    setDaemon(true);
                }

                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 5; ++i) {
                            internalCluster().restartRandomDataNode();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            restartThread.start();
            for (int i = 0; i < 5; ++i) {
                internalCluster().restartRandomDataNode();
            }
            restartThread.join(30000);
            assertFalse(restartThread.isAlive());
        } finally {
            stop.set(true);
            searchThreads.forEach(thread -> {
                try {
                    thread.join(30000);
                    if (thread.isAlive()) {
                        logger.warn("Thread: " + thread + " is still alive");
                        // do not continue unless thread terminates to avoid getting other confusing test errors. Please kill me...
                        thread.join();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // hack to ensure all search contexts are removed, seems we risk leaked search contexts when coordinator dies.
        client().admin().indices().prepareDelete("test").get();
    }

    private void setClusterDefaultAllowPartialResults(boolean allowPartialResults) {
        String key = SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey();

        Settings transientSettings = Settings.builder().put(key, allowPartialResults).build();

        ClusterUpdateSettingsResponse response1 = client().admin().cluster()
                .prepareUpdateSettings()
                .setTransientSettings(transientSettings)
                .get();

        assertAcked(response1);
        assertEquals(response1.getTransientSettings().getAsBoolean(key, null), allowPartialResults);
    }    
    
    private void buildRedIndex(int numShards) throws Exception {
        buildIndex(numShards, 0, 10);

        stopNodeAndEnsureRed();
    }

    private void buildIndex(int numShards, int numReplicas, int docCount) {
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", numShards).put("index.number_of_replicas", numReplicas)));
        ensureGreen();
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex("test", "type1", ""+i).setSource("field1", i).get();
        }
        refresh();
    }

    private void stopNodeAndEnsureRed() throws Exception {
        internalCluster().stopRandomDataNode();

        client().admin().cluster().prepareHealth().setWaitForStatus(ClusterHealthStatus.RED).get();

        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            List<ShardRouting> unassigneds = clusterState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED);
            assertThat(unassigneds.size(), greaterThan(0));
        });
    }

    @After
    public void cleanup() {
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(SearchService.DEFAULT_ALLOW_PARTIAL_SEARCH_RESULTS.getKey())));
    }        
}

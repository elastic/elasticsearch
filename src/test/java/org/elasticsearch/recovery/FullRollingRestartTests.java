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

package org.elasticsearch.recovery;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
public class FullRollingRestartTests extends ElasticsearchIntegrationTest {

    protected void assertTimeout(ClusterHealthRequestBuilder requestBuilder) {
        ClusterHealthResponse clusterHealth = requestBuilder.get();
        if (clusterHealth.isTimedOut()) {
            logger.info("cluster health request timed out:\n{}", clusterHealth);
            fail("cluster health request timed out");
        }
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Test
    @Slow
    public void testFullRollingRestart() throws Exception {
        Settings settings = ImmutableSettings.builder().put(ZenDiscovery.SETTING_JOIN_TIMEOUT, "30s").build();
        internalCluster().startNode(settings);
        createIndex("test");

        final String healthTimeout = "1m";

        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test", "type1", Long.toString(i))
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();
        }
        flush();
        for (int i = 1000; i < 2000; i++) {
            client().prepareIndex("test", "type1", Long.toString(i))
                    .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map()).execute().actionGet();
        }

        logger.info("--> now start adding nodes");
        internalCluster().startNodesAsync(2, settings).get();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout(healthTimeout).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3"));

        logger.info("--> add two more nodes");
        internalCluster().startNodesAsync(2, settings).get();

        // We now have 5 nodes
        setMinimumMasterNodes(3);

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout(healthTimeout).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("5"));

        logger.info("--> refreshing and checking data");
        refresh();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2000l);
        }

        // now start shutting nodes down
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout(healthTimeout).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("4"));

        // going down to 3 nodes. note that the min_master_node may not be in effect when we shutdown the 4th
        // node, but that's OK as it is set to 3 before.
        setMinimumMasterNodes(2);
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout(healthTimeout).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3"));

        logger.info("--> stopped two nodes, verifying data");
        refresh();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2000l);
        }

        // closing the 3rd node
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout(healthTimeout).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("2"));

        // closing the 2nd node
        setMinimumMasterNodes(1);
        internalCluster().stopRandomDataNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout(healthTimeout).setWaitForYellowStatus().setWaitForRelocatingShards(0).setWaitForNodes("1"));

        logger.info("--> one node left, verifying data");
        refresh();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareCount().setQuery(matchAllQuery()).get(), 2000l);
        }
    }
}

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

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UpdateNumberOfReplicasIT extends ESIntegTestCase {

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    @Test
    public void simpleUpdateNumberOfReplicasIT() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test", 2));
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());

        NumShards numShards = getNumShards("test");

        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(numShards.numReplicas));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.totalNumShards));

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).get();
        }

        refresh();

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).get();
            assertHitCount(countResponse, 10l);
        }

        logger.info("Increasing the number of replicas from 1 to 2");
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 2)).execute().actionGet());
        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForActiveShards(numShards.numPrimaries * 2).execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        //only 2 copies allocated (1 replica) across 2 nodes
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        logger.info("starting another node to new replicas will be allocated to it");
        allowNodes("test", 3);

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes(">=3").execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        //all 3 copies allocated across 3 nodes
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).get();
            assertHitCount(countResponse, 10l);
        }

        logger.info("Decreasing number of replicas from 2 to 0");
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 0)).get());

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes(">=3").execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        //a single copy is allocated (replica set to 0)
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 10);
        }
    }

    @Test
    public void testAutoExpandNumberOfReplicas0ToData() throws IOException {
        internalCluster().ensureAtMostNumDataNodes(2);
        logger.info("--> creating index test with auto expand replicas");
        assertAcked(prepareCreate("test", 2, settingsBuilder().put("auto_expand_replicas", "0-all")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 2).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 3).setWaitForNodes(">=3").execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        logger.info("--> closing one node");
        internalCluster().ensureAtMostNumDataNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 2).setWaitForNodes(">=2").execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        logger.info("--> closing another node");
        internalCluster().ensureAtMostNumDataNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes(">=1").setWaitForActiveShards(numShards.numPrimaries).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));
    }

    @Test
    public void testAutoExpandNumberReplicas1ToData() throws IOException {
        logger.info("--> creating index test with auto expand replicas");
        internalCluster().ensureAtMostNumDataNodes(2);
        assertAcked(prepareCreate("test", 2, settingsBuilder().put("auto_expand_replicas", "1-all")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 2).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 3).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        logger.info("--> closing one node");
        internalCluster().ensureAtMostNumDataNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes(">=2").setWaitForActiveShards(numShards.numPrimaries * 2).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        logger.info("--> closing another node");
        internalCluster().ensureAtMostNumDataNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes(">=1").setWaitForActiveShards(numShards.numPrimaries).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));
    }

    @Test
    public void testAutoExpandNumberReplicas2() {
        logger.info("--> creating index test with auto expand replicas set to 0-2");
        assertAcked(prepareCreate("test", 3, settingsBuilder().put("auto_expand_replicas", "0-2")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 3).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        logger.info("--> add two more nodes");
        allowNodes("test", 4);
        allowNodes("test", 5);

        logger.info("--> update the auto expand replicas to 0-3");
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("auto_expand_replicas", "0-3")).execute().actionGet();

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(numShards.numPrimaries * 4).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(3));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 4));
    }

    @Test
    public void testUpdateWithInvalidNumberOfReplicas() {
        createIndex("test");
        try {
            client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.settingsBuilder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(-10, -1))
                )
                .execute().actionGet();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertThat("message contains error about shard count: " + e.getMessage(),
                e.getMessage().contains("the value of the setting index.number_of_replicas must be a non negative integer"), equalTo(true));
        }
    }
}

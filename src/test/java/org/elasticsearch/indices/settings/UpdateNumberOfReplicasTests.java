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

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class UpdateNumberOfReplicasTests extends ElasticsearchIntegrationTest {


    @Test
    public void simpleUpdateNumberOfReplicasTests() throws Exception {
        logger.info("Creating index test");
        prepareCreate("test", 2).execute().actionGet();
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(5));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource(jsonBuilder().startObject()
                    .field("value", "test" + i)
                    .endObject()).get();
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).get();
            assertHitCount(countResponse, 10l);
        }

        logger.info("Increasing the number of replicas from 1 to 2");
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 2)).execute().actionGet();
        Thread.sleep(200);

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForActiveShards(10).execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(5));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(10));

        logger.info("starting another node to new replicas will be allocated to it");
        allowNodes("test", 3);
        Thread.sleep(100);

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes(">=3").execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(5));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(15));

        for (int i = 0; i < 10; i++) {
            CountResponse countResponse = client().prepareCount().setQuery(matchAllQuery()).get();
            assertHitCount(countResponse, 10l);
        }

        logger.info("Decreasing number of replicas from 2 to 0");
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("index.number_of_replicas", 0)).get();
        Thread.sleep(200);

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes(">=3").execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(5));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(5));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 10);
        }
    }

    @Test
    public void testAutoExpandNumberOfReplicas0ToData() {
        cluster().ensureAtMostNumNodes(2);
        logger.info("--> creating index test with auto expand replicas");
        prepareCreate("test", 2, settingsBuilder().put("index.number_of_shards", 2).put("auto_expand_replicas", "0-all")).execute().actionGet();

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(4).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(4));

        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(6).setWaitForNodes(">=3").execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(6));

        logger.info("--> closing one node");
        cluster().ensureAtMostNumNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(4).setWaitForNodes(">=2").execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(4));

        logger.info("--> closing another node");
        cluster().ensureAtMostNumNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes(">=1").setWaitForActiveShards(2).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(2));
    }

    @Test
    public void testAutoExpandNumberReplicas1ToData() {
        logger.info("--> creating index test with auto expand replicas");
        cluster().ensureAtMostNumNodes(2);
        prepareCreate("test", 2, settingsBuilder().put("index.number_of_shards", 2).put("auto_expand_replicas", "1-all")).execute().actionGet();

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(4).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(4));

        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(6).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(6));

        logger.info("--> closing one node");
        cluster().ensureAtMostNumNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForNodes(">=2").setWaitForActiveShards(4).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(4));

        logger.info("--> closing another node");
        cluster().ensureAtMostNumNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForNodes(">=1").setWaitForActiveShards(2).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(2));
    }

    @Test
    public void testAutoExpandNumberReplicas2() {
        logger.info("--> creating index test with auto expand replicas set to 0-2");
        prepareCreate("test", 3, settingsBuilder().put("index.number_of_shards", 2).put("auto_expand_replicas", "0-2")).execute().actionGet();

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(6).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(6));

        logger.info("--> add two more nodes");
        allowNodes("test", 4);
        allowNodes("test", 5);

        logger.info("--> update the auto expand replicas to 0-3");
        client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("auto_expand_replicas", "0-3")).execute().actionGet();

        logger.info("--> running cluster health");
        clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().setWaitForActiveShards(8).execute().actionGet();
        logger.info("--> done cluster health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(3));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(8));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.settings;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UpdateNumberOfReplicasIT extends ESIntegTestCase {
    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    public void testSimpleUpdateNumberOfReplicas() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test", 2));
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .get();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());

        NumShards numShards = getNumShards("test");

        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(numShards.numReplicas));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.totalNumShards));

        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("value", "test" + i).endObject())
                .get();
        }

        refresh();

        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 10L);
        }

        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        logger.info("Increasing the number of replicas from 1 to 2");
        setReplicaCount(2, "test");
        logger.info("Running Cluster Health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .get();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        // only 2 copies allocated (1 replica) across 2 nodes
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        final long afterReplicaIncreaseSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterReplicaIncreaseSettingsVersion, equalTo(1 + settingsVersion));

        logger.info("starting another node to new replicas will be allocated to it");
        allowNodes("test", 3);

        final long afterStartingAnotherNodeVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();

        logger.info("Running Cluster Health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNodes(">=3")
            .get();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        // all 3 copies allocated across 3 nodes
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 10L);
        }

        logger.info("Decreasing number of replicas from 2 to 0");
        setReplicaCount(0, "test");

        logger.info("Running Cluster Health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNodes(">=3")
            .get();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        // a single copy is allocated (replica set to 0)
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));

        for (int i = 0; i < 10; i++) {
            assertHitCount(prepareSearch().setQuery(matchAllQuery()), 10);
        }

        final long afterReplicaDecreaseSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterReplicaDecreaseSettingsVersion, equalTo(1 + afterStartingAnotherNodeVersion));
    }

    public void testAutoExpandNumberOfReplicas0ToData() throws IOException {
        internalCluster().ensureAtMostNumDataNodes(2);
        logger.info("--> creating index test with auto expand replicas");
        assertAcked(prepareCreate("test", 2, Settings.builder().put("auto_expand_replicas", "0-all")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("test").setWaitForActiveShards(ActiveShardCount.ALL));

            clusterHealth = clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .setWaitForActiveShards(numShards.numPrimaries * 2)
                .get();
            logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
            assertThat(clusterHealth.isTimedOut(), equalTo(false));
            assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
            assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
            assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));
        }

        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();

        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 3)
            .setWaitForNodes(">=3")
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        final long afterAddingOneNodeSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterAddingOneNodeSettingsVersion, equalTo(1 + settingsVersion));

        logger.info("--> closing one node");
        internalCluster().ensureAtMostNumDataNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .setWaitForNodes(">=2")
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        final long afterClosingOneNodeSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterClosingOneNodeSettingsVersion, equalTo(1 + afterAddingOneNodeSettingsVersion));

        logger.info("--> closing another node");
        internalCluster().ensureAtMostNumDataNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(">=1")
            .setWaitForActiveShards(numShards.numPrimaries)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));

        final long afterClosingAnotherNodeSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterClosingAnotherNodeSettingsVersion, equalTo(1 + afterClosingOneNodeSettingsVersion));
    }

    public void testAutoExpandNumberReplicas1ToData() throws IOException {
        logger.info("--> creating index test with auto expand replicas");
        internalCluster().ensureAtMostNumDataNodes(2);
        assertAcked(prepareCreate("test", 2, Settings.builder().put("auto_expand_replicas", "1-all")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("test").setWaitForActiveShards(ActiveShardCount.ALL));

            clusterHealth = clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .setWaitForActiveShards(numShards.numPrimaries * 2)
                .get();
            logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
            assertThat(clusterHealth.isTimedOut(), equalTo(false));
            assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
            assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
            assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));
        }

        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 3)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        final long afterAddingOneNodeSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterAddingOneNodeSettingsVersion, equalTo(1 + settingsVersion));

        logger.info("--> closing one node");
        internalCluster().ensureAtMostNumDataNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(">=2")
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        final long afterClosingOneNodeSettingsVersion = clusterAdmin().prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterClosingOneNodeSettingsVersion, equalTo(1 + afterAddingOneNodeSettingsVersion));

        logger.info("--> closing another node");
        internalCluster().ensureAtMostNumDataNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForNodes(">=1")
            .setWaitForActiveShards(numShards.numPrimaries)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));
    }

    public void testAutoExpandNumberReplicas2() {
        logger.info("--> creating index test with auto expand replicas set to 0-2");
        assertAcked(prepareCreate("test", 3, Settings.builder().put("auto_expand_replicas", "0-2")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 3)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        logger.info("--> add two more nodes");
        allowNodes("test", 4);
        allowNodes("test", 5);

        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        logger.info("--> update the auto expand replicas to 0-3");
        updateIndexSettings(Settings.builder().put("auto_expand_replicas", "0-3"), "test");

        logger.info("--> running cluster health");
        clusterHealth = clusterAdmin().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 4)
            .get();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(3));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 4));

        /*
         * The settings version increases twice, the first time from the settings update increasing auto expand replicas, and the second
         * time from the number of replicas changed by the allocation service.
         */
        assertThat(
            clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion(),
            equalTo(1 + 1 + settingsVersion)
        );
    }

    public void testUpdateWithInvalidNumberOfReplicas() {
        createIndex("test");
        final long settingsVersion = clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion();
        final int value = randomIntBetween(-10, -1);
        try {
            indicesAdmin().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, value))
                .get();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
            assertThat(
                clusterAdmin().prepareState().get().getState().metadata().index("test").getSettingsVersion(),
                equalTo(settingsVersion)
            );
        }
    }

    public void testUpdateNumberOfReplicasAllowNoIndices() {
        createIndex("test-index", Settings.builder().put("index.number_of_replicas", 0).build());
        final IndicesOptions options = IndicesOptions.DEFAULT;
        assertAcked(
            indicesAdmin().prepareUpdateSettings("non-existent-*")
                .setSettings(Settings.builder().put("index.number_of_replicas", 1))
                .setIndicesOptions(options)
        );
        final int numberOfReplicas = Integer.parseInt(
            indicesAdmin().prepareGetSettings("test-index").get().getSetting("test-index", "index.number_of_replicas")
        );
        assertThat(numberOfReplicas, equalTo(0));
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 2)
public class AwarenessAllocationIT extends ESIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AwarenessAllocationIT.class);

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testSimpleAwareness() throws Exception {
        Settings commonSettings = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "rack_id").build();

        logger.info("--> starting 2 nodes on the same rack");
        internalCluster().startNodes(2, Settings.builder().put(commonSettings).put("node.attr.rack_id", "rack_1").build());

        createIndex("test1");
        createIndex("test2");

        NumShards test1 = getNumShards("test1");
        NumShards test2 = getNumShards("test2");
        // no replicas will be allocated as both indices end up on a single node
        final int totalPrimaries = test1.numPrimaries + test2.numPrimaries;

        ensureGreen();

        final List<String> indicesToClose = randomSubsetOf(Arrays.asList("test1", "test2"));
        indicesToClose.forEach(indexToClose -> assertAcked(indicesAdmin().prepareClose(indexToClose).get()));

        logger.info("--> starting 1 node on a different rack");
        final String node3 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.rack_id", "rack_2").build());

        // On slow machines the initial relocation might be delayed
        assertBusy(() -> {
            logger.info("--> waiting for no relocation");
            ClusterHealthResponse clusterHealth = clusterAdmin().prepareHealth()
                .setIndices("test1", "test2")
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .setWaitForNodes("3")
                .setWaitForNoRelocatingShards(true)
                .get();

            assertThat("Cluster health request timed out", clusterHealth.isTimedOut(), equalTo(false));

            logger.info("--> checking current state");
            ClusterState clusterState = clusterAdmin().prepareState().get().getState();

            // check that closed indices are effectively closed
            final List<String> notClosedIndices = indicesToClose.stream()
                .filter(index -> clusterState.metadata().index(index).getState() != State.CLOSE)
                .toList();
            assertThat("Some indices not closed", notClosedIndices, empty());

            // verify that we have all the primaries on node3
            Map<String, Integer> counts = computeShardCounts(clusterState);
            assertThat(counts.get(node3), equalTo(totalPrimaries));
        }, 10, TimeUnit.SECONDS);
    }

    public void testAwarenessZones() {
        Settings commonSettings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        logger.info("--> starting 4 nodes on different zones");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        String B_1 = nodes.get(2);
        String A_1 = nodes.get(3);

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = clusterAdmin().prepareHealth().setWaitForNodes("4").get();
        assertThat(health.isTimedOut(), equalTo(false));

        createIndex("test", 5, 1);

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("test"));
        }

        logger.info("--> waiting for shards to be allocated");
        health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNoRelocatingShards(true)
            .get();
        assertThat(health.isTimedOut(), equalTo(false));

        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        Map<String, Integer> counts = computeShardCounts(clusterState);

        assertThat(counts.get(A_1), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(B_1), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(A_0), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(B_0), anyOf(equalTo(2), equalTo(3)));
    }

    public void testAwarenessZonesIncrementalNodes() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .build();

        logger.info("--> starting 2 nodes on zones 'a' & 'b'");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);

        createIndex("test", 5, 1);

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("test"));
        }

        ClusterHealthResponse health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("2")
            .setWaitForNoRelocatingShards(true)
            .get();
        assertThat(health.isTimedOut(), equalTo(false));
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        Map<String, Integer> counts = computeShardCounts(clusterState);

        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(5));
        logger.info("--> starting another node in zone 'b'");

        String B_1 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("3")
            .get();
        assertThat(health.isTimedOut(), equalTo(false));
        clusterAdmin().prepareReroute().get();
        health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("3")
            .setWaitForActiveShards(10)
            .setWaitForNoRelocatingShards(true)
            .get();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = clusterAdmin().prepareState().get().getState();
        counts = computeShardCounts(clusterState);

        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));

        String noZoneNode = internalCluster().startNode();
        health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("4")
            .get();
        assertThat(health.isTimedOut(), equalTo(false));
        clusterAdmin().prepareReroute().get();
        health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("4")
            .setWaitForActiveShards(10)
            .setWaitForNoRelocatingShards(true)
            .get();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = clusterAdmin().prepareState().get().getState();
        counts = computeShardCounts(clusterState);

        assertThat(counts.get(A_0), equalTo(5));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.containsKey(noZoneNode), equalTo(false));
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.awareness.attributes", ""));
        health = clusterAdmin().prepareHealth()
            .setIndices("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes("4")
            .setWaitForActiveShards(10)
            .setWaitForNoRelocatingShards(true)
            .get();

        assertThat(health.isTimedOut(), equalTo(false));
        clusterState = clusterAdmin().prepareState().get().getState();
        counts = computeShardCounts(clusterState);

        assertThat(counts.get(A_0), equalTo(3));
        assertThat(counts.get(B_0), equalTo(3));
        assertThat(counts.get(B_1), equalTo(2));
        assertThat(counts.get(noZoneNode), equalTo(2));
    }

    public void testForceAwarenessSettingValidation() {
        final String prefix = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey();

        final IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(prefix + "nonsense", "foo"))
        );
        assertThat(illegalArgumentException.getMessage(), containsString("[cluster.routing.allocation.awareness.force.]"));
        assertThat(illegalArgumentException.getCause(), instanceOf(SettingsException.class));
        assertThat(illegalArgumentException.getCause().getMessage(), containsString("nonsense"));

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                clusterAdmin().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(prefix + "attr.not_values", "foo"))
            ).getMessage(),
            containsString("[cluster.routing.allocation.awareness.force.attr.not_values]")
        );

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                clusterAdmin().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(prefix + "attr.values.junk", "foo"))
            ).getMessage(),
            containsString("[cluster.routing.allocation.awareness.force.attr.values.junk]")
        );
    }

    Map<String, Integer> computeShardCounts(ClusterState clusterState) {
        Map<String, Integer> counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    counts.merge(clusterState.nodes().get(indexShardRoutingTable.shard(copy).currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }
        return counts;
    }
}

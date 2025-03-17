/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.collector.shards;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.BaseCollectorTestCase;
import org.elasticsearch.xpack.monitoring.collector.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.xpack.monitoring.MonitoringTestUtils.randomMonitoringNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardsCollectorTests extends BaseCollectorTestCase {

    /** Used to match no indices when collecting shards information **/
    private static final String[] NONE = new String[] { "_none" };

    public void testShouldCollectReturnsFalseIfNotMaster() {
        // this controls the blockage
        whenLocalNodeElectedMaster(false);

        final ShardsCollector collector = new ShardsCollector(clusterService, licenseState);

        assertThat(collector.shouldCollect(false), is(false));
    }

    public void testShouldCollectReturnsTrue() {
        whenLocalNodeElectedMaster(true);

        final ShardsCollector collector = new ShardsCollector(clusterService, licenseState);

        assertThat(collector.shouldCollect(true), is(true));
    }

    public void testDoCollectWhenNoClusterState() throws Exception {
        final ShardsCollector collector = new ShardsCollector(clusterService, licenseState);

        final Collection<MonitoringDoc> results = collector.doCollect(randomMonitoringNode(random()), randomNonNegativeLong(), null);
        assertThat(results, notNullValue());
        assertThat(results.size(), equalTo(0));
    }

    public void testDoCollect() throws Exception {
        final String clusterName = randomAlphaOfLength(10);
        whenClusterStateWithName(clusterName);

        final String clusterUUID = UUID.randomUUID().toString();
        whenClusterStateWithUUID(clusterUUID);

        final String stateUUID = UUID.randomUUID().toString();
        when(clusterState.stateUUID()).thenReturn(stateUUID);

        final String[] indices = randomFrom(
            NONE,
            Strings.EMPTY_ARRAY,
            new String[] { "_all" },
            new String[] { "_index*", "_does-not-exist" }
        );
        withCollectionIndices(indices);

        final RoutingTable routingTable = mockRoutingTable();
        when(clusterState.routingTable()).thenReturn(routingTable);

        final DiscoveryNode localNode = localNode("_current");
        final MonitoringDoc.Node node = Collector.convertNode(randomNonNegativeLong(), localNode);
        when(nodes.get(eq("_current"))).thenReturn(localNode);
        when(clusterState.getNodes()).thenReturn(nodes);

        final ShardsCollector collector = new ShardsCollector(clusterService, licenseState);
        assertNull(collector.getCollectionTimeout());
        assertArrayEquals(indices, collector.getCollectionIndices());

        final long interval = randomNonNegativeLong();

        final Collection<MonitoringDoc> results = collector.doCollect(node, interval, clusterState);
        verify(clusterState).metadata();
        verify(metadata).clusterUUID();

        assertThat(results, notNullValue());
        assertThat(results.size(), equalTo((indices != NONE) ? (int) routingTable.allShards().count() : 0));

        for (MonitoringDoc monitoringDoc : results) {
            assertNotNull(monitoringDoc);
            assertThat(monitoringDoc, instanceOf(ShardMonitoringDoc.class));

            final ShardMonitoringDoc document = (ShardMonitoringDoc) monitoringDoc;
            assertThat(document.getCluster(), equalTo(clusterUUID));
            assertThat(document.getTimestamp(), greaterThan(0L));
            assertThat(document.getIntervalMillis(), equalTo(interval));
            assertThat(document.getSystem(), is(MonitoredSystem.ES));
            assertThat(document.getType(), equalTo(ShardMonitoringDoc.TYPE));
            if (document.getShardRouting().primary()) {
                assertThat(document.getId(), equalTo(ShardMonitoringDoc.id(stateUUID, document.getShardRouting(), 0)));
            } else {
                assertThat(document.getId(), matchesPattern("^[\\w-]+:(_current|_na)+:_index:s\\d:r[1-9]+[0-9]*"));
            }
            assertThat(document.getClusterStateUUID(), equalTo(stateUUID));

            if (document.getShardRouting().assignedToNode()) {
                assertThat(document.getNode(), equalTo(node));
            } else {
                assertThat(document.getNode(), nullValue());
            }

        }

        assertWarnings(
            "[xpack.monitoring.collection.indices] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the deprecation documentation for the next major version."
        );
    }

    private static RoutingTable mockRoutingTable() {
        final List<ShardRouting> allShards = new ArrayList<>();

        final int numberOfPrimaryShards = randomIntBetween(0, 10);
        for (int i = 0; i < numberOfPrimaryShards; i++) {
            ShardRoutingState state = randomFrom(STARTED, UNASSIGNED);
            ShardId shardId = new ShardId("_index", randomAlphaOfLength(12), i);
            ShardRouting primary = TestShardRouting.newShardRouting(shardId, state == STARTED ? "_current" : null, true, state);
            allShards.add(primary);
        }

        final int numberOfReplicaShards = randomIntBetween(0, 3);
        for (int i = 0; i < numberOfPrimaryShards; i++) {
            for (int j = 0; j < numberOfReplicaShards; j++) {
                ShardRoutingState state = randomFrom(STARTED, UNASSIGNED);
                ShardId shardId = new ShardId("_index", randomAlphaOfLength(12), i);
                ShardRouting replica = TestShardRouting.newShardRouting(shardId, state == STARTED ? "_current" : null, false, state);
                allShards.add(replica);
            }
        }

        final RoutingTable routingTable = mock(RoutingTable.class);
        // _index* matches the test data above
        when(routingTable.allShards("_index*")).thenReturn(allShards);
        // _all is reserved to mean all indices in the routing table
        when(routingTable.allShards("_all")).thenReturn(allShards);
        // When collection indices is set to [], it's treated the same as "_all", so the key set of the routing table is used to grab the
        // index names
        when(routingTable.allShards("_index")).thenReturn(allShards);
        // This mock routing table only has the index named "_index", so if collection indices is set to ["_none"] no shards should be
        // found.
        when(routingTable.allShards("_none")).thenReturn(new ArrayList<>(0));

        final IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        final Map<String, IndexRoutingTable> indicesRouting = Map.of("_index", indexRoutingTable);
        when(routingTable.indicesRouting()).thenReturn(indicesRouting);
        when(routingTable.index("_index")).thenReturn(indexRoutingTable);

        when(indexRoutingTable.size()).thenReturn(numberOfPrimaryShards);
        for (int i = 0; i < numberOfPrimaryShards; i++) {
            final IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
            when(indexRoutingTable.shard(i)).thenReturn(shardRoutingTable);
            when(shardRoutingTable.primaryShard()).thenReturn(allShards.get(i));
            List<ShardRouting> replicas = new ArrayList<>();
            int replicaIndexStart = numberOfPrimaryShards + i * numberOfReplicaShards;
            int replicaIndexEnd = replicaIndexStart + numberOfReplicaShards;
            for (int j = replicaIndexStart; j < replicaIndexEnd; j++) {
                replicas.add(allShards.get(j));
            }
            when(shardRoutingTable.replicaShards()).thenReturn(replicas);
        }

        // This is only used by the test to decide how many shards should be covered
        when(routingTable.allShards()).thenReturn(allShards.stream());

        Collections.shuffle(allShards, new Random(numberOfPrimaryShards));

        return routingTable;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.TargetShards;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Where a query's snapshot reads each shard of its point in time.
 */
public class QuerySnapshotTests extends ESTestCase {

    /**
     * One index with one shard, on a primary and two replicas.
     */
    private final ProjectState project = ClusterStateCreationUtils.state(
        "idx",
        true,
        ShardRoutingState.STARTED,
        ShardRoutingState.STARTED,
        ShardRoutingState.STARTED
    ).projectState();
    private final IndexShardRoutingTable routing = project.routingTable().index("idx").shard(0);
    private final ShardId shard = routing.shardId();

    /**
     * A writable shard's reader is bound to the copy that holds it, so the shard is read there only.
     */
    public void testWritableShardIsReadWhereItsReaderIs() {
        DiscoveryNode holder = copy(1);
        QuerySnapshot snapshot = snapshot(Map.of(shard, reader(holder, null)));
        assertFalse(snapshot.reproducible(shard));
        TargetShards targets = snapshot.targetShards(project);
        assertThat(targets.getShard(shard).remainingNodes(), equalTo(List.of(holder)));
        assertThat("each read adds its own", targets.getShard(shard).aliasFilter(), nullValue());
    }

    /**
     * Any copy can reopen a reader with a searcher id, so the shard is read where the reader is first, then on the others.
     */
    public void testReproducibleShardIsReadOnAnyCopyItsReaderFirst() {
        DiscoveryNode holder = copy(2);
        QuerySnapshot snapshot = snapshot(Map.of(shard, reader(holder, "commit")));
        assertTrue(snapshot.reproducible(shard));
        List<DiscoveryNode> nodes = snapshot.targetShards(project).getShard(shard).remainingNodes();
        assertThat(nodes.getFirst(), equalTo(holder));
        assertThat(nodes, containsInAnyOrder(copy(0), copy(1), copy(2)));
    }

    /**
     * A reproducible shard falls back to initializing copies too, as a search on a point in time does.
     */
    public void testFallsBackToInitializingCopies() {
        ProjectState initializing = ClusterStateCreationUtils.state(
            "idx",
            true,
            ShardRoutingState.STARTED,
            ShardRoutingState.STARTED,
            ShardRoutingState.INITIALIZING
        ).projectState();
        IndexShardRoutingTable copies = initializing.routingTable().index("idx").shard(0);
        DiscoveryNode holder = initializing.cluster().nodes().get(copies.primaryShard().currentNodeId());
        QuerySnapshot snapshot = snapshot(Map.of(copies.shardId(), reader(holder, "commit")));
        List<DiscoveryNode> nodes = snapshot.targetShards(initializing).getShard(copies.shardId()).remainingNodes();
        assertThat(nodes.getFirst(), equalTo(holder));
        assertThat(nodes.size(), equalTo(3));
    }

    /**
     * The counts are those of the point in time, which only covers the shards it targeted.
     */
    public void testCountsAreThoseOfThePointInTime() {
        TargetShards targets = new QuerySnapshot(new BytesArray("pit"), Map.of(shard, reader(copy(0), null)), 4, 3, () -> () -> {})
            .targetShards(project);
        assertThat(targets.totalShards(), equalTo(4));
        assertThat(targets.skippedShards(), equalTo(3));
    }

    /**
     * A shard of an index deleted since the point in time opened has no copy to read, even if its holder is still there.
     */
    public void testShardOfADeletedIndexHasNoCopy() {
        ShardId deleted = new ShardId(new Index("deleted", "uuid"), 0);
        TargetShards targets = snapshot(Map.of(deleted, reader(copy(0), null))).targetShards(project);
        assertThat(targets.getShard(deleted).remainingNodes(), equalTo(List.of()));
    }

    /**
     * The sender takes the nodes out of a shard's list as it tries them, so each caller gets its own lists.
     */
    public void testEachCallerGetsItsOwnTargets() {
        QuerySnapshot snapshot = snapshot(Map.of(shard, reader(copy(0), "commit")));
        snapshot.targetShards(project).getShard(shard).remainingNodes().clear();
        assertThat(snapshot.targetShards(project).getShard(shard).remainingNodes().size(), equalTo(3));
    }

    public void testClosesOnce() {
        QuerySnapshot snapshot = snapshot(Map.of());
        assertTrue(snapshot.markClosed());
        assertFalse(snapshot.markClosed());
    }

    private DiscoveryNode copy(int i) {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int c = 0; c < routing.size(); c++) {
            nodes.add(project.cluster().nodes().get(routing.shard(c).currentNodeId()));
        }
        return nodes.get(i);
    }

    private static SearchContextIdForNode reader(DiscoveryNode node, String searcherId) {
        return new SearchContextIdForNode(null, node.getId(), new ShardSearchContextId("session", randomNonNegativeLong(), searcherId));
    }

    private static QuerySnapshot snapshot(Map<ShardId, SearchContextIdForNode> shards) {
        return new QuerySnapshot(new BytesArray("pit"), shards, shards.size(), 0, () -> () -> {});
    }
}

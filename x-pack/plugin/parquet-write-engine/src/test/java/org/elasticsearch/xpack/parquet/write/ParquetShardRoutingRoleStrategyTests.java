/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The roles are what keep every read away from the primary of a parquet index, so each way a routing table creates copies must give
 * them: a new index, a restore, and replicas that a settings update adds.
 */
public class ParquetShardRoutingRoleStrategyTests extends ESTestCase {

    private final ShardRoutingRoleStrategy strategy = new ParquetShardRoutingRoleStrategy();

    private static IndexMetadata index(String name, boolean parquet, int replicas) {
        final var settings = indexSettings(IndexVersion.current(), 2, replicas);
        if (parquet) {
            settings.put("index.storage_format", "parquet");
        }
        return IndexMetadata.builder(name).settings(settings).build();
    }

    public void testANewParquetIndexHasAnIndexOnlyPrimaryAndSearchReplicas() {
        final IndexMetadata parquet = index("parquet", true, 2);
        final IndexRoutingTable table = new IndexRoutingTable.Builder(strategy, parquet.getIndex()).initializeAsNew(parquet).build();
        for (int shard = 0; shard < table.size(); shard++) {
            final IndexShardRoutingTable copies = table.shard(shard);
            assertThat(copies.primaryShard().role(), equalTo(ShardRouting.Role.INDEX_ONLY));
            assertFalse("a read must never resolve to the primary", copies.primaryShard().isSearchable());
            assertThat(copies.replicaShards(), hasSize(2));
            for (ShardRouting replica : copies.replicaShards()) {
                assertThat(replica.role(), equalTo(ShardRouting.Role.SEARCH_ONLY));
                assertTrue(replica.isSearchable());
                assertFalse(replica.isPromotableToPrimary());
            }
        }
    }

    public void testOtherIndicesKeepDefaultCopies() {
        final IndexMetadata other = index("other", false, 1);
        final IndexRoutingTable table = new IndexRoutingTable.Builder(strategy, other.getIndex()).initializeAsNew(other).build();
        for (int shard = 0; shard < table.size(); shard++) {
            for (int copy = 0; copy < table.shard(shard).size(); copy++) {
                assertThat(table.shard(shard).shard(copy).role(), equalTo(ShardRouting.Role.DEFAULT));
            }
        }
        assertThat(strategy.newReplicaRole(other), equalTo(ShardRouting.Role.DEFAULT));
        assertThat(strategy.newEmptyRole(other, 0), equalTo(ShardRouting.Role.DEFAULT));
    }

    public void testRestoredCopiesGetTheSameRoles() {
        final IndexMetadata parquet = index("parquet", true, 1);
        assertThat(strategy.newRestoredRole(parquet, 0), equalTo(ShardRouting.Role.INDEX_ONLY));
        assertThat(strategy.newRestoredRole(parquet, 1), equalTo(ShardRouting.Role.SEARCH_ONLY));
    }

    public void testReplicasThatASettingsUpdateAddsAreSearchReplicas() {
        final IndexMetadata parquet = index("parquet", true, 0);
        final RoutingTable initial = RoutingTable.builder(strategy).addAsNew(parquet).build();
        final RoutingTable updated = RoutingTable.builder(strategy, initial)
            .updateNumberOfReplicas(2, new String[] { "parquet" }, name -> name.equals("parquet") ? parquet : null)
            .build();
        for (int shard = 0; shard < 2; shard++) {
            final IndexShardRoutingTable copies = updated.index("parquet").shard(shard);
            assertThat(copies.primaryShard().role(), equalTo(ShardRouting.Role.INDEX_ONLY));
            assertThat(copies.replicaShards(), hasSize(2));
            for (ShardRouting replica : copies.replicaShards()) {
                assertThat(replica.role(), equalTo(ShardRouting.Role.SEARCH_ONLY));
            }
        }
    }

    public void testAReplicaOfAnUnknownIndexIsADefaultReplica() {
        // the allocation decider refuses such a replica of a parquet index, so it stays unassigned instead of being promotable
        assertThat(strategy.newReplicaRole(null), equalTo(ShardRouting.Role.DEFAULT));
    }
}

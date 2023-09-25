/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class IndexRoutingTableTests extends ESTestCase {

    public void testAllSearchShardsActive() {
        Index index = new Index(randomIdentifier(), UUIDs.randomBase64UUID());
        // 2 primaries that are search and index
        ShardId p1 = new ShardId(index, 0);
        IndexShardRoutingTable shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(getShard(p1, true, ShardRoutingState.STARTED, ShardRouting.Role.DEFAULT))
        );
        ShardId p2 = new ShardId(index, 1);
        IndexShardRoutingTable shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(getShard(p2, true, ShardRoutingState.STARTED, ShardRouting.Role.DEFAULT))
        );
        IndexRoutingTable indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertTrue(indexRoutingTable.allSearchShardsActive());

        // 2 primaries that are index only
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(getShard(p1, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY))
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(getShard(p2, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY))
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertFalse(indexRoutingTable.allSearchShardsActive());

        // 2 primaries that are index only with replicas that are not all available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertFalse(indexRoutingTable.allSearchShardsActive());

        // 2 primaries that are index only with some replicas that are all available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertTrue(indexRoutingTable.allSearchShardsActive());

        // 2 primaries that are index only with at least 1 replica per primary that is available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, randomFrom(ShardRoutingState.values()), ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertTrue(indexRoutingTable.allSearchShardsActive());
    }

    private ShardRouting getShard(ShardId shardId, boolean isPrimary, ShardRoutingState state, ShardRouting.Role role) {
        return new ShardRouting(
            shardId,
            state == ShardRoutingState.UNASSIGNED ? null : randomIdentifier(),
            state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.STARTED ? null : randomIdentifier(),
            isPrimary,
            state,
            TestShardRouting.buildRecoveryTarget(isPrimary, state),
            TestShardRouting.buildUnassignedInfo(state),
            TestShardRouting.buildRelocationFailureInfo(state),
            TestShardRouting.buildAllocationId(state),
            randomLongBetween(-1, 1024),
            role
        );
    }

}

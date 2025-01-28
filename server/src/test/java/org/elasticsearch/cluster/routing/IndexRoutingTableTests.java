/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.util.List;

import static org.elasticsearch.index.IndexSettings.INDEX_FAST_REFRESH_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexRoutingTableTests extends ESTestCase {

    public void testReadyForSearch() {
        Index index = new Index(randomIdentifier(), UUIDs.randomBase64UUID());
        ClusterState clusterState = mock(ClusterState.class, Mockito.RETURNS_DEEP_STUBS);
        when(clusterState.metadata().index(any(Index.class)).getSettings()).thenReturn(
            Settings.builder().put(INDEX_FAST_REFRESH_SETTING.getKey(), randomBoolean()).build()
        );
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
        assertTrue(indexRoutingTable.readyForSearch(clusterState));

        // 2 primaries that are index only
        shardTable1 = new IndexShardRoutingTable(p1, List.of(getShard(p1, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY)));
        shardTable2 = new IndexShardRoutingTable(p2, List.of(getShard(p2, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY)));
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertFalse(indexRoutingTable.readyForSearch(clusterState));

        // 2 unassigned primaries that are index only
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(getShard(p1, true, ShardRoutingState.UNASSIGNED, ShardRouting.Role.INDEX_ONLY))
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(getShard(p2, true, ShardRoutingState.UNASSIGNED, ShardRouting.Role.INDEX_ONLY))
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertFalse(indexRoutingTable.readyForSearch(clusterState));

        // 2 primaries that are index only with replicas that are not all available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertFalse(indexRoutingTable.readyForSearch(clusterState));

        // 2 primaries that are index only with some replicas that are all available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertTrue(indexRoutingTable.readyForSearch(clusterState));

        // 2 unassigned primaries that are index only with some replicas that are all available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, ShardRoutingState.UNASSIGNED, ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, ShardRoutingState.UNASSIGNED, ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertTrue(indexRoutingTable.readyForSearch(clusterState));

        // 2 primaries that are index only with at least 1 replica per primary that is available
        shardTable1 = new IndexShardRoutingTable(
            p1,
            List.of(
                getShard(p1, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY),
                getShard(p1, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p1, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        shardTable2 = new IndexShardRoutingTable(
            p2,
            List.of(
                getShard(p2, true, ShardRoutingState.STARTED, ShardRouting.Role.INDEX_ONLY),
                getShard(p2, false, ShardRoutingState.UNASSIGNED, ShardRouting.Role.SEARCH_ONLY),
                getShard(p2, false, ShardRoutingState.STARTED, ShardRouting.Role.SEARCH_ONLY)
            )
        );
        indexRoutingTable = new IndexRoutingTable(index, new IndexShardRoutingTable[] { shardTable1, shardTable2 });
        assertTrue(indexRoutingTable.readyForSearch(clusterState));
    }

    private ShardRouting getShard(ShardId shardId, boolean isPrimary, ShardRoutingState state, ShardRouting.Role role) {
        return new ShardRouting(
            shardId,
            state == ShardRoutingState.UNASSIGNED ? null : randomIdentifier(),
            state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.STARTED ? null : randomIdentifier(),
            isPrimary,
            state,
            TestShardRouting.buildRecoverySource(isPrimary, state),
            TestShardRouting.buildUnassignedInfo(state),
            TestShardRouting.buildRelocationFailureInfo(state),
            TestShardRouting.buildAllocationId(state),
            randomLongBetween(-1, 1024),
            role
        );
    }

}

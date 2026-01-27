/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheInfoService;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheInfoService.NodeState;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HasFrozenCacheAllocationDeciderTests extends ESTestCase {

    public void testCanAllocateToNode() {
        final var frozenCacheService = mock(FrozenCacheInfoService.class);
        final var allocation = mock(RoutingAllocation.class);
        final var decider = new HasFrozenCacheAllocationDecider(frozenCacheService);

        final var indexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(indexSettings(IndexVersion.current(), between(1, 10), between(0, 2)))
            .build();

        for (var nodeState : NodeState.values()) {
            when(frozenCacheService.getNodeState(any(DiscoveryNode.class))).thenReturn(nodeState);
            when(allocation.isSimulating()).thenReturn(randomBoolean());
            assertThat(decider.canAllocateToNode(indexMetadata, mock(DiscoveryNode.class), allocation), equalTo(Decision.ALWAYS));
        }

        final var partialSearchableSnapshotIndexMetadata = IndexMetadata.builder(randomIdentifier())
            .settings(
                indexSettings(IndexVersion.current(), between(1, 10), between(0, 2)).put(INDEX_STORE_TYPE_SETTING.getKey(), "snapshot")
                    .put(SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, true)
            )
            .build();

        for (var nodeState : NodeState.values()) {
            when(frozenCacheService.getNodeState(any(DiscoveryNode.class))).thenReturn(nodeState);
            final boolean isSimulating = randomBoolean();
            when(allocation.isSimulating()).thenReturn(isSimulating);
            final Decision decision = decider.canAllocateToNode(
                partialSearchableSnapshotIndexMetadata,
                mock(DiscoveryNode.class),
                allocation
            );
            final Decision.Type expectedType;
            if (nodeState == NodeState.HAS_CACHE) {
                expectedType = Decision.Type.YES;
            } else if (nodeState == NodeState.FETCHING) {
                expectedType = isSimulating ? Decision.Type.NO : Decision.Type.THROTTLE;
            } else {
                expectedType = Decision.Type.NO;
            }
            assertThat("incorrect decision for " + nodeState, decision.type(), is(expectedType));
        }
    }

}

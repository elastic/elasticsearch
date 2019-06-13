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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfoTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class MetaDataDeleteIndexServiceTests extends ESTestCase {
    private final AllocationService allocationService = mock(AllocationService.class);
    private final MetaDataDeleteIndexService service = new MetaDataDeleteIndexService(Settings.EMPTY, null, allocationService);

    public void testDeleteMissing() {
        Index index = new Index("missing", "doesn't matter");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.deleteIndices(state, singleton(index)));
        assertEquals(index, e.getIndex());
    }

    public void testDeleteSnapshotting() {
        String index = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = new SnapshotsInProgress(new SnapshotsInProgress.Entry(snapshot, true, false,
                SnapshotsInProgress.State.INIT, singletonList(new IndexId(index, "doesn't matter")),
                System.currentTimeMillis(), (long) randomIntBetween(0, 1000), ImmutableOpenMap.of(),
                SnapshotInfoTests.randomUserMetadata()));
        ClusterState state = ClusterState.builder(clusterState(index))
                .putCustom(SnapshotsInProgress.TYPE, snaps)
                .build();
        Exception e = expectThrows(SnapshotInProgressException.class,
                () -> service.deleteIndices(state, singleton(state.metaData().getIndices().get(index).getIndex())));
        assertEquals("Cannot delete indices that are being snapshotted: [[" + index + "]]. Try again after snapshot finishes "
                + "or cancel the currently running snapshot.", e.getMessage());
    }

    public void testDeleteUnassigned() {
        // Create an unassigned index
        String index = randomAlphaOfLength(5);
        ClusterState before = clusterState(index);

        // Mock the built reroute
        when(allocationService.reroute(any(ClusterState.class), any(String.class))).then(i -> i.getArguments()[0]);

        // Remove it
        ClusterState after = service.deleteIndices(before, singleton(before.metaData().getIndices().get(index).getIndex()));

        // It is gone
        assertNull(after.metaData().getIndices().get(index));
        assertNull(after.routingTable().index(index));
        assertNull(after.blocks().indices().get(index));

        // Make sure we actually attempted to reroute
        verify(allocationService).reroute(any(ClusterState.class), any(String.class));
    }

    private ClusterState clusterState(String index) {
        IndexMetaData indexMetaData = IndexMetaData.builder(index)
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        return ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder().put(indexMetaData, false))
                .routingTable(RoutingTable.builder().addAsNew(indexMetaData).build())
                .blocks(ClusterBlocks.builder().addBlocks(indexMetaData))
                .build();
    }
}

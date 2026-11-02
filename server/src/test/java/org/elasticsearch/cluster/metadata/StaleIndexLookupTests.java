/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * After a rename, components that captured the {@link Index} before it hold a stale name. Lookups by {@link Index} must still
 * find the index through its UUID, and must not confuse it with a new index that reuses the old name.
 */
public class StaleIndexLookupTests extends ESTestCase {

    public void testLookupsFallBackToUuidAfterRename() {
        final Index stale = new Index("old-name", UUIDs.randomBase64UUID(random()));
        final Index renamed = new Index("new-name", stale.getUUID());
        final Index reusingOldName = new Index("old-name", UUIDs.randomBase64UUID(random()));

        final IndexMetadata renamedMetadata = indexMetadata(renamed);
        final IndexMetadata reusingMetadata = indexMetadata(reusingOldName);
        final ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(renamedMetadata, false)
            .put(reusingMetadata, false)
            .build();
        final RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(renamedMetadata)
            .addAsNew(reusingMetadata)
            .build();
        final ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(project))
            .putRoutingTable(project.id(), routingTable)
            .build();

        // the stale Index resolves to the renamed index, not to the index that reuses the old name
        assertThat(project.index(stale), sameInstance(renamedMetadata));
        assertThat(project.getIndexSafe(stale), sameInstance(renamedMetadata));
        assertTrue(project.hasIndex(stale));
        assertThat(project.index(reusingOldName), sameInstance(reusingMetadata));
        assertThat(project.index(renamed), sameInstance(renamedMetadata));
        assertThat(project.index("old-name"), sameInstance(reusingMetadata));

        assertThat(state.metadata().lookupProject(stale).orElseThrow().id(), equalTo(project.id()));
        assertThat(state.metadata().projectFor(stale).index(stale), sameInstance(renamedMetadata));

        final IndexRoutingTable renamedRouting = routingTable.index(stale);
        assertThat(renamedRouting.getIndex(), equalTo(renamed));
        assertThat(renamedRouting.getIndex().getName(), equalTo("new-name"));
        assertTrue(routingTable.hasIndex(stale));
        assertThat(routingTable.index(reusingOldName).getIndex().getName(), equalTo("old-name"));
        assertThat(routingTable.shardRoutingTable(new ShardId(stale, 0)).shardId().getIndex().getName(), equalTo("new-name"));

        // an index that no longer exists at all is still not found
        final Index gone = new Index("gone", UUIDs.randomBase64UUID(random()));
        assertNull(project.index(gone));
        assertFalse(project.hasIndex(gone));
        expectThrows(IndexNotFoundException.class, () -> project.getIndexSafe(gone));
        assertNull(routingTable.index(gone));
        assertTrue(state.metadata().lookupProject(gone).isEmpty());
    }

    private static IndexMetadata indexMetadata(Index index) {
        return IndexMetadata.builder(index.getName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}

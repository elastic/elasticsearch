/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * {@link SnapshotsServiceUtils#projectForSnapshot} for an index that was renamed after the snapshot started: the snapshot knows the
 * index by the name it had then, which is also the name of its repository id, so that is the name the finalized snapshot has to use.
 */
public class ProjectForSnapshotTests extends ESTestCase {

    private static IndexMetadata index(String name, String uuid) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), uuid, 1, 0)).build();
    }

    private static SnapshotsInProgress.Entry snapshotOf(boolean includeGlobalState, List<String> dataStreams, Index... indices) {
        final Map<String, IndexId> indexIds = new HashMap<>();
        final Map<ShardId, ShardSnapshotStatus> shards = new HashMap<>();
        for (Index index : indices) {
            indexIds.put(index.getName(), new IndexId(index.getName(), UUIDs.randomBase64UUID()));
            shards.put(new ShardId(index, 0), ShardSnapshotStatus.UNASSIGNED_QUEUED);
        }
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId("snap", UUIDs.randomBase64UUID())),
            includeGlobalState,
            false,
            State.STARTED,
            indexIds,
            dataStreams,
            List.of(),
            0L,
            0L,
            shards,
            null,
            Map.of(),
            IndexVersion.current()
        );
    }

    /** An index nobody renamed is stored as it is. */
    public void testUnchangedIndexIsStoredAsItIs() {
        final IndexMetadata a = index("a", "uuid-a");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(a, false).build();
        final ProjectMetadata view = SnapshotsServiceUtils.projectForSnapshot(
            snapshotOf(randomBoolean(), List.of(), a.getIndex()),
            project
        );
        assertThat(view.index("a"), sameInstance(a));
    }

    /**
     * "a" became "c" and "b" became "a" after the snapshot of "a" and "b" started. The old name "a" denotes the other index now, so a
     * lookup by name would store that index's metadata for the snapshot's "a".
     */
    public void testNameTakenOverByAnotherIndex() {
        final boolean includeGlobalState = randomBoolean();
        final Index startedAsA = new Index("a", "uuid-a");
        final Index startedAsB = new Index("b", "uuid-b");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(index("c", "uuid-a"), false)
            .put(index("a", "uuid-b"), false)
            .put(index("unrelated", "uuid-d"), false)
            .build();

        final ProjectMetadata view = SnapshotsServiceUtils.projectForSnapshot(
            snapshotOf(includeGlobalState, List.of(), startedAsA, startedAsB),
            project
        );

        assertThat(view.index("a").getIndexUUID(), equalTo("uuid-a"));
        assertThat(view.index("b").getIndexUUID(), equalTo("uuid-b"));
        assertThat("the new name is not part of the image", view.index("c"), nullValue());
        assertThat(view.index("unrelated") != null, equalTo(includeGlobalState));
    }

    /** A data stream names its backing indices as the snapshot does, or a restore of the data stream could not find them. */
    public void testDataStreamOfRenamedBackingIndex() {
        final boolean includeGlobalState = randomBoolean();
        final Index first = new Index(".ds-logs-1", "uuid-1");
        final Index second = new Index(".ds-logs-2", "uuid-2");
        final Index firstRenamed = new Index("logs-archive", first.getUUID());
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(index(firstRenamed.getName(), first.getUUID()), false)
            .put(index(second.getName(), second.getUUID()), false)
            .put(DataStreamTestHelper.newInstance("logs", List.of(firstRenamed, second)))
            .build();

        final ProjectMetadata view = SnapshotsServiceUtils.projectForSnapshot(
            snapshotOf(includeGlobalState, List.of("logs"), first, second),
            project
        );

        assertThat(view.dataStreams().get("logs").getIndices(), equalTo(List.of(first, second)));
        assertThat(view.dataStreams().get("logs").getIndices().get(0).getName(), equalTo(first.getName()));
        assertThat(view.index(first.getName()).getIndexUUID(), equalTo(first.getUUID()));
        assertThat(view.index(firstRenamed.getName()), nullValue());
    }
}

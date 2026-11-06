/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void testSnapshotGuardFollowsRename() {
        final Index original = new Index("source", "uuid-original");
        final IndexMetadata renamed = index("renamed", original.getUUID());
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(renamed, false).build();
        final SnapshotsInProgress.Entry snapshot = snapshotOf(false, List.of(), original);
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(snapshot))
            .build();
        assertThat(
            SnapshotsServiceUtils.snapshottingIndicesBySnapshot(state.projectState(ProjectId.DEFAULT), Set.of(renamed.getIndex())),
            equalTo(Map.of(snapshot.snapshot(), Set.of(renamed.getIndex())))
        );
    }

    public void testSnapshotGuardDoesNotBlockReplacement() {
        final Index original = new Index("source", "uuid-original");
        final IndexMetadata renamed = index("renamed", original.getUUID());
        final IndexMetadata replacement = index("source", "uuid-replacement");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(renamed, false).put(replacement, false).build();
        final SnapshotsInProgress.Entry snapshot = snapshotOf(false, List.of(), original);
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(snapshot))
            .build();
        assertTrue(
            SnapshotsServiceUtils.snapshottingIndicesBySnapshot(state.projectState(ProjectId.DEFAULT), Set.of(replacement.getIndex()))
                .isEmpty()
        );
    }

    public void testReevaluatingQueuedSnapshotFollowsRename() {
        final Index original = new Index("source", "uuid-original");
        final IndexMetadata renamed = index("renamed", original.getUUID());
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(renamed, false).build();
        final SnapshotsInProgress.Entry snapshot = snapshotOf(false, List.of(), original);
        final SnapshotsInProgress snapshots = SnapshotsInProgress.EMPTY.withAddedEntry(snapshot);
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putRoutingTable(
                ProjectId.DEFAULT,
                org.elasticsearch.cluster.routing.RoutingTable.builder(
                    org.elasticsearch.cluster.TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
                ).addAsNew(renamed).build()
            )
            .putCustom(SnapshotsInProgress.TYPE, snapshots)
            .build();
        final var assignments = SnapshotsServiceUtils.shards(
            snapshots,
            SnapshotDeletionsInProgress.EMPTY,
            state.projectState(ProjectId.DEFAULT),
            snapshot.indices().values(),
            true,
            RepositoryData.EMPTY,
            "repo",
            false,
            snapshot::indexByName
        );
        assertNotNull("the queued snapshot must retain its original shard UUID", assignments.get(new ShardId(original, 0)));
    }

    public void testDeleteDuringSnapshotIsStillRejectedAfterRename() {
        final Index original = new Index("source", "uuid-original");
        final IndexMetadata renamed = index("renamed", original.getUUID());
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(renamed, false).build();
        final SnapshotsInProgress.Entry snapshot = snapshotOf(false, List.of(), original);
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putRoutingTable(
                ProjectId.DEFAULT,
                org.elasticsearch.cluster.routing.RoutingTable.builder(
                    org.elasticsearch.cluster.TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
                ).addAsNew(renamed).build()
            )
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(snapshot))
            .build();
        expectThrows(
            SnapshotInProgressException.class,
            () -> org.elasticsearch.cluster.metadata.MetadataDeleteIndexService.deleteIndices(
                state.projectState(ProjectId.DEFAULT),
                Set.of(renamed.getIndex()),
                org.elasticsearch.common.settings.Settings.EMPTY
            )
        );
    }

    public void testAliasReusesOriginalSnapshotName() {
        final Index original = new Index("source", "uuid-original");
        final IndexMetadata renamed = IndexMetadata.builder(index("renamed", original.getUUID()))
            .putAlias(AliasMetadata.builder("source"))
            .build();
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT).put(renamed, false).build();
        final ProjectMetadata view = SnapshotsServiceUtils.projectForSnapshot(snapshotOf(false, List.of(), original), project);
        assertThat(view.index("source").getIndexUUID(), equalTo(original.getUUID()));
    }

    /**
     * An alias that was added after the snapshot started and is named like another index of the snapshot, which only has that name in
     * the snapshot's view, would clash with it: "b" was renamed, and an alias "b" now points at the other index.
     */
    public void testAliasWithTheOriginalNameOfAnotherIndex() {
        final Index startedAsA = new Index("a", "uuid-a");
        final Index startedAsB = new Index("b", "uuid-b");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(IndexMetadata.builder(index("a", "uuid-a")).putAlias(AliasMetadata.builder("b")).build(), false)
            .put(index("renamed-b", "uuid-b"), false)
            .build();

        final ProjectMetadata view = SnapshotsServiceUtils.projectForSnapshot(
            snapshotOf(randomBoolean(), List.of(), startedAsA, startedAsB),
            project
        );

        assertThat(view.index("b").getIndexUUID(), equalTo("uuid-b"));
        assertThat(view.index("a").getAliases().containsKey("b"), equalTo(false));
    }

    /** With global state the view also holds the indices the snapshot does not cover, whose aliases can clash as well. */
    public void testAliasOfAnIndexOutsideTheSnapshotWithTheOriginalName() {
        final Index startedAs = new Index("source", "uuid-source");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(index("renamed", "uuid-source"), false)
            .put(IndexMetadata.builder(index("unrelated", "uuid-unrelated")).putAlias(AliasMetadata.builder("source")).build(), false)
            .build();

        final ProjectMetadata view = SnapshotsServiceUtils.projectForSnapshot(snapshotOf(true, List.of(), startedAs), project);

        assertThat(view.index("source").getIndexUUID(), equalTo("uuid-source"));
        assertThat(view.index("unrelated").getAliases().containsKey("source"), equalTo(false));
        assertThat(view.index("renamed"), nullValue());
    }

    /**
     * Only an index whose metadata finalization changed beyond its name counts as adjusted: that is the metadata the repository must
     * not reuse for a later snapshot, which has the same versions but the alias.
     */
    public void testOnlyIndicesWhoseAliasWasLeftOutAreAdjusted() {
        final Index startedAsSource = new Index("source", "uuid-source");
        final Index startedAsOther = new Index("other", "uuid-other");
        final Index startedAsPlain = new Index("plain", "uuid-plain");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(index("renamed", "uuid-source"), false)
            .put(IndexMetadata.builder(index("other", "uuid-other")).putAlias(AliasMetadata.builder("source")).build(), false)
            .put(index("plain", "uuid-plain"), false)
            .build();
        final var snapshot = snapshotOf(randomBoolean(), List.of(), startedAsSource, startedAsOther, startedAsPlain);

        assertThat(SnapshotsServiceUtils.indicesWithAdjustedMetadata(snapshot, project), equalTo(Set.of("other")));
    }

    public void testNoIndexIsAdjustedWithoutARename() {
        final Index plain = new Index("plain", "uuid-plain");
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(IndexMetadata.builder(index("plain", "uuid-plain")).putAlias(AliasMetadata.builder("plain-alias")).build(), false)
            .build();

        assertTrue(SnapshotsServiceUtils.indicesWithAdjustedMetadata(snapshotOf(randomBoolean(), List.of(), plain), project).isEmpty());
    }
}

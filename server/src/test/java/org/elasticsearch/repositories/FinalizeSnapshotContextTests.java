/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class FinalizeSnapshotContextTests extends ESTestCase {

    private static final String REPO = "test-repo";

    public void testEnsureSnapshotInProgress() {
        final var snapshot = new Snapshot(ProjectId.DEFAULT, REPO, new SnapshotId("snap", randomUUID()));
        final var entry = snapshotsInProgressEntry(snapshot);
        final var context = newContext(true, SnapshotInfo.inProgress(entry));

        context.ensureSnapshotInProgress(stateWith(SnapshotsInProgress.EMPTY.withAddedEntry(entry)));

        // another master dropped the snapshot from the cluster state, and a snapshot of the same name may have been created since
        final var other = snapshotsInProgressEntry(new Snapshot(ProjectId.DEFAULT, REPO, new SnapshotId("snap", randomUUID())));
        for (var staleState : List.of(stateWith(SnapshotsInProgress.EMPTY), stateWith(SnapshotsInProgress.EMPTY.withAddedEntry(other)))) {
            final var e = expectThrows(IllegalStateException.class, () -> context.ensureSnapshotInProgress(staleState));
            assertThat(e.getMessage(), containsString("is no longer in the cluster state"));
        }
    }

    public void testEnsureSnapshotInProgressIsNoOpIfNotRequired() {
        final var snapshot = new Snapshot(ProjectId.DEFAULT, REPO, new SnapshotId("snap", randomUUID()));
        final var context = newContext(false, SnapshotInfo.inProgress(snapshotsInProgressEntry(snapshot)));
        context.ensureSnapshotInProgress(stateWith(SnapshotsInProgress.EMPTY));
    }

    private static ClusterState stateWith(SnapshotsInProgress snapshotsInProgress) {
        return ClusterState.builder(ClusterState.EMPTY_STATE).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress).build();
    }

    private static SnapshotsInProgress.Entry snapshotsInProgressEntry(Snapshot snapshot) {
        return SnapshotsInProgress.Entry.snapshot(
            snapshot,
            randomBoolean(),
            randomBoolean(),
            SnapshotsInProgress.State.SUCCESS,
            Map.of(),
            List.of(),
            List.of(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            Map.of(),
            null,
            Map.of(),
            IndexVersion.current()
        );
    }

    private static FinalizeSnapshotContext newContext(boolean requireSnapshotInProgress, SnapshotInfo snapshotInfo) {
        return new FinalizeSnapshotContext(
            false,
            requireSnapshotInProgress,
            FinalizeSnapshotContext.UpdatedShardGenerations.EMPTY,
            RepositoryData.EMPTY_REPO_GEN,
            Metadata.EMPTY_METADATA,
            snapshotInfo,
            IndexVersion.current(),
            ActionListener.noop(),
            () -> {}
        );
    }
}

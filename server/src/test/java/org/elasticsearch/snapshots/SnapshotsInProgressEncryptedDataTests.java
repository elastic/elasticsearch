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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotsInProgressEncryptedDataTests extends ESTestCase {

    public void testWithAddedEntries() {
        final Snapshot first = randomSnapshot();
        final Snapshot second = randomSnapshot();
        final SnapshotEncryptedData firstData = randomEncryptedData();
        final SnapshotEncryptedData secondData = randomEncryptedData();

        assertThat(SnapshotsInProgressEncryptedData.EMPTY.withAddedEntries(Map.of()), sameInstance(SnapshotsInProgressEncryptedData.EMPTY));
        final SnapshotsInProgressEncryptedData custom = SnapshotsInProgressEncryptedData.EMPTY.withAddedEntries(Map.of(first, firstData))
            .withAddedEntries(Map.of(second, secondData));
        assertThat(custom.get(first), equalTo(firstData));
        assertThat(custom.get(second), equalTo(secondData));
    }

    public void testWithAddedEntriesCopiesPassword() {
        final Snapshot snapshot = randomSnapshot();
        final SnapshotEncryptedData requested = randomEncryptedData();
        final String password = requested.password().toString();

        final SnapshotsInProgressEncryptedData custom = SnapshotsInProgressEncryptedData.EMPTY.withAddedEntries(
            Map.of(snapshot, requested)
        );
        requested.password().close();

        assertThat(custom.get(snapshot).password().toString(), equalTo(password));
    }

    public void testRetainRunning() {
        final Snapshot running = randomSnapshot();
        final Snapshot finished = randomSnapshot();
        final SnapshotsInProgressEncryptedData custom = SnapshotsInProgressEncryptedData.EMPTY.withAddedEntries(
            Map.of(running, randomEncryptedData(), finished, randomEncryptedData())
        );

        final SnapshotsInProgressEncryptedData retained = custom.retainRunning(snapshotsInProgress(running));
        assertNotNull(retained.get(running));
        assertNull(retained.get(finished));
        assertThat(retained.retainRunning(snapshotsInProgress(running)), sameInstance(retained));
        assertThat(custom.retainRunning(snapshotsInProgress(running, finished)), sameInstance(custom));
    }

    public void testUpdateWithSnapshotsDropsEntriesOfRemovedSnapshots() {
        final Snapshot running = randomSnapshot();
        final Snapshot removed = randomSnapshot();
        final SnapshotsInProgressEncryptedData custom = SnapshotsInProgressEncryptedData.EMPTY.withAddedEntries(
            Map.of(running, randomEncryptedData(), removed, randomEncryptedData())
        );
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress(running, removed))
            .putCustom(SnapshotsInProgressEncryptedData.TYPE, custom)
            .build();

        final ClusterState updated = SnapshotsServiceUtils.updateWithSnapshots(state, snapshotsInProgress(running), null);
        assertNotNull(SnapshotsInProgressEncryptedData.get(updated).get(running));
        assertNull(SnapshotsInProgressEncryptedData.get(updated).get(removed));

        final ClusterState deletionsOnly = SnapshotsServiceUtils.updateWithSnapshots(state, null, SnapshotDeletionsInProgress.EMPTY);
        assertThat(SnapshotsInProgressEncryptedData.get(deletionsOnly), sameInstance(custom));
    }

    public void testSerialization() throws IOException {
        final SnapshotsInProgressEncryptedData custom = SnapshotsInProgressEncryptedData.EMPTY.withAddedEntries(
            Map.of(randomSnapshot(), randomEncryptedData())
        );
        assertTrue(custom.isPrivate());
        assertThat(copyWriteable(custom, new NamedWriteableRegistry(List.of()), SnapshotsInProgressEncryptedData::new), equalTo(custom));
    }

    private static SnapshotEncryptedData randomEncryptedData() {
        return new SnapshotEncryptedData(
            SnapshotEncryptedData.TYPE_PASSWORD,
            new SecureString(randomAlphaOfLength(20).toCharArray()),
            randomBoolean() ? null : randomAlphaOfLength(5)
        );
    }

    private static Snapshot randomSnapshot() {
        return new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId(randomAlphaOfLength(10), randomUUID()));
    }

    private static SnapshotsInProgress snapshotsInProgress(Snapshot... snapshots) {
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY;
        for (Snapshot snapshot : snapshots) {
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(
                SnapshotsInProgress.startedEntry(
                    snapshot,
                    true,
                    false,
                    Map.of(),
                    List.of(),
                    0L,
                    0L,
                    Map.of(),
                    Map.of(),
                    IndexVersion.current(),
                    List.of()
                )
            );
        }
        return snapshotsInProgress;
    }
}

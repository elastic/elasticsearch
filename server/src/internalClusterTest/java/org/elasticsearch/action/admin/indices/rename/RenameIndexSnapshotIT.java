/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * How a rename interacts with snapshots: refused while the index is being snapshotted or restored (the prototype's current
 * choice, see T6 in docs/internal/atomic-index-rename.md), and a renamed index can be snapshotted and restored under its new name.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RenameIndexSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testRenameRefusedWhileSnapshotting() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndexWithContent("source");

        blockDataNode("repo", dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshot = startFullSnapshot("repo", "snap");
        waitForBlock(dataNode, "repo");
        try {
            final var e = expectThrows(SnapshotInProgressException.class, () -> rename("source", "renamed"));
            assertThat(e.getMessage(), containsString("being snapshotted"));
        } finally {
            unblockNode("repo", dataNode);
        }
        final SnapshotInfo snapshotInfo = assertSuccessful(snapshot);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));

        // once the snapshot is done the rename goes through
        rename("source", "renamed");
        assertTrue(indexExists("renamed"));
        assertFalse(indexExists("source"));
    }

    public void testRenameRefusedWhileRestoring() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndexWithContent("source");
        createFullSnapshot("repo", "snap");
        assertAcked(indicesAdmin().prepareDelete("source"));

        blockNodeOnAnyFiles("repo", dataNode);
        final ActionFuture<RestoreSnapshotResponse> restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setIndices("source")
            .setWaitForCompletion(true)
            .execute();
        waitForBlock(dataNode, "repo");
        try {
            final var e = expectThrows(IllegalStateException.class, () -> rename("source", "renamed"));
            assertThat(e.getMessage(), containsString("being restored"));
        } finally {
            unblockNode("repo", dataNode);
        }
        assertThat(restore.get().getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("source");

        rename("source", "renamed");
        refresh("renamed");
        ElasticsearchAssertions.assertHitCount(prepareSearch("renamed").setSize(0), 1L);
    }

    /**
     * The repository keys indices by name, so the renamed index is a new index to the repository (T6: a full upload rather than
     * an incremental one). It must still snapshot and restore correctly under its new name, next to the snapshot taken under
     * the old one.
     */
    public void testSnapshotAndRestoreRenamedIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("repo", "fs");
        createIndexWithContent("source");
        refresh("source");
        final long docs = docCount("source");
        createFullSnapshot("repo", "snap-before");

        rename("source", "renamed");
        indexDoc("renamed", "extra", "field", "value");
        refresh("renamed");
        final SnapshotInfo after = createFullSnapshot("repo", "snap-after");
        assertThat(after.indices(), equalTo(java.util.List.of("renamed")));

        assertAcked(indicesAdmin().prepareDelete("renamed"));
        final RestoreSnapshotResponse restoreAfter = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap-after")
            .setIndices("renamed")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreAfter.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("renamed");
        ElasticsearchAssertions.assertHitCount(prepareSearch("renamed").setSize(0), docs + 1);

        // the snapshot taken under the old name restores the old name, next to the renamed index, with the old contents
        final RestoreSnapshotResponse restoreBefore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap-before")
            .setIndices("source")
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreBefore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("source");
        ElasticsearchAssertions.assertHitCount(prepareSearch("source").setSize(0), docs);
    }

    /**
     * The repository matches a renamed index to the id it was snapshotted under before by its UUID (D6), so the first snapshot after
     * a rename uploads only what changed, deleting the old-name snapshot keeps the files the new-name snapshot references, and the
     * repository stays consistent (checked by the base class after the test).
     */
    public void testSnapshotAfterRenameIsIncremental() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("repo", "fs");
        createIndex("source", indexSettings(1, 0).build(), false);
        final int docs = randomIntBetween(20, 100);
        for (int i = 0; i < docs; i++) {
            indexDoc("source", String.valueOf(i), "field", "value" + i);
        }
        flushAndRefresh("source");
        createFullSnapshot("repo", "snap-before");

        rename("source", "renamed");
        createFullSnapshot("repo", "snap-after");

        // the same repository id under both names, so the same blob path
        RepositoryData repositoryData = getRepositoryData("repo");
        assertThat(repositoryData.getIndices().get("renamed").getId(), equalTo(repositoryData.getIndices().get("source").getId()));
        // nothing was written between the two snapshots, so the second one uploaded no segment file at all
        final SnapshotStats afterStats = indexSnapshotStats("repo", "snap-after", "renamed");
        assertThat(afterStats.getTotalFileCount(), greaterThan(0));
        assertThat(afterStats.getIncrementalFileCount(), equalTo(0));

        // deleting the old-name snapshot rewrites the shared shard listing but keeps the files the new-name snapshot references
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap-before").get());
        repositoryData = getRepositoryData("repo");
        assertFalse(repositoryData.getIndices().containsKey("source"));
        assertTrue(repositoryData.getIndices().containsKey("renamed"));

        assertAcked(indicesAdmin().prepareDelete("renamed"));
        final RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap-after")
            .setIndices("renamed")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("renamed");
        ElasticsearchAssertions.assertHitCount(prepareSearch("renamed").setSize(0), docs);

        // and the shared listing is intact: the next snapshot reuses the segment files again
        createFullSnapshot("repo", "snap-third");
        final SnapshotStats thirdStats = indexSnapshotStats("repo", "snap-third", "renamed");
        assertThat(thirdStats.getIncrementalFileCount(), lessThan(thirdStats.getTotalFileCount()));
    }

    private SnapshotStats indexSnapshotStats(String repository, String snapshot, String index) {
        final SnapshotStatus status = clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repository)
            .setSnapshots(snapshot)
            .get()
            .getSnapshots()
            .get(0);
        return status.getIndices().get(index).getStats();
    }

    private long docCount(String index) {
        final long[] count = new long[1];
        ElasticsearchAssertions.assertResponse(
            prepareSearch(index).setSize(0),
            response -> count[0] = response.getHits().getTotalHits().value()
        );
        return count[0];
    }

    private void rename(String source, String target) {
        final var response = client().execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(source, target)).actionGet();
        assertTrue(response.isAcknowledged());
    }
}

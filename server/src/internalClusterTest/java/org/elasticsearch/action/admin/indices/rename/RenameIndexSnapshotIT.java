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
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * How a rename interacts with snapshots: a snapshot in flight carries on and is an image of the cluster as it started, a restore in
 * flight carries on under the new name, and a renamed index snapshots and restores under its new name like any other index (T5 in
 * docs/internal/atomic-index-rename.md).
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RenameIndexSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testLaterSnapshotKeepsAliasFilteredFromEarlierSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndex("source", indexSettings(1, 0).build(), false);
        createIndex("other", indexSettings(1, 0).build(), false);
        indexDoc("source", "1", "field", "source");
        indexDoc("other", "1", "field", "other");
        flushAndRefresh("source", "other");
        blockDataNode("repo", dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshot = startFullSnapshot("repo", "during");
        waitForBlock(dataNode, "repo");
        try {
            rename("source", "renamed");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("other", "source"));
        } finally {
            unblockNode("repo", dataNode);
        }
        assertSuccessful(snapshot);
        createFullSnapshot("repo", "after");
        assertTrue(clusterService().state().metadata().getProject().index("other").getAliases().containsKey("source"));
        assertAcked(indicesAdmin().prepareDelete("renamed", "other"));
        final RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "after")
            .setIndices("other")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("other");
        assertTrue(
            "the later snapshot must retain the alias present when it was taken",
            clusterService().state().metadata().getProject().index("other").getAliases().containsKey("source")
        );
    }

    public void testAliasReusesOriginalSnapshotName() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndexWithContent("source");
        flushAndRefresh("source");
        blockDataNode("repo", dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshot = startFullSnapshot("repo", "snap");
        waitForBlock(dataNode, "repo");
        try {
            rename("source", "renamed");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("renamed", "source"));
        } finally {
            unblockNode("repo", dataNode);
        }
        final SnapshotInfo info = snapshot.actionGet(org.elasticsearch.core.TimeValue.timeValueSeconds(30)).getSnapshotInfo();
        assertThat(info.state(), equalTo(SnapshotState.SUCCESS));
    }

    /**
     * A snapshot in flight does not block a rename, and is not changed by it: the entry keeps the name and the repository id the
     * index had when the snapshot started, the shard uploads already running finish under that id, and the snapshot finalizes as an
     * image of the cluster as it started, with the index under its old name.
     */
    public void testRenameWhileSnapshotting() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndexWithContent("source");
        refresh("source");
        final long docs = docCount("source");

        blockDataNode("repo", dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshot = startFullSnapshot("repo", "snap");
        waitForBlock(dataNode, "repo");
        try {
            rename("source", "renamed");
            assertTrue(indexExists("renamed"));
            assertFalse(indexExists("source"));
            final var entry = SnapshotsInProgress.get(clusterService().state()).forRepo("repo").get(0);
            assertThat(entry.indices().keySet(), equalTo(Set.of("source")));
        } finally {
            unblockNode("repo", dataNode);
        }
        final SnapshotInfo snapshotInfo = assertSuccessful(snapshot);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.indices(), equalTo(List.of("source")));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        assertAcked(indicesAdmin().prepareDelete("renamed"));
        final RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("source");
        ElasticsearchAssertions.assertHitCount(prepareSearch("source").setSize(0), docs);
    }

    /**
     * The name of one index is taken over by another while a snapshot of both is in flight: "a" becomes "c" and "b" becomes "a". The
     * snapshot is an image of the cluster as it started, so restoring it gives each name the contents it had then, not the contents of
     * the index that holds the name now.
     */
    public void testNameTakenOverWhileSnapshotting() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndex("a", indexSettings(1, 0).build(), false);
        createIndex("b", indexSettings(1, 0).build(), false);
        for (int i = 0; i < 3; i++) {
            indexDoc("a", "a" + i, "field", "a");
        }
        for (int i = 0; i < 5; i++) {
            indexDoc("b", "b" + i, "field", "b");
        }
        flushAndRefresh("a", "b");

        blockDataNode("repo", dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshot = startFullSnapshot("repo", "snap");
        waitForBlock(dataNode, "repo");
        try {
            final var response = client().execute(
                TransportIndicesAliasesAction.TYPE,
                new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
                    IndicesAliasesRequest.AliasActions.renameIndex().index("a").newIndex("c")
                ).addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index("b").newIndex("a"))
            ).actionGet();
            assertTrue(response.isAcknowledged());
            assertThat(docCount("c"), equalTo(3L));
            assertThat(docCount("a"), equalTo(5L));
        } finally {
            unblockNode("repo", dataNode);
        }
        final SnapshotInfo snapshotInfo = assertSuccessful(snapshot);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
        assertThat(Set.copyOf(snapshotInfo.indices()), equalTo(Set.of("a", "b")));

        assertAcked(indicesAdmin().prepareDelete("a", "c"));
        final RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("a", "b");
        assertThat(docCount("a"), equalTo(3L));
        assertThat(docCount("b"), equalTo(5L));
    }

    /**
     * Like {@link #testRenameWhileSnapshotting}, but the repository already knows the index under its old name from an earlier
     * snapshot, so the repository id the running snapshot writes to is one that the old name owns. The renamed index must not
     * end up under a second name with that id; every snapshot has to stay restorable and the repository consistent (checked by the
     * base class after the test).
     */
    public void testRenameWhileSnapshottingAnIndexTheRepositoryKnows() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndexWithContent("source");
        refresh("source");
        final long docs = docCount("source");
        createFullSnapshot("repo", "snap-before");

        // new segment files, so that the next snapshot has something to upload, and so to block on
        indexDoc("source", "extra", "field", "value");
        flushAndRefresh("source");
        final long docsDuring = docCount("source");

        blockDataNode("repo", dataNode);
        final ActionFuture<CreateSnapshotResponse> snapshot = startFullSnapshot("repo", "snap-during");
        waitForBlock(dataNode, "repo");
        try {
            rename("source", "renamed");
        } finally {
            unblockNode("repo", dataNode);
        }
        assertSuccessful(snapshot);
        createFullSnapshot("repo", "snap-after");

        // the snapshot that was running across the rename knows the index by its old name, like the earlier one, and writes under the
        // id the repository already had for that name; the later one knows it by the new name
        final RepositoryData repositoryData = getRepositoryData("repo");
        assertThat(repositoryData.getIndices().keySet(), equalTo(Set.of("source", "renamed")));
        assertThat(repositoryData.getIndices().get("renamed").getId(), not(equalTo(repositoryData.getIndices().get("source").getId())));
        assertThat(getSnapshot("repo", "snap-during").indices(), equalTo(List.of("source")));
        assertThat(getSnapshot("repo", "snap-after").indices(), equalTo(List.of("renamed")));

        assertAcked(indicesAdmin().prepareDelete("renamed"));
        for (String snapshotName : List.of("snap-before", "snap-during", "snap-after")) {
            final String indexName = getSnapshot("repo", snapshotName).indices().get(0);
            final RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", snapshotName)
                .setWaitForCompletion(true)
                .get();
            assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
            ensureGreen(indexName);
            ElasticsearchAssertions.assertHitCount(
                prepareSearch(indexName).setSize(0),
                snapshotName.equals("snap-before") ? docs : docsDuring
            );
            assertAcked(indicesAdmin().prepareDelete(indexName));
        }
    }

    /**
     * A restore in flight does not block a rename of the index being restored. The restore entry is renamed with the index, so the
     * restore completes and is cleaned up under the new name.
     */
    public void testRenameWhileRestoring() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        createRepository("repo", "mock");
        createIndexWithContent("source");
        refresh("source");
        final long docs = docCount("source");
        createFullSnapshot("repo", "snap");
        assertAcked(indicesAdmin().prepareDelete("source"));

        blockNodeOnAnyFiles("repo", dataNode);
        final ActionFuture<RestoreSnapshotResponse> restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setIndices("source")
            .setWaitForCompletion(true)
            .execute();
        waitForBlock(dataNode, "repo");
        try {
            rename("source", "renamed");
            assertTrue(indexExists("renamed"));
            final var entry = RestoreInProgress.get(clusterService().state()).iterator().next();
            assertThat(entry.indices(), equalTo(List.of("renamed")));
            assertTrue(entry.shards().keySet().stream().allMatch(shardId -> shardId.getIndexName().equals("renamed")));
        } finally {
            unblockNode("repo", dataNode);
        }
        assertThat(restore.get().getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("renamed");
        assertBusy(() -> assertTrue(RestoreInProgress.get(clusterService().state()).isEmpty()));
        ElasticsearchAssertions.assertHitCount(prepareSearch("renamed").setSize(0), docs);
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
        assertThat(after.indices(), equalTo(List.of("renamed")));

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
     * The repository keys indices by name, one repository id per name, so a renamed index gets an id of its own and its first
     * snapshot uploads it in full. Sharing the old name's id would make a later index that reuses the old name collide with it in one
     * snapshot (see {@link #testSnapshotAfterOriginalNameIsReused}). Deleting the old-name snapshot must keep the new one intact.
     */
    public void testSnapshotAfterRenameUploadsUnderANewId() throws Exception {
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

        RepositoryData repositoryData = getRepositoryData("repo");
        assertThat(repositoryData.getIndices().get("renamed").getId(), not(equalTo(repositoryData.getIndices().get("source").getId())));
        final SnapshotStats afterStats = indexSnapshotStats("repo", "snap-after", "renamed");
        assertThat(afterStats.getTotalFileCount(), greaterThan(0));
        assertThat(afterStats.getIncrementalFileCount(), equalTo(afterStats.getTotalFileCount()));

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

        // from then on the renamed index snapshots incrementally like any other
        createFullSnapshot("repo", "snap-third");
        final SnapshotStats thirdStats = indexSnapshotStats("repo", "snap-third", "renamed");
        assertThat(thirdStats.getIncrementalFileCount(), lessThan(thirdStats.getTotalFileCount()));
    }

    /**
     * An index renamed away from a name and a new, unrelated index that takes that name are two indices in one snapshot. Each must
     * restore with its own data, and the repository must stay consistent (checked by the base class after the test).
     */
    public void testSnapshotAfterOriginalNameIsReused() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        createRepository("repo", "fs");
        createIndex("source", indexSettings(1, 0).build(), false);
        indexDoc("source", "original", "field", "original");
        flushAndRefresh("source");
        createFullSnapshot("repo", "snap-before");
        rename("source", "renamed");
        createFullSnapshot("repo", "snap-renamed");
        createIndex("source", indexSettings(1, 0).build(), false);
        indexDoc("source", "replacement-one", "field", "replacement");
        indexDoc("source", "replacement-two", "field", "replacement");
        flushAndRefresh("source", "renamed");
        createFullSnapshot("repo", "snap-both");

        final RepositoryData repositoryData = getRepositoryData("repo");
        assertThat(repositoryData.getIndices().get("source").getId(), not(equalTo(repositoryData.getIndices().get("renamed").getId())));

        assertAcked(indicesAdmin().prepareDelete("source", "renamed"));
        final RestoreSnapshotResponse restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap-both")
            .setWaitForCompletion(true)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen("source", "renamed");
        assertThat(docCount("source"), equalTo(2L));
        assertThat(docCount("renamed"), equalTo(1L));
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

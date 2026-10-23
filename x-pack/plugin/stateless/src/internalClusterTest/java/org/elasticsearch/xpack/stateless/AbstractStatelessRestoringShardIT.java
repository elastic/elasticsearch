/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.ShardRestoringException;
import org.elasticsearch.snapshots.mockstore.MockRepository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.awaitPrimaryInSnapshotRestore;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.blockAllDataNodes;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.unblockAllDataNodes;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.waitForBlockOnAnyDataNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * The set-up shared by the tests of what happens to an API when its index is being restored from a snapshot on a stateless cluster, where
 * the restore asked for its shards to be reported.
 *
 * <p>In stateless a restoring primary is on an index node, which is not searchable, and its search copy is only assigned once the
 * primary is active. A search coordinator therefore has no copy to search for the shard, which is what makes it report the shard as
 * being restored, when the restore asked for that. A stateful cluster routes the search to the initializing primary instead, where it
 * waits, so this cannot be tested there.
 */
public abstract class AbstractStatelessRestoringShardIT extends AbstractStatelessPluginIntegTestCase {

    protected static final String REPO = "test-repo";
    protected static final String SNAPSHOT = "test-snapshot";
    protected static final String RESTORED_INDEX = "restored-index";
    protected static final String HEALTHY_INDEX = "healthy-index";
    protected static final int DOCS = 10;

    /**
     * The base class registers its own repository type named {@code mock}, an object store backed by the file system, which cannot block.
     * The snapshot repository here has to, to hold the restore in flight, so it takes that name instead.
     */
    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    /**
     * Creates an index with documents, snapshots it and deletes it, and creates a second index that stays healthy.
     */
    protected void setUpSnapshotAndHealthyIndex() {
        startMasterOnlyNode();
        startIndexNodes(1);
        startSearchNode();
        createIndex(RESTORED_INDEX, indexSettings(1, 1).build());
        createIndex(HEALTHY_INDEX, indexSettings(1, 1).build());
        ensureGreen(RESTORED_INDEX, HEALTHY_INDEX);
        indexDocs(RESTORED_INDEX, DOCS);
        indexDocs(HEALTHY_INDEX, DOCS);
        flush(RESTORED_INDEX, HEALTHY_INDEX);
        createRepository(REPO, "mock");
        createSnapshot(REPO, SNAPSHOT, List.of(RESTORED_INDEX), List.of());
        assertAcked(indicesAdmin().prepareDelete(RESTORED_INDEX));
    }

    /**
     * Blocks the repository, starts a restore of {@link #RESTORED_INDEX} and waits until its primary is initializing from the snapshot.
     *
     * @return the restore UUID, which the exception carries as its recovery ID
     */
    protected String blockAndStartRestore(boolean reportShardRestoring) {
        blockAllDataNodes(REPO);
        final String restoreUuid = UUIDs.randomBase64UUID();
        final PlainActionFuture<RestoreService.RestoreCompletionResponse> started = new PlainActionFuture<>();
        internalCluster().getCurrentMasterNodeInstance(RestoreService.class)
            .restoreSnapshot(
                ProjectId.DEFAULT,
                new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, REPO, SNAPSHOT).indices(RESTORED_INDEX).waitForCompletion(false),
                restoreUuid,
                reportShardRestoring,
                started,
                (state, builder) -> {}
            );
        awaitPrimaryInSnapshotRestore(RESTORED_INDEX);
        waitForBlockOnAnyDataNode(REPO);
        safeGet(started);
        return restoreUuid;
    }

    protected void finishRestore() throws Exception {
        unblockAllDataNodes(REPO);
        ensureGreen(RESTORED_INDEX);
    }

    protected static ShardRestoringException shardRestoringCause(Exception e) {
        return (ShardRestoringException) ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException)
            .orElseThrow(() -> new AssertionError("no ShardRestoringException in the causes of " + e, e));
    }

    protected static void assertForbiddenShardRestoring(Throwable e, String restoreUuid) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.FORBIDDEN));
        final var restoring = ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof ShardRestoringException);
        assertTrue("no ShardRestoringException in the causes of " + e, restoring.isPresent());
        assertThat(((ShardRestoringException) restoring.get()).recoveryId(), equalTo(restoreUuid));
    }
}

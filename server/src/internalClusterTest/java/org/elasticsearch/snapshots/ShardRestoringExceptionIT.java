/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests verifying that operations against a shard that is being restored from a snapshot
 * return {@link ShardRestoringException} (HTTP 409) rather than the previous 503.
 *
 * <p>Each test blocks a restore in the INITIALIZING stage using {@link MockRepository}, issues the
 * operation under test, and asserts the exception type, restore UUID, and shard identity.
 *
 * <p>Test setup always uses a single-shard, no-replica index. The index is deleted before restore
 * (not closed) to force chunk-blob reads from the repository, which is what {@code blockAllDataNodes}
 * actually blocks — a closed index would skip repo reads and the primary would start too quickly.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class ShardRestoringExceptionIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";
    private static final String DOC_ID = "some_id";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class);
    }

    /**
     * Creates the repository and snapshot once for the entire suite. Each test restores from this
     * snapshot — creating the repo and snapshot per test was the bottleneck that caused suite timeout.
     */
    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        // Delete (not close) so repo reads are required during recovery, which is what
        // blockAllDataNodes actually blocks. Each test restores from this snapshot.
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    /**
     * Blocks the restore at the INITIALIZING stage and returns the restore UUID. The repository
     * and snapshot are created once in {@link #setupSuiteScopeCluster()} and reused across tests.
     *
     * @return the restore UUID from the blocked shard's {@link RecoverySource.SnapshotRecoverySource},
     *         which all {@link ShardRestoringException}s thrown during the restore must carry
     */
    private String setUpBlockedRestore() throws Exception {
        blockAllDataNodes(REPO);
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPO, SNAPSHOT).setIndices(INDEX).setWaitForCompletion(false).execute();
        awaitPrimaryInSnapshotRestore(INDEX);

        ShardRouting primary = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(INDEX)
            .shard(0)
            .primaryShard();
        return ((RecoverySource.SnapshotRecoverySource) primary.recoverySource()).restoreUUID();
    }

    /**
     * Unblocks the repository and deletes the index, cancelling any in-progress restore.
     * We do not wait for the shard to go green — deleting an index that is being restored
     * cancels the restore immediately, so waiting for green would require a full segment
     * download from the blob store and would push the suite past its 20-minute timeout.
     * The repository is not deleted here — it is shared across the suite.
     */
    private void tearDownBlockedRestore() throws Exception {
        unblockAllDataNodes(REPO);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ShardRestoringException assertShardRestoringException(Throwable t) {
        Throwable cause = ExceptionsHelper.unwrapCause(t);
        assertThat("expected ShardRestoringException, got: " + t, cause, instanceOf(ShardRestoringException.class));
        return (ShardRestoringException) cause;
    }

    private static void assertRestoreUuid(ShardRestoringException sre, String expectedRestoreUuid) {
        assertThat(sre.restoreUuid(), notNullValue());
        assertThat(sre.restoreUuid(), equalTo(expectedRestoreUuid));
    }

    // -------------------------------------------------------------------------
    // Read path — Category A: top-level status 503 → 409
    // -------------------------------------------------------------------------

    /**
     * {@code GET /{index}/_doc/{id}} (realtime=true) against a restoring shard returns
     * {@link ShardRestoringException} via {@code TransportGetAction}.
     */
    public void testGetDocWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(ShardRestoringException.class, client().prepareGet(INDEX, DOC_ID));
            assertRestoreUuid(e, expectedRestoreUuid);
            assertThat(e.getIndex().getName(), equalTo(INDEX));
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code GET /{index}/_doc/{id}} with {@code realtime=false} downgrades to a
     * {@code TransportSingleShardAction} read-from-any-copy path and also throws
     * {@link ShardRestoringException} when all copies are restoring.
     */
    public void testGetDocNonRealtimeWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(ShardRestoringException.class, client().prepareGet(INDEX, DOC_ID).setRealtime(false));
            assertRestoreUuid(e, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code GET /{index}/_explain/{id}} returns {@link ShardRestoringException} via
     * {@code TransportSingleShardAction}.
     */
    public void testExplainWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(
                ShardRestoringException.class,
                client().prepareExplain(INDEX, DOC_ID).setQuery(QueryBuilders.matchAllQuery())
            );
            assertRestoreUuid(e, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code GET /{index}/_termvectors/{id}} returns {@link ShardRestoringException} via
     * {@code TransportSingleShardAction}.
     */
    public void testTermVectorsWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(ShardRestoringException.class, client().prepareTermVectors(INDEX, DOC_ID));
            assertRestoreUuid(e, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Write path — Category A
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_doc} against a restoring primary returns {@link ShardRestoringException}
     * immediately — the 409 fires before {@code ?timeout} elapses, unlike the previous 503 which
     * appeared only after the timeout.
     */
    public void testIndexDocWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(
                ShardRestoringException.class,
                client().prepareIndex(INDEX).setSource("field", "value")
            );
            assertRestoreUuid(e, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code DELETE /{index}/_doc/{id}} returns {@link ShardRestoringException} immediately.
     */
    public void testDeleteDocWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(ShardRestoringException.class, client().prepareDelete(INDEX, DOC_ID));
            assertRestoreUuid(e, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code POST /{index}/_update/{id}} returns {@link ShardRestoringException} after
     * {@code ?timeout} elapses. A short timeout is used here so the test completes quickly;
     * in production the default timeout is 1 minute.
     */
    public void testUpdateDocWhileRestoringReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException e = expectThrows(
                ShardRestoringException.class,
                client().prepareUpdate(INDEX, DOC_ID).setDoc("field", "new-value").setTimeout(TimeValue.timeValueMillis(100))
            );
            assertRestoreUuid(e, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Search — Category A (allow_partial_search_results=false)
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_search} with {@code allow_partial_search_results=false} against a
     * fully-restoring index throws {@link SearchPhaseExecutionException} (HTTP 409) with a
     * {@link ShardRestoringException} cause. The restore UUID in the cause cross-references the
     * active restore visible in {@code GET _recovery}.
     */
    public void testSearchAllShardsRestoringWithPartialResultsFalseReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            SearchPhaseExecutionException spee = expectThrows(
                SearchPhaseExecutionException.class,
                client().prepareSearch(INDEX).setAllowPartialSearchResults(false)
            );
            assertThat(spee.status(), equalTo(RestStatus.CONFLICT));
            ShardRestoringException sre = assertShardRestoringException(spee.getCause());
            assertRestoreUuid(sre, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code POST /{index}/_search} with {@code allow_partial_search_results=true} against a
     * fully-restoring index throws {@link SearchPhaseExecutionException} (HTTP 409). When every
     * shard copy is restoring, the "all shards failed" path in
     * {@code AbstractSearchAsyncAction.executeNextPhase} overrides the partial-results leniency
     * and propagates the {@link ShardRestoringException} cause, so the status is 409 rather than 503.
     */
    public void testSearchAllShardsRestoringWithPartialResultsTrueReturnsShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            SearchPhaseExecutionException spee = expectThrows(
                SearchPhaseExecutionException.class,
                client().prepareSearch(INDEX).setAllowPartialSearchResults(true)
            );
            assertThat(spee.status(), equalTo(RestStatus.CONFLICT));
            ShardRestoringException sre = assertShardRestoringException(spee.getCause());
            assertRestoreUuid(sre, expectedRestoreUuid);
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Bulk — Category B: HTTP 200 envelope; per-item 503 → 409
    // -------------------------------------------------------------------------

    /**
     * {@code POST /_bulk} keeps its HTTP 200 envelope but each item routed to a restoring shard
     * has {@code items[].<op>.status == 409} and {@code .error.type == "shard_restoring_exception"}.
     */
    public void testBulkWhileRestoringReturnsPerItemShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            BulkResponse bulk = client().prepareBulk()
                .add(client().prepareIndex(INDEX).setId("new1").setSource("field", "value"))
                .add(client().prepareIndex(INDEX).setId("new2").setSource("field", "value"))
                .get();

            // The envelope is always 200 for _bulk; individual items carry the failure.
            assertThat(bulk.hasFailures(), equalTo(true));
            for (BulkItemResponse item : bulk) {
                assertThat(item.isFailed(), equalTo(true));
                assertThat(item.getFailure().getStatus(), equalTo(RestStatus.CONFLICT));
                ShardRestoringException sre = assertShardRestoringException(item.getFailure().getCause());
                assertRestoreUuid(sre, expectedRestoreUuid);
            }
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Multi-get — Category B
    // -------------------------------------------------------------------------

    /**
     * {@code GET /_mget} keeps its HTTP 200 envelope but each failed item has a nested
     * {@link ShardRestoringException} in {@code docs[].error}.
     */
    public void testMgetWhileRestoringReturnsPerDocShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            MultiGetResponse response = client().prepareMultiGet().add(INDEX, DOC_ID).add(INDEX, "other-id").get();

            // Both docs are on the restoring shard — every item must fail.
            assertThat(response.getResponses().length, greaterThan(0));
            for (var item : response.getResponses()) {
                assertThat(item.isFailed(), equalTo(true));
                ShardRestoringException sre = assertShardRestoringException(item.getFailure().getFailure());
                assertRestoreUuid(sre, expectedRestoreUuid);
            }
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Multi-search — Category B
    // -------------------------------------------------------------------------

    /**
     * {@code GET /_msearch} keeps its HTTP 200 outer envelope but each sub-response for a
     * restoring index has status 409 when {@code allow_partial_search_results=false}.
     */
    public void testMsearchWhileRestoringReturnsPerSubResponseShardRestoringException() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            MultiSearchResponse msearch = client().prepareMultiSearch()
                .add(client().prepareSearch(INDEX).setAllowPartialSearchResults(false))
                .get();
            try {
                for (MultiSearchResponse.Item item : msearch.getResponses()) {
                    assertThat(item.isFailure(), equalTo(true));
                    Exception failure = item.getFailure();
                    assertThat(failure, instanceOf(SearchPhaseExecutionException.class));
                    SearchPhaseExecutionException spee = (SearchPhaseExecutionException) failure;
                    assertThat(spee.status(), equalTo(RestStatus.CONFLICT));
                    ShardRestoringException sre = assertShardRestoringException(spee.getCause());
                    assertRestoreUuid(sre, expectedRestoreUuid);
                }
            } finally {
                msearch.decRef();
            }
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Restore UUID cross-reference: exception UUID == cluster-state recovery source UUID
    // -------------------------------------------------------------------------

    /**
     * The {@code restore_uuid} carried in {@link ShardRestoringException} equals the
     * {@code restoreUUID} from the shard's {@link RecoverySource.SnapshotRecoverySource} in the
     * routing table, which is the same value exposed by {@code GET _recovery} as {@code restoreUUID}.
     * This asserts the cross-reference contract that allows callers to correlate a 409 failure to
     * an observable restore operation.
     */
    public void testRestoreUuidInExceptionMatchesClusterState() throws Exception {
        final String expectedRestoreUuid = setUpBlockedRestore();
        try {
            ShardRestoringException sre = expectThrows(ShardRestoringException.class, client().prepareGet(INDEX, DOC_ID));

            // The UUID from the exception must match the SnapshotRecoverySource in the routing table,
            // which is the same value GET _recovery exposes as restoreUUID.
            assertThat(sre.restoreUuid(), equalTo(expectedRestoreUuid));
        } finally {
            tearDownBlockedRestore();
        }
    }

    // -------------------------------------------------------------------------
    // Post-restore: no exception once shard is STARTED
    // -------------------------------------------------------------------------

    /**
     * Once the restore completes and the shard transitions to STARTED, the same GET that previously
     * returned 409 succeeds. This verifies that {@link ShardRestoringException} is a transient
     * condition specific to the INITIALIZING phase.
     */
    public void testGetDocSucceedsAfterRestoreCompletes() throws Exception {
        setUpBlockedRestore();
        try {
            // Confirm the shard is restoring during the block.
            expectThrows(ShardRestoringException.class, client().prepareGet(INDEX, DOC_ID));
            unblockAllDataNodes(REPO);
            ensureGreen(INDEX);
            // After restore the GET must succeed.
            assertThat(client().prepareGet(INDEX, DOC_ID).get().isExists(), equalTo(true));
        } finally {
            // Delete the index so the next test can restore it fresh. The repo is shared across
            // the suite and must not be deleted here.
            assertAcked(indicesAdmin().prepareDelete(INDEX));
        }
    }

    // -------------------------------------------------------------------------
    // Broadcast ops — Category C: unchanged, _shards.failed stays 0
    // -------------------------------------------------------------------------

    /**
     * {@code POST /{index}/_refresh} silently swallows {@link ShardRestoringException} via
     * {@code isShardNotAvailableException} — identical behaviour to today's
     * {@code NoShardAvailableActionException}. {@code _shards.failed} stays 0.
     */
    public void testRefreshWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        setUpBlockedRestore();
        try {
            assertThat(indicesAdmin().prepareRefresh(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code POST /{index}/_flush} silently swallows {@link ShardRestoringException} —
     * {@code _shards.failed} stays 0.
     */
    public void testFlushWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        setUpBlockedRestore();
        try {
            assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            tearDownBlockedRestore();
        }
    }

    /**
     * {@code POST /{index}/_forcemerge} silently swallows {@link ShardRestoringException} —
     * {@code _shards.failed} stays 0.
     */
    public void testForceMergeWhileRestoringSwallowsExceptionAndReportsNoFailedShards() throws Exception {
        setUpBlockedRestore();
        try {
            assertThat(indicesAdmin().prepareForceMerge(INDEX).get().getFailedShards(), equalTo(0));
        } finally {
            tearDownBlockedRestore();
        }
    }
}

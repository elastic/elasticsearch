/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotRetentionAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.slm.history.SnapshotHistoryStore.SLM_HISTORY_DATA_STREAM;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for Snapshot Lifecycle Management that require a slow or blocked snapshot repo (using {@link MockRepository}
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SLMSnapshotBlockingIntegTests extends AbstractSnapshotIntegTestCase {
    private static final String NEVER_EXECUTE_CRON_SCHEDULE = "* * * 31 FEB ? *";

    static final String REPO = "my-repo";
    List<String> dataNodeNames = null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .build();
    }

    @Before
    public void ensureClusterNodes() {
        logger.info("--> starting enough nodes to ensure we have enough to safely stop for tests");
        internalCluster().startMasterOnlyNodes(2);
        dataNodeNames = internalCluster().startDataOnlyNodes(2);
        ensureGreen();
    }

    @After
    public void cleanUp() throws Exception {
        awaitNoMoreRunningOperations();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockRepository.Plugin.class,
            LocalStateCompositeXPackPlugin.class,
            IndexLifecycle.class,
            DataStreamsPlugin.class
        );
    }

    public void testSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(indexName, i + "", Collections.singletonMap("foo", "bar"));
        }
        createRepository(REPO, "mock");

        logger.info("--> creating policy {}", policyName);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, REPO, indexName, true);

        logger.info("--> blocking master from completing snapshot");
        blockMasterFromFinalizingSnapshotOnIndexFile(REPO);

        logger.info("--> executing snapshot lifecycle");
        final String snapshotName = executePolicy(policyName);

        // Check that the executed snapshot shows up in the SLM output
        assertBusy(() -> {
            GetSnapshotLifecycleAction.Response getResp = client().execute(
                GetSnapshotLifecycleAction.INSTANCE,
                new GetSnapshotLifecycleAction.Request(policyName)
            ).get();
            logger.info("--> checking for in progress snapshot...");

            assertThat(getResp.getPolicies().size(), greaterThan(0));
            SnapshotLifecyclePolicyItem item = getResp.getPolicies().get(0);
            assertNotNull(item.getSnapshotInProgress());
            SnapshotLifecyclePolicyItem.SnapshotInProgress inProgress = item.getSnapshotInProgress();
            assertThat(inProgress.getSnapshotId().getName(), equalTo(snapshotName));
            assertThat(inProgress.getStartTime(), greaterThan(0L));
            assertThat(
                inProgress.getState(),
                anyOf(equalTo(SnapshotsInProgress.State.STARTED), equalTo(SnapshotsInProgress.State.SUCCESS))
            );
            assertNull(inProgress.getFailure());
        });

        logger.info("--> unblocking snapshots");
        unblockNode(REPO, internalCluster().getMasterName());

        // Cancel/delete the snapshot
        try {
            clusterAdmin().prepareDeleteSnapshot(REPO, snapshotName).get();
        } catch (SnapshotMissingException e) {
            // ignore
        }
    }

    public void testRetentionWhileSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyId = "slm-policy";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(indexName, null, Collections.singletonMap("foo", "bar"));
        }
        createRepository(REPO, "mock");

        logger.info("--> creating policy {}", policyId);
        createSnapshotPolicy(
            policyId,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            REPO,
            indexName,
            true,
            false,
            new SnapshotRetentionConfiguration(TimeValue.timeValueSeconds(0), null, null)
        );

        // Create a snapshot and wait for it to be complete (need something that can be deleted)
        final String completedSnapshotName = executePolicy(policyId);
        logger.info("--> kicked off snapshot {}", completedSnapshotName);
        assertBusy(() -> {
            try {
                SnapshotsStatusResponse s = getSnapshotStatus(completedSnapshotName);
                assertThat("expected a snapshot but none were returned", s.getSnapshots().size(), equalTo(1));
                SnapshotStatus status = s.getSnapshots().get(0);
                logger.info("--> waiting for snapshot {} to be completed, got: {}", completedSnapshotName, status.getState());
                assertThat(status.getState(), equalTo(SnapshotsInProgress.State.SUCCESS));
            } catch (SnapshotMissingException e) {
                logger.error("expected a snapshot but it was missing", e);
                fail("expected a snapshot with name " + completedSnapshotName + " but it does not exist");
            }
        });

        awaitNoMoreRunningOperations(randomFrom(dataNodeNames));

        logger.info("--> indexing more docs to force new segment files");
        for (int i = 0; i < docCount; i++) {
            index(indexName, null, Collections.singletonMap("foo", "bar"));
        }
        refresh(indexName);

        try {
            // Take another snapshot, but before doing that, block it from completing
            logger.info("--> blocking data nodes from completing snapshot");
            blockAllDataNodes(REPO);
            logger.info("--> blocked data nodes, executing policy");
            final String secondSnapName = executePolicy(policyId);
            logger.info("--> executed policy, got snapname [{}]", secondSnapName);

            // Check that the executed snapshot shows up in the SLM output as in_progress
            logger.info("--> Waiting for at least one data node to hit the block");
            waitForBlockOnAnyDataNode(REPO);
            assertBusy(() -> {
                logger.info("--> at least one data node has hit the block");
                GetSnapshotLifecycleAction.Response getResp = client().execute(
                    GetSnapshotLifecycleAction.INSTANCE,
                    new GetSnapshotLifecycleAction.Request(policyId)
                ).get();
                logger.info("--> checking for in progress snapshot...");

                assertThat(getResp.getPolicies().size(), greaterThan(0));
                SnapshotLifecyclePolicyItem item = getResp.getPolicies().get(0);
                assertNotNull(item.getSnapshotInProgress());
                SnapshotLifecyclePolicyItem.SnapshotInProgress inProgress = item.getSnapshotInProgress();
                assertThat(inProgress.getSnapshotId().getName(), equalTo(secondSnapName));
                assertThat(inProgress.getStartTime(), greaterThan(0L));
                assertThat(
                    inProgress.getState(),
                    anyOf(equalTo(SnapshotsInProgress.State.INIT), equalTo(SnapshotsInProgress.State.STARTED))
                );
                assertNull(inProgress.getFailure());
            }, 60, TimeUnit.SECONDS);

            // Run retention
            logger.info("--> triggering retention");
            assertTrue(
                client().execute(ExecuteSnapshotRetentionAction.INSTANCE, new ExecuteSnapshotRetentionAction.Request())
                    .get()
                    .isAcknowledged()
            );

            logger.info("--> unblocking snapshots");
            unblockNode(REPO, internalCluster().getMasterName());
            unblockAllDataNodes(REPO);

            // Check that the snapshot created by the policy has been removed by retention
            assertBusy(() -> {
                // Trigger a cluster state update so that it re-checks for a snapshot in progress
                clusterAdmin().prepareReroute().get();
                logger.info("--> waiting for snapshot to be deleted");
                try {
                    SnapshotsStatusResponse s = getSnapshotStatus(completedSnapshotName);
                    assertNull("expected no snapshot but one was returned", s.getSnapshots().get(0));
                } catch (SnapshotMissingException e) {
                    // Great, we wanted it to be deleted!
                }
            });

            // Cancel the ongoing snapshot (or just delete it if it finished)
            assertBusy(() -> {
                try {
                    logger.info("--> cancelling snapshot {}", secondSnapName);
                    clusterAdmin().prepareDeleteSnapshot(REPO, secondSnapName).get();
                } catch (ConcurrentSnapshotExecutionException e) {
                    logger.info("--> attempted to stop second snapshot", e);
                    // just wait and retry
                    fail("attempted to stop second snapshot but a snapshot or delete was in progress");
                }
            });

            // Assert that the history document has been written for taking the snapshot and deleting it
            assertBusy(() -> {
                SearchResponse resp = client().prepareSearch(".slm-history*")
                    .setQuery(QueryBuilders.matchQuery("snapshot_name", completedSnapshotName))
                    .get();
                logger.info(
                    "--> checking history written for {}, got: {}",
                    completedSnapshotName,
                    Strings.arrayToCommaDelimitedString(resp.getHits().getHits())
                );
                assertThat(resp.getHits().getTotalHits().value, equalTo(2L));
            });
        } finally {
            unblockNode(REPO, internalCluster().getMasterName());
            unblockAllDataNodes(REPO);
        }
    }

    public void testBasicFailureRetention() throws Exception {
        testUnsuccessfulSnapshotRetention(false);
    }

    public void testBasicPartialRetention() throws Exception {
        testUnsuccessfulSnapshotRetention(true);
    }

    public void testRetentionWithMultipleRepositories() throws Exception {
        disableRepoConsistencyCheck("test leaves behind an empty repository");
        final String secondRepo = "other-repo";
        createRepository(secondRepo, "fs");
        final String policyId = "some-policy-id";
        createSnapshotPolicy(
            policyId,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            secondRepo,
            "*",
            true,
            true,
            new SnapshotRetentionConfiguration(null, 1, 2)
        );
        logger.info("-->  start snapshot");
        client().execute(ExecuteSnapshotLifecycleAction.INSTANCE, new ExecuteSnapshotLifecycleAction.Request(policyId)).get();
        // make sure the SLM history data stream is green and won't not be green for long because of delayed allocation when data nodes
        // are stopped
        ensureGreen(SLM_HISTORY_DATA_STREAM);
        updateIndexSettings(
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0),
            SLM_HISTORY_DATA_STREAM
        );
        testUnsuccessfulSnapshotRetention(randomBoolean());
    }

    private void testUnsuccessfulSnapshotRetention(boolean partialSuccess) throws Exception {
        final String indexName = "test-idx";
        final String policyId = "test-policy";
        final SnapshotState expectedUnsuccessfulState = partialSuccess ? SnapshotState.PARTIAL : SnapshotState.FAILED;
        // Setup
        createAndPopulateIndex(indexName);
        createRepositoryNoVerify(REPO, "mock");

        createSnapshotPolicy(
            policyId,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            REPO,
            indexName,
            true,
            partialSuccess,
            new SnapshotRetentionConfiguration(null, 1, 2)
        );

        // Create a failed snapshot
        AtomicReference<String> failedSnapshotName = new AtomicReference<>();
        {
            if (partialSuccess) {
                logger.info("-->  stopping random data node, which should cause shards to go missing");
                internalCluster().stopRandomDataNode();
                assertBusy(
                    () -> assertEquals(ClusterHealthStatus.RED, clusterAdmin().prepareHealth().get().getStatus()),
                    30,
                    TimeUnit.SECONDS
                );

                blockMasterFromFinalizingSnapshotOnIndexFile(REPO);

                logger.info("-->  start snapshot");
                ActionFuture<ExecuteSnapshotLifecycleAction.Response> snapshotFuture = client().execute(
                    ExecuteSnapshotLifecycleAction.INSTANCE,
                    new ExecuteSnapshotLifecycleAction.Request(policyId)
                );

                waitForBlock(internalCluster().getMasterName(), REPO);

                logger.info("-->  stopping master node");
                internalCluster().stopCurrentMasterNode();

                logger.info("-->  wait until the snapshot is done");
                failedSnapshotName.set(snapshotFuture.get().getSnapshotName());
                assertNotNull(failedSnapshotName.get());
            } else {
                final String snapshotName = "failed-snapshot-1";
                addBwCFailedSnapshot(REPO, snapshotName, Collections.singletonMap(SnapshotsService.POLICY_ID_METADATA_FIELD, policyId));
                failedSnapshotName.set(snapshotName);
            }

            logger.info("-->  verify that snapshot [{}] is {}", failedSnapshotName.get(), expectedUnsuccessfulState);
            assertBusy(() -> {
                try {
                    GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(REPO)
                        .setSnapshots(failedSnapshotName.get())
                        .get();
                    SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
                    assertEquals(expectedUnsuccessfulState, snapshotInfo.state());
                } catch (SnapshotMissingException ex) {
                    logger.info("failed to find snapshot {}, retrying", failedSnapshotName);
                    throw new AssertionError(ex);
                }
            });
        }

        // Run retention - we'll check the results later to make sure it's had time to run.
        {
            logger.info("--> executing SLM retention");
            assertAcked(client().execute(ExecuteSnapshotRetentionAction.INSTANCE, new ExecuteSnapshotRetentionAction.Request()).get());
        }

        // Take a successful snapshot
        AtomicReference<String> successfulSnapshotName = new AtomicReference<>();
        {
            logger.info("--> deleting old index [{}], as it is now missing shards", indexName);
            assertAcked(indicesAdmin().prepareDelete(indexName).get());
            createAndPopulateIndex(indexName);

            logger.info("--> unblocking snapshots");
            unblockNode(REPO, internalCluster().getMasterName());
            unblockAllDataNodes(REPO);

            logger.info("--> taking new snapshot");

            ActionFuture<ExecuteSnapshotLifecycleAction.Response> snapshotResponse = client().execute(
                ExecuteSnapshotLifecycleAction.INSTANCE,
                new ExecuteSnapshotLifecycleAction.Request(policyId)
            );
            logger.info("--> waiting for snapshot to complete");
            successfulSnapshotName.set(snapshotResponse.get().getSnapshotName());
            assertNotNull(successfulSnapshotName.get());
            logger.info("-->  verify that snapshot [{}] succeeded", successfulSnapshotName.get());
            assertBusy(() -> {
                final SnapshotInfo snapshotInfo;
                try {
                    GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(REPO)
                        .setSnapshots(successfulSnapshotName.get())
                        .execute()
                        .actionGet();
                    snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
                } catch (SnapshotMissingException sme) {
                    throw new AssertionError(sme);
                }
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            }, 30L, TimeUnit.SECONDS);
        }

        // Check that the failed snapshot from before still exists, now that retention has run
        {
            logger.info("-->  verify that snapshot [{}] still exists", failedSnapshotName.get());
            GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(REPO)
                .setSnapshots(failedSnapshotName.get())
                .get();
            SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
            assertEquals(expectedUnsuccessfulState, snapshotInfo.state());
        }

        // Run retention again and make sure the failure was deleted
        {
            logger.info("--> executing SLM retention");
            assertAcked(client().execute(ExecuteSnapshotRetentionAction.INSTANCE, new ExecuteSnapshotRetentionAction.Request()).get());
            logger.info("--> waiting for {} snapshot [{}] to be deleted", expectedUnsuccessfulState, failedSnapshotName.get());
            assertBusy(() -> {
                try {
                    GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(REPO)
                        .setSnapshots(failedSnapshotName.get())
                        .get();
                    assertThat(snapshotsStatusResponse.getSnapshots(), empty());
                } catch (SnapshotMissingException e) {
                    // This is what we want to happen
                }
                logger.info(
                    "--> {} snapshot [{}] has been deleted, checking successful snapshot [{}] still exists",
                    expectedUnsuccessfulState,
                    failedSnapshotName.get(),
                    successfulSnapshotName.get()
                );
                GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(REPO)
                    .setSnapshots(successfulSnapshotName.get())
                    .get();
                SnapshotInfo snapshotInfo = snapshotsStatusResponse.getSnapshots().get(0);
                assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            }, 30L, TimeUnit.SECONDS);
        }
    }

    public void testSLMRetentionAfterRestore() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        indexRandomDocs(indexName, 20);
        createRepository(REPO, "mock");

        logger.info("--> creating policy {}", policyName);
        createSnapshotPolicy(
            policyName,
            "snap",
            NEVER_EXECUTE_CRON_SCHEDULE,
            REPO,
            indexName,
            true,
            false,
            new SnapshotRetentionConfiguration(TimeValue.ZERO, null, null)
        );

        logger.info("--> executing snapshot lifecycle");
        final String snapshotName = executePolicy(policyName);

        // Check that the executed snapshot shows up in the SLM output
        assertBusy(() -> {
            GetSnapshotLifecycleAction.Response getResp = client().execute(
                GetSnapshotLifecycleAction.INSTANCE,
                new GetSnapshotLifecycleAction.Request(policyName)
            ).get();
            logger.info("--> checking for in progress snapshot...");

            assertThat(getResp.getPolicies().size(), greaterThan(0));
            SnapshotLifecyclePolicyItem item = getResp.getPolicies().get(0);
            SnapshotInvocationRecord lastSuccess = item.getLastSuccess();
            assertNotNull(lastSuccess);
            assertThat(lastSuccess.getSnapshotName(), equalTo(snapshotName));
        });

        logger.info("--> restoring index");
        RestoreSnapshotRequest restoreReq = new RestoreSnapshotRequest(REPO, snapshotName);
        restoreReq.indices(indexName);
        restoreReq.renamePattern("(.+)");
        restoreReq.renameReplacement("restored_$1");
        restoreReq.waitForCompletion(true);
        RestoreSnapshotResponse resp = client().execute(RestoreSnapshotAction.INSTANCE, restoreReq).get();
        assertThat(resp.status(), equalTo(RestStatus.OK));

        logger.info("--> executing SLM retention");
        assertAcked(client().execute(ExecuteSnapshotRetentionAction.INSTANCE, new ExecuteSnapshotRetentionAction.Request()).get());
        logger.info("--> waiting for {} snapshot to be deleted", snapshotName);
        assertBusy(() -> {
            try {
                GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(REPO).setSnapshots(snapshotName).get();
                assertThat(snapshotsStatusResponse.getSnapshots(), empty());
            } catch (SnapshotMissingException e) {
                // This is what we want to happen
            }
            logger.info("--> snapshot [{}] has been deleted", snapshotName);
        }, 30L, TimeUnit.SECONDS);
    }

    private SnapshotsStatusResponse getSnapshotStatus(String snapshotName) {
        return clusterAdmin().prepareSnapshotStatus(REPO).setSnapshots(snapshotName).get();
    }

    private void createAndPopulateIndex(String indexName) throws InterruptedException {
        logger.info("--> creating and populating index [{}]", indexName);
        assertAcked(prepareCreate(indexName, 0, indexSettingsNoReplicas(6)));
        ensureGreen();
        indexRandomDocs(indexName, randomIntBetween(50, 100));
    }

    private void createSnapshotPolicy(
        String policyName,
        String snapshotNamePattern,
        String schedule,
        String repoId,
        String indexPattern,
        boolean ignoreUnavailable
    ) {
        createSnapshotPolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            indexPattern,
            ignoreUnavailable,
            false,
            SnapshotRetentionConfiguration.EMPTY
        );
    }

    private void createSnapshotPolicy(
        String policyName,
        String snapshotNamePattern,
        String schedule,
        String repoId,
        String indexPattern,
        boolean ignoreUnavailable,
        boolean partialSnapsAllowed,
        SnapshotRetentionConfiguration retention
    ) {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", ignoreUnavailable);
        snapConfig.put("partial", partialSnapsAllowed);
        if (randomBoolean()) {
            Map<String, Object> metadata = new HashMap<>();
            int fieldCount = randomIntBetween(2, 5);
            for (int i = 0; i < fieldCount; i++) {
                metadata.put(
                    randomValueOtherThanMany(key -> "policy".equals(key) || metadata.containsKey(key), () -> randomAlphaOfLength(5)),
                    randomAlphaOfLength(4)
                );
            }
        }
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            snapConfig,
            retention
        );

        PutSnapshotLifecycleAction.Request putLifecycle = new PutSnapshotLifecycleAction.Request(policyName, policy);
        try {
            client().execute(PutSnapshotLifecycleAction.INSTANCE, putLifecycle).get();
        } catch (Exception e) {
            logger.error("failed to create slm policy", e);
            fail("failed to create policy " + policy + " got: " + e);
        }
    }

    /**
     * Execute the given policy and return the generated snapshot name
     */
    private String executePolicy(String policyId) {
        ExecuteSnapshotLifecycleAction.Request executeReq = new ExecuteSnapshotLifecycleAction.Request(policyId);
        ExecuteSnapshotLifecycleAction.Response resp = null;
        try {
            resp = client().execute(ExecuteSnapshotLifecycleAction.INSTANCE, executeReq).get();
            return resp.getSnapshotName();
        } catch (Exception e) {
            logger.error("failed to execute policy", e);
            fail("failed to execute policy " + policyId + " got: " + e);
            return "bad";
        }
    }
}

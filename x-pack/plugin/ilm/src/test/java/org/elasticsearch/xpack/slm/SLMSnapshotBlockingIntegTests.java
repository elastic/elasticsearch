/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyItem;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for Snapshot Lifecycle Management that require a slow or blocked snapshot repo (using {@link MockRepository}
 */
public class SLMSnapshotBlockingIntegTests extends ESIntegTestCase {

    @After
    public void resetSLMSettings() {
        // unset retention settings
        client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(LifecycleSettings.SLM_RETENTION_SCHEDULE, (String) null)
                .build())
            .get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class, LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    public void testSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(indexName, "_doc", i + "", Collections.singletonMap("foo", "bar"));
        }

        // Create a snapshot repo
        initializeRepo(repoId);

        logger.info("--> creating policy {}", policyName);
        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true);

        logger.info("--> blocking master from completing snapshot");
        blockMasterFromFinalizingSnapshotOnIndexFile(repoId);

        logger.info("--> executing snapshot lifecycle");
        final String snapshotName = executePolicy(policyName);

        // Check that the executed snapshot shows up in the SLM output
        assertBusy(() -> {
            GetSnapshotLifecycleAction.Response getResp =
                client().execute(GetSnapshotLifecycleAction.INSTANCE, new GetSnapshotLifecycleAction.Request(policyName)).get();
            logger.info("--> checking for in progress snapshot...");

            assertThat(getResp.getPolicies().size(), greaterThan(0));
            SnapshotLifecyclePolicyItem item = getResp.getPolicies().get(0);
            assertNotNull(item.getSnapshotInProgress());
            SnapshotLifecyclePolicyItem.SnapshotInProgress inProgress = item.getSnapshotInProgress();
            assertThat(inProgress.getSnapshotId().getName(), equalTo(snapshotName));
            assertThat(inProgress.getStartTime(), greaterThan(0L));
            assertThat(inProgress.getState(), anyOf(equalTo(SnapshotsInProgress.State.INIT), equalTo(SnapshotsInProgress.State.STARTED)));
            assertNull(inProgress.getFailure());
        });

        logger.info("--> unblocking snapshots");
        unblockRepo(repoId);

        // Cancel/delete the snapshot
        try {
            client().admin().cluster().prepareDeleteSnapshot(repoId, snapshotName).get();
        } catch (SnapshotMissingException e) {
            // ignore
        }
    }

    public void testRetentionWhileSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyId = "slm-policy";
        final String repoId = "slm-repo";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(indexName, "_doc", i + "", Collections.singletonMap("foo", "bar"));
        }

        initializeRepo(repoId);

        logger.info("--> creating policy {}", policyId);
        createSnapshotPolicy(policyId, "snap", "1 2 3 4 5 ?", repoId, indexName, true,
            new SnapshotRetentionConfiguration(TimeValue.timeValueSeconds(0), null, null));

        // Create a snapshot and wait for it to be complete (need something that can be deleted)
        final String completedSnapshotName = executePolicy(policyId);
        logger.info("--> kicked off snapshot {}", completedSnapshotName);
        assertBusy(() -> {
            try {
                SnapshotsStatusResponse s =
                    client().admin().cluster().prepareSnapshotStatus(repoId).setSnapshots(completedSnapshotName).get();
                assertThat("expected a snapshot but none were returned", s.getSnapshots().size(), equalTo(1));
                SnapshotStatus status = s.getSnapshots().get(0);
                logger.info("--> waiting for snapshot {} to be completed, got: {}", completedSnapshotName, status.getState());
                assertThat(status.getState(), equalTo(SnapshotsInProgress.State.SUCCESS));
            } catch (SnapshotMissingException e) {
                logger.error("expected a snapshot but it was missing", e);
                fail("expected a snapshot with name " + completedSnapshotName + " but it does not exist");
            }
        });

        // Take another snapshot, but before doing that, block it from completing
        logger.info("--> blocking nodes from completing snapshot");
        blockAllDataNodes(repoId);
        final String secondSnapName = executePolicy(policyId);

        // Check that the executed snapshot shows up in the SLM output as in_progress
        assertBusy(() -> {
            GetSnapshotLifecycleAction.Response getResp =
                client().execute(GetSnapshotLifecycleAction.INSTANCE, new GetSnapshotLifecycleAction.Request(policyId)).get();
            logger.info("--> checking for in progress snapshot...");

            assertThat(getResp.getPolicies().size(), greaterThan(0));
            SnapshotLifecyclePolicyItem item = getResp.getPolicies().get(0);
            assertNotNull(item.getSnapshotInProgress());
            SnapshotLifecyclePolicyItem.SnapshotInProgress inProgress = item.getSnapshotInProgress();
            assertThat(inProgress.getSnapshotId().getName(), equalTo(secondSnapName));
            assertThat(inProgress.getStartTime(), greaterThan(0L));
            assertThat(inProgress.getState(), anyOf(equalTo(SnapshotsInProgress.State.INIT), equalTo(SnapshotsInProgress.State.STARTED)));
            assertNull(inProgress.getFailure());
        });

        // Run retention every second
        client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(LifecycleSettings.SLM_RETENTION_SCHEDULE, "*/1 * * * * ?")
                .build())
            .get();
        // Guarantee that retention gets a chance to run before unblocking, I know sleeps are not
        // ideal, but we don't currently have a way to force retention to run, so waiting at least
        // a second is the best we can do for now.
        Thread.sleep(1500);

        logger.info("--> unblocking snapshots");
        unblockRepo(repoId);
        unblockAllDataNodes(repoId);

        // Check that the snapshot created by the policy has been removed by retention
        assertBusy(() -> {
            // Trigger a cluster state update so that it re-checks for a snapshot in progress
            client().admin().cluster().prepareReroute().get();
            logger.info("--> waiting for snapshot to be deleted");
            try {
                SnapshotsStatusResponse s =
                    client().admin().cluster().prepareSnapshotStatus(repoId).setSnapshots(completedSnapshotName).get();
                assertNull("expected no snapshot but one was returned", s.getSnapshots().get(0));
            } catch (SnapshotMissingException e) {
                // Great, we wanted it to be deleted!
            }
        });

        // Cancel/delete the snapshot
        try {
            client().admin().cluster().prepareDeleteSnapshot(repoId, secondSnapName).get();
        } catch (SnapshotMissingException e) {
            // ignore
        }
    }

    private void initializeRepo(String repoName) {
        client().admin().cluster().preparePutRepository(repoName)
            .setType("mock")
            .setSettings(Settings.builder()
                .put("compress", randomBoolean())
                .put("location", randomAlphaOfLength(6))
                .build())
            .get();
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId,
                                      String indexPattern, boolean ignoreUnavailable) {
        createSnapshotPolicy(policyName, snapshotNamePattern, schedule, repoId, indexPattern,
            ignoreUnavailable, SnapshotRetentionConfiguration.EMPTY);
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId,
                                      String indexPattern, boolean ignoreUnavailable,
                                      SnapshotRetentionConfiguration retention) {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", ignoreUnavailable);
        if (randomBoolean()) {
            Map<String, Object> metadata = new HashMap<>();
            int fieldCount = randomIntBetween(2,5);
            for (int i = 0; i < fieldCount; i++) {
                metadata.put(randomValueOtherThanMany(key -> "policy".equals(key) || metadata.containsKey(key),
                    () -> randomAlphaOfLength(5)), randomAlphaOfLength(4));
            }
        }
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyName, snapshotNamePattern, schedule,
            repoId, snapConfig, retention);

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

    public static String blockMasterFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, masterName)
            .repository(repositoryName)).setBlockOnWriteIndexFile(true);
        return masterName;
    }

    public static String unblockRepo(final String repositoryName) {
        final String masterName = internalCluster().getMasterName();
        ((MockRepository)internalCluster().getInstance(RepositoriesService.class, masterName)
            .repository(repositoryName)).unblock();
        return masterName;
    }

    public static void blockAllDataNodes(String repository) {
        for(RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository)repositoriesService.repository(repository)).blockOnDataFiles(true);
        }
    }

    public static void unblockAllDataNodes(String repository) {
        for(RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository)repositoriesService.repository(repository)).unblock();
        }
    }
}

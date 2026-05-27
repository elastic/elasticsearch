/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSLMStatusAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.StartSLMAction;
import org.elasticsearch.xpack.core.slm.action.StopSLMAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SLMHealthBlockedSnapshotIT extends AbstractSnapshotIntegTestCase {

    // never auto-trigger, instead we will manually trigger in test for better control
    private static final String NEVER_EXECUTE_CRON_SCHEDULE = "* * * 31 FEB ? *";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockRepository.Plugin.class,
            MockTransportService.TestPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            IndexLifecycle.class,
            SnapshotLifecycle.class,
            DataStreamsPlugin.class,
            TestDelayedRepoPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .put(LifecycleSettings.SLM_MINIMUM_INTERVAL, TimeValue.timeValueSeconds(1L))    // use a small value to allow frequent snapshot
            .build();
    }

    public static class TestDelayedRepoPlugin extends Plugin implements RepositoryPlugin {

        // Use static vars since instantiated by plugin system
        private static final AtomicBoolean doDelay = new AtomicBoolean(true);
        private static final CountDownLatch delayedRepoLatch = new CountDownLatch(1);

        static void enable() {
            doDelay.set(true);
        }

        static void disable() {
            doDelay.set(false);
        }

        static void removeDelay() {
            delayedRepoLatch.countDown();
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Map.of(
                TestDelayedRepo.TYPE,
                (projectId, metadata) -> new TestDelayedRepo(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    () -> {
                        if (doDelay.get()) {
                            try {
                                assertTrue(delayedRepoLatch.await(1, TimeUnit.MINUTES));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                )
            );
        }
    }

    static class TestDelayedRepo extends FsRepository {
        private static final String TYPE = "delayed";
        private final Runnable delayFn;

        protected TestDelayedRepo(
            ProjectId projectId,
            RepositoryMetadata metadata,
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            Runnable delayFn
        ) {
            super(projectId, metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            this.delayFn = delayFn;
        }

        @Override
        protected void snapshotFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) throws IOException {
            delayFn.run();
            super.snapshotFile(context, fileInfo);
        }
    }

    public void testSlmHealthYellowWithBlockedSnapshot() throws Exception {
        final String repoName = "test-repo";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createRepository(repoName, TestDelayedRepo.TYPE);

        String idxName = "test-index";
        String policyHealthy = "policy-health";
        String policyHealthyBelowThreshold = "policy-health-below-threshold";
        String policyUnhealthy = "policy-unhealthy";

        List<String> policyNames = List.of(policyHealthy, policyHealthyBelowThreshold, policyUnhealthy);
        List<String> policyNamesUnhealthy = List.of(policyUnhealthy);

        createRandomIndex(idxName);
        putSnapshotPolicy(policyHealthy, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName, null);
        // 1hr unhealthyIfNoSnapshotWithin should not be exceeded during test period, so policy is healthy
        putSnapshotPolicy(policyHealthyBelowThreshold, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName, TimeValue.ONE_HOUR);
        // zero unhealthyIfNoSnapshotWithin will always be exceeded, so policy is always unhealthy
        putSnapshotPolicy(policyUnhealthy, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName, TimeValue.ZERO);

        ensureGreen();

        // allow snapshots to run
        TestDelayedRepoPlugin.disable();

        // create a successful snapshot, so there's baseline time to check against missing snapshot threshold
        List<String> firstSnapshots = executePolicies(masterNode, policyNames);
        waitForSnapshotsAndClusterState(repoName, firstSnapshots);

        // block snapshot execution, create second set of snapshots, assert YELLOW health
        TestDelayedRepoPlugin.enable();
        List<String> secondSnapshots = executePolicies(masterNode, policyNames);
        assertSlmYellowMissingSnapshot(policyNamesUnhealthy);

        // resume snapshot execution
        TestDelayedRepoPlugin.removeDelay();
        waitForSnapshotsAndClusterState(repoName, secondSnapshots);

        // increase policy unhealthy threshold, assert GREEN health
        putSnapshotPolicy(policyUnhealthy, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName, TimeValue.ONE_HOUR);
        assertBusy(() -> {
            GetHealthAction.Request getHealthRequest = new GetHealthAction.Request(true, 1000);
            GetHealthAction.Response health = admin().cluster().execute(GetHealthAction.INSTANCE, getHealthRequest).get();
            assertThat(health.getStatus(), equalTo(HealthStatus.GREEN));
        });
    }

    public void testSlmHealthYellowWhenStoppedWithPolicies() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createRepository("test-repo", "mock");

        final String policyName = "policy-stopped";
        stopSlm();
        putSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, "test-repo", "test-index", null);

        assertSlmYellowWithImpactAndDiagnosis(
            SlmHealthIndicatorService.AUTOMATION_DISABLED_IMPACT_ID,
            SlmHealthIndicatorService.SLM_NOT_RUNNING.definition().id(),
            List.of()
        );

        startSlm();
        assertBusy(() -> {
            GetHealthAction.Response health = admin().cluster()
                .execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000))
                .get();
            HealthIndicatorResult slmIndicator = health.findIndicator(SlmHealthIndicatorService.NAME);
            assertThat(slmIndicator.status(), equalTo(HealthStatus.GREEN));
            assertThat(slmIndicator.symptom(), equalTo("Snapshot Lifecycle Management is running"));
        });
        clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo").get();
    }

    public void testSlmHealthYellowWithRepeatedSnapshotFailures() throws Exception {
        final String repoName = "test-repo";
        final String idxName = "test-index";
        final String policyName = "policy-failures";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final long failedSnapshotWarnThreshold = randomLongBetween(1, 5);
        updateClusterSettings(
            Settings.builder()
                .put(LifecycleSettings.SLM_HEALTH_FAILED_SNAPSHOT_WARN_THRESHOLD_SETTING.getKey(), failedSnapshotWarnThreshold)
        );

        createRepository(repoName, "mock");
        createIndexOnDataNode(idxName, dataNode);
        ensureGreen();

        stopSlm();
        putSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName, null);

        String successfulSnapshot = executePolicy(masterNode, policyName);
        waitForSnapshot(repoName, successfulSnapshot);
        waitForNoSnapshotsInProgress();
        assertInvocationsSinceLastSuccess(policyName, 0L);

        startSlm();
        try {
            for (long expectedInvocations = 1L; expectedInvocations <= failedSnapshotWarnThreshold; expectedInvocations++) {
                assertSlmHealthGreen();
                executePolicyWithSnapshotFailure(masterNode, policyName, repoName, dataNode, expectedInvocations);
            }

            assertSlmYellowWithImpactAndDiagnosis(
                SlmHealthIndicatorService.STALE_SNAPSHOTS_IMPACT_ID,
                SlmHealthIndicatorService.DIAGNOSIS_CHECK_RECENTLY_FAILED_SNAPSHOTS_ID,
                List.of(policyName)
            );
        } finally {
            unblockNode(repoName, dataNode);
            unblockNode(repoName, masterNode);
        }

        deletePartialSnapshots(repoName);
        waitForNoSnapshotsInProgress();
        String recoveredSnapshot = executePolicy(masterNode, policyName);
        waitForSnapshot(repoName, recoveredSnapshot);
        assertBusy(() -> {
            GetHealthAction.Response health = admin().cluster()
                .execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000))
                .get();
            assertThat(health.getStatus(), equalTo(HealthStatus.GREEN));
        });
        assertAcked(clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).get());
    }

    private void deletePartialSnapshots(String repoName) throws Exception {
        for (SnapshotInfo snapshotInfo : clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repoName).get().getSnapshots()) {
            if (snapshotInfo.state() == SnapshotState.PARTIAL || snapshotInfo.state() == SnapshotState.FAILED) {
                assertAcked(
                    clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotInfo.snapshotId().getName()).get()
                );
            }
        }
    }

    private void stopSlm() throws Exception {
        assertAcked(client().execute(StopSLMAction.INSTANCE, new StopSLMAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).get());
        assertBusy(
            () -> assertThat(
                client().execute(GetSLMStatusAction.INSTANCE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
                    .get()
                    .getOperationMode(),
                equalTo(OperationMode.STOPPED)
            )
        );
    }

    private void startSlm() throws Exception {
        assertAcked(
            client().execute(StartSLMAction.INSTANCE, new StartSLMAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)).get()
        );
        assertBusy(
            () -> assertThat(
                client().execute(GetSLMStatusAction.INSTANCE, new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
                    .get()
                    .getOperationMode(),
                equalTo(OperationMode.RUNNING)
            )
        );
    }

    private void createIndexOnDataNode(String idxName, String dataNode) {
        createIndexWithContent(idxName, indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", dataNode).build());
    }

    private void executePolicyWithSnapshotFailure(
        String masterNode,
        String policyName,
        String repoName,
        String dataNode,
        long expectedInvocations
    ) throws Exception {
        MockRepository repository = getRepositoryOnNode(repoName, dataNode);
        repository.setBlockAndFailOnWriteSnapFiles();
        try {
            executePolicy(masterNode, policyName);
            waitForBlock(dataNode, repoName);
            unblockNode(repoName, dataNode);
            assertInvocationsSinceLastSuccess(policyName, expectedInvocations);
            waitForNoSnapshotsInProgress();
        } finally {
            repository.unblock();
            waitForNoSnapshotsInProgress();
        }
    }

    private void createRandomIndex(String idxName) {
        createIndex(idxName);

        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(idxName).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        indicesAdmin().refresh(new RefreshRequest(idxName)).actionGet();
    }

    private void putSnapshotPolicy(
        String policyName,
        String snapshotNamePattern,
        String schedule,
        String repoId,
        String indexPattern,
        TimeValue unhealthyIfNoSnapshotWithin
    ) throws ExecutionException, InterruptedException {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", false);
        snapConfig.put("partial", true);

        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            snapConfig,
            SnapshotRetentionConfiguration.EMPTY,
            unhealthyIfNoSnapshotWithin
        );

        PutSnapshotLifecycleAction.Request putLifecycle = new PutSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyName,
            policy
        );

        client().execute(PutSnapshotLifecycleAction.INSTANCE, putLifecycle).get();
    }

    private void assertSlmHealthGreen() throws Exception {
        assertBusy(() -> {
            GetHealthAction.Response health = admin().cluster()
                .execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000))
                .get();
            assertThat(health.getStatus(), equalTo(HealthStatus.GREEN));
            HealthIndicatorResult slmIndicator = health.findIndicator(SlmHealthIndicatorService.NAME);
            assertThat(slmIndicator.status(), equalTo(HealthStatus.GREEN));
        });
    }

    private void assertSlmYellowMissingSnapshot(List<String> unhealthyPolicies) throws Exception {
        assertSlmYellowWithImpactAndDiagnosis(
            SlmHealthIndicatorService.MISSING_SNAPSHOT_IMPACT_ID,
            SlmHealthIndicatorService.DIAGNOSIS_CONTACT_SUPPORT_ID,
            unhealthyPolicies
        );
    }

    private void assertSlmYellowWithImpactAndDiagnosis(String impactId, String diagnosisId, List<String> unhealthyPolicies)
        throws Exception {
        assertBusy(() -> {
            GetHealthAction.Request getHealthRequest = new GetHealthAction.Request(true, 1000);
            GetHealthAction.Response health = admin().cluster().execute(GetHealthAction.INSTANCE, getHealthRequest).get();
            assertThat(health.getStatus(), equalTo(HealthStatus.YELLOW));
            HealthIndicatorResult slmIndicator = health.findIndicator(SlmHealthIndicatorService.NAME);
            assertThat(slmIndicator.status(), equalTo(HealthStatus.YELLOW));
            assertThat(slmIndicator.impacts().size(), equalTo(1));
            assertThat(slmIndicator.impacts().getFirst().id(), equalTo(impactId));
            List<HealthIndicatorImpact> matchingImpacts = slmIndicator.impacts()
                .stream()
                .filter(impact -> impactId.equals(impact.id()))
                .toList();
            assertThat(matchingImpacts.size(), equalTo(1));

            if (unhealthyPolicies.isEmpty() == false) {
                assertThat(slmIndicator.diagnosisList().size(), equalTo(1));
                Diagnosis diagnosis = slmIndicator.diagnosisList().getFirst();
                assertThat(diagnosis.definition().id(), equalTo(diagnosisId));
                List<Diagnosis.Resource> resources = diagnosis.affectedResources();
                assertThat(resources, notNullValue());
                assertThat(resources.size(), equalTo(1));
                assertThat(resources.getFirst().getValues(), equalTo(unhealthyPolicies));
            } else {
                assertThat(slmIndicator.diagnosisList().size(), equalTo(1));
                assertThat(slmIndicator.diagnosisList().getFirst().definition().id(), equalTo(diagnosisId));
            }
        });
    }

    private void waitForNoSnapshotsInProgress() throws Exception {
        assertBusy(() -> assertTrue(SnapshotsInProgress.get(internalCluster().clusterService().state()).isEmpty()));
    }

    private void assertInvocationsSinceLastSuccess(String policyName, long expectedInvocations) {
        awaitClusterState(state -> {
            SnapshotLifecycleMetadata slmMetadata = state.metadata().getProject(ProjectId.DEFAULT).custom(SnapshotLifecycleMetadata.TYPE);
            if (slmMetadata == null) {
                return false;
            }
            SnapshotLifecyclePolicyMetadata policyMetadata = slmMetadata.getSnapshotConfigurations().get(policyName);
            return policyMetadata != null && policyMetadata.getInvocationsSinceLastSuccess() == expectedInvocations;
        });
    }

    private List<String> executePolicies(String node, List<String> policies) throws Exception {
        List<String> snapshots = new ArrayList<>();
        for (String policyName : policies) {
            snapshots.add(executePolicy(node, policyName));
        }
        return snapshots;
    }

    /**
     * Execute the given policy and return the generated snapshot name
     */
    private String executePolicy(String node, String policyId) throws ExecutionException, InterruptedException {
        ExecuteSnapshotLifecycleAction.Request executeReq = new ExecuteSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyId
        );
        ExecuteSnapshotLifecycleAction.Response resp = client(node).execute(ExecuteSnapshotLifecycleAction.INSTANCE, executeReq).get();
        return resp.getSnapshotName();
    }

    private void waitForSnapshotsAndClusterState(String repo, List<String> snapshots) throws Exception {
        for (String snapshot : snapshots) {
            waitForSnapshot(repo, snapshot);
        }
        assertBusy(() -> assertTrue(SnapshotsInProgress.get(internalCluster().clusterService().state()).isEmpty()));
    }

    private void waitForSnapshot(String repo, String snapshotName) throws Exception {
        assertBusy(() -> {
            try {
                SnapshotsStatusResponse s = getSnapshotStatus(repo, snapshotName);
                assertThat("expected a snapshot but none were returned", s.getSnapshots().size(), equalTo(1));
                SnapshotStatus status = s.getSnapshots().get(0);
                logger.info("--> waiting for snapshot {} to be completed, got: {}", snapshotName, status.getState());
                assertThat(status.getState(), equalTo(SnapshotsInProgress.State.SUCCESS));
            } catch (SnapshotMissingException e) {
                fail("expected a snapshot with name " + snapshotName + " but it does not exist");
            }
        });
    }

    private SnapshotsStatusResponse getSnapshotStatus(String repo, String snapshotName) {
        return clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repo).setSnapshots(snapshotName).get();
    }

}

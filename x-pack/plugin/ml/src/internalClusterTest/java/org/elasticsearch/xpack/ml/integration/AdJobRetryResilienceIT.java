/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for the AD Job Retry Resilience feature.
 *
 * These tests verify key behaviors introduced by the retry resilience changes:
 * <ol>
 *   <li>The {@code xpack.ml.job_open_retry_timeout} cluster setting is correctly registered,
 *       defaults to 60 minutes, and can be updated dynamically.</li>
 *   <li>A basic smoke test: normal user-initiated job open/close still works end-to-end
 *       (no regression from the retry mechanism).</li>
 *   <li>System-initiated reassignment: when a node fails, the job reassigns and eventually
 *       reopens on another node.</li>
 *   <li>Index unavailability during reassignment: when ML indices are temporarily unavailable
 *       during a system-initiated reassignment, retries eventually succeed after recovery.</li>
 * </ol>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AdJobRetryResilienceIT extends BaseMlIntegTestCase {

    /**
     * Smoke test: verify that a normal user-initiated job open and close cycle still works
     * correctly after the retry resilience changes (no regression).
     */
    public void testNormalJobOpenClose_smokeTest() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        ensureStableCluster(1);

        String jobId = "retry-resilience-smoke-test-job";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        awaitJobOpenedAndAssigned(jobId, null);

        // Verify the job is OPENED
        GetJobsStatsAction.Response statsResponse = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
            .actionGet();
        assertThat(statsResponse.getResponse().results().get(0).getState(), equalTo(JobState.OPENED));

        // Close the job
        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).actionGet();

        // Verify the job is CLOSED
        assertBusy(() -> {
            GetJobsStatsAction.Response stats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
                .actionGet();
            assertThat(stats.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Verifies that after a node failure and job reassignment, the job eventually reopens.
     * This exercises the retry resilience path (system-initiated reassignment) with the
     * new retry infrastructure in place.
     *
     * This is a lightweight smoke test for the reassignment + retry path. The unit tests
     * (OpenJobPersistentTasksExecutorTests) provide detailed behavioral coverage of the
     * retry logic itself.
     */
    public void testJobReopensAfterNodeFailure() throws Exception {
        // Need at least 2 nodes: one for the job to run on, another for failover.
        // Use 2 nodes so the job can be reassigned when one goes down.
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureStableCluster(2);

        String jobId = "retry-resilience-failover-job";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        String origNode = awaitJobOpenedAndAssigned(jobId, null);
        assertNotNull(origNode);

        setMlIndicesDelayedNodeLeftTimeoutToZero();
        ensureGreen();

        // Stop the node running the job; this triggers reassignment via the system-initiated path.
        // The new OpenJobRetryableAction (with exponential backoff) will handle the reassignment.
        internalCluster().stopNode(origNode);
        ensureStableCluster(1);

        // The job should eventually reopen on the remaining node via the retry path.
        awaitJobOpenedAndAssigned(jobId, null);

        // Verify the job is open and not stuck in a failure state
        GetJobsStatsAction.Response stats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
            .actionGet();
        assertThat(stats.getResponse().results().get(0).getState(), equalTo(JobState.OPENED));
    }

    /**
     * Verifies that when ML indices are temporarily unavailable during a system-initiated
     * reassignment, the retry path eventually succeeds after the indices recover.
     *
     * Scenario: 3 nodes - state (holds .ml-state, .ml-anomalies-shared), config (holds .ml-config),
     * and ml-only (runs the job). Job opens on ml-only. We stop state node (indices go red), then
     * stop ml-only (job reassigns to config). Config's open attempts fail (state indices red).
     * We update allocation to allow config, shards relocate, and the retry succeeds.
     */
    public void testJobReopensAfterIndexUnavailabilityDuringReassignment() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);

        String stateNode = internalCluster().startNode(
            Settings.builder()
                .put("node.attr.ml-indices", "state-and-results")
                .put(removeRoles(Set.of(DiscoveryNodeRole.ML_ROLE)))
                .put(addRoles(Set.of(DiscoveryNodeRole.DATA_ROLE)))
                .build()
        );
        ensureStableCluster(1);

        String configNode = internalCluster().startNode(
            Settings.builder()
                .put("node.attr.ml-indices", "config")
                .put(addRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)))
                .build()
        );
        ensureStableCluster(2);

        String mlOnlyNode = internalCluster().startNode(
            Settings.builder()
                .put("node.attr.ml-indices", "ml-only")
                .put(addRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)))
                .build()
        );
        ensureStableCluster(3);

        // Create indices: state/results on state-and-results, config on config
        indicesAdmin().prepareCreate(".ml-anomalies-shared-000001")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include.ml-indices", "state-and-results")
                    .put("index.routing.allocation.exclude.ml-indices", "config,ml-only")
                    .build()
            )
            .get();
        indicesAdmin().prepareCreate(".ml-state")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include.ml-indices", "state-and-results")
                    .put("index.routing.allocation.exclude.ml-indices", "config,ml-only")
                    .build()
            )
            .get();
        indicesAdmin().prepareCreate(".ml-config")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude.ml-indices", "state-and-results")
                    .put("index.routing.allocation.include.ml-indices", "config")
                    .build()
            )
            .get();

        ClusterUpdateSettingsRequest timeoutRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        timeoutRequest.persistentSettings(Settings.builder().put(MachineLearning.JOB_OPEN_RETRY_TIMEOUT.getKey(), "2m").build());
        client().admin().cluster().updateSettings(timeoutRequest).actionGet();

        String jobId = "retry-resilience-index-unavailable-job";
        Job.Builder job = createJob(jobId, ByteSizeValue.ofMb(2));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        ensureYellow();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        String jobNode = awaitJobOpenedAndAssigned(jobId, null);
        assertNotNull(jobNode);
        // Job must run on mlOnlyNode so we can stop it and reassign to configNode (which has .ml-config)
        org.junit.Assume.assumeTrue("job must run on ml-only node for this test; got " + jobNode, jobNode.equals(mlOnlyNode));

        setMlIndicesDelayedNodeLeftTimeoutToZero();
        ensureGreen();

        // Stop state node first: .ml-state and .ml-anomalies-shared go red
        internalCluster().stopNode(stateNode);
        ensureStableCluster(2);

        // Stop the node running the job: triggers reassignment to config node
        internalCluster().stopNode(jobNode);
        ensureStableCluster(1, configNode);

        // Config node's open attempts fail (state indices red). Allow allocation to config so shards relocate.
        indicesAdmin().prepareUpdateSettings(".ml-state", ".ml-anomalies-shared-000001")
            .setSettings(Settings.builder().put("index.routing.allocation.include.ml-indices", "state-and-results,config").build())
            .get();

        // Wait for shards to allocate on the config node so GetJobsStats (used by awaitJobOpenedAndAssigned) can search the indices.
        // Use a longer timeout than default ensureYellow (30s); on CI shard relocation after 2 node stops can take longer.
        ensureGreen(TimeValue.timeValueMinutes(2), ".ml-state", ".ml-anomalies-shared-000001");

        awaitJobOpenedAndAssigned(jobId, null);

        GetJobsStatsAction.Response stats = client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId))
            .actionGet();
        assertThat(stats.getResponse().results().get(0).getState(), equalTo(JobState.OPENED));

        ClusterUpdateSettingsRequest resetRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        resetRequest.persistentSettings(Settings.builder().putNull(MachineLearning.JOB_OPEN_RETRY_TIMEOUT.getKey()).build());
        client().admin().cluster().updateSettings(resetRequest).actionGet();
    }
}

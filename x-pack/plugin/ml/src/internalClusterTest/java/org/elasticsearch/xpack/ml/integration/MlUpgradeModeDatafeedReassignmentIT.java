/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Verifies that toggling ML upgrade mode off correctly reassigns many jobs and their datafeeds at once.
 *
 * <p>This exercises the mass-reassignment path through {@link org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector}
 * where, on every cluster-state update during recovery, the master re-evaluates every unassigned datafeed. The
 * datafeed node selector short-circuits the (expensive) index-pattern verification whenever a datafeed's job is not
 * yet (re)opened, so this test guards that the short-circuit does not change the externally observable outcome:
 * while upgrade mode is enabled the jobs and datafeeds become unassigned, and once it is disabled they all reopen,
 * reassign and resume processing data.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MlUpgradeModeDatafeedReassignmentIT extends BaseMlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(MachineLearning.CONCURRENT_JOB_ALLOCATIONS.getKey(), 4)
            .build();
    }

    public void testDatafeedsReassignWhenUpgradeModeDisabled() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster();

        // Index some historical data for the datafeeds to read during their initial lookback.
        client().admin().indices().prepareCreate("data").setMapping("time", "type=date").get();
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        long initialDocs = randomIntBetween(128, 1024);
        indexDocs(logger, "data", initialDocs, twoWeeksAgo, weekAgo);

        int numJobs = 5;
        List<String> jobIds = new ArrayList<>(numJobs);
        List<String> datafeedIds = new ArrayList<>(numJobs);
        for (int i = 0; i < numJobs; i++) {
            String jobId = "upgrade-mode-reassign-" + i;
            String datafeedId = jobId + "-datafeed";
            jobIds.add(jobId);
            datafeedIds.add(datafeedId);
            setupJobAndDatafeed(jobId, datafeedId);
        }

        // Speed up persistent-task rechecks so reassignment happens promptly in the test.
        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName()
        );
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueMillis(200));

        // All jobs opened and all datafeeds started and assigned to a node, and the lookback over the
        // historical data has been processed.
        assertAllAssigned(jobIds, datafeedIds);
        for (String jobId : jobIds) {
            waitForJobToHaveProcessedAtLeast(jobId, initialDocs);
        }

        // Enable upgrade mode: every job and datafeed must become unassigned (node == null).
        setUpgradeMode(true);
        assertBusy(() -> {
            for (String jobId : jobIds) {
                assertThat("job " + jobId + " should be unassigned in upgrade mode", getJobStats(jobId).getNode(), is(nullValue()));
            }
            for (String datafeedId : datafeedIds) {
                assertThat(
                    "datafeed " + datafeedId + " should be unassigned in upgrade mode",
                    getDatafeedStats(datafeedId).getNode(),
                    is(nullValue())
                );
                // The datafeed task still exists while upgrade mode is on; it is simply awaiting reassignment.
                assertThat(getDatafeedStats(datafeedId).getDatafeedState(), is(DatafeedState.STARTED));
            }
        }, 30, TimeUnit.SECONDS);

        // Disable upgrade mode: all jobs and datafeeds must reopen, reassign and come back online together.
        setUpgradeMode(false);
        assertAllAssigned(jobIds, datafeedIds);

        // Index a fresh batch slightly in the future and confirm every reassigned datafeed actually
        // resumes processing it. This proves the feeds are live again rather than merely reporting
        // STARTED while stalled. (Future timestamps so the real-time datafeeds read them once wall-clock
        // catches up, mirroring MlDistributedFailureIT.)
        long freshDocs = randomIntBetween(16, 64);
        long soon = System.currentTimeMillis();
        indexDocs(logger, "data", freshDocs, soon + 5000, soon + 6000);
        for (String jobId : jobIds) {
            waitForJobToHaveProcessedAtLeast(jobId, initialDocs + freshDocs);
        }

        // Clean up.
        for (int i = 0; i < numJobs; i++) {
            client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedIds.get(i))).actionGet();
            client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobIds.get(i))).actionGet();
        }
    }

    private void waitForJobToHaveProcessedAtLeast(String jobId, long numDocs) throws Exception {
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(jobId);
            assertThat("job " + jobId + " processed record count", dataCounts.getProcessedRecordCount(), greaterThanOrEqualTo(numDocs));
        }, 60, TimeUnit.SECONDS);
    }

    private void assertAllAssigned(List<String> jobIds, List<String> datafeedIds) throws Exception {
        assertBusy(() -> {
            for (String jobId : jobIds) {
                assertThat("job " + jobId + " state", getJobStats(jobId).getState(), is(JobState.OPENED));
                assertThat("job " + jobId + " node", getJobStats(jobId).getNode(), is(not(nullValue())));
            }
            for (String datafeedId : datafeedIds) {
                assertThat("datafeed " + datafeedId + " state", getDatafeedStats(datafeedId).getDatafeedState(), is(DatafeedState.STARTED));
                assertThat("datafeed " + datafeedId + " node", getDatafeedStats(datafeedId).getNode(), is(not(nullValue())));
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void setUpgradeMode(boolean enabled) {
        client().execute(SetUpgradeModeAction.INSTANCE, new SetUpgradeModeAction.Request(enabled)).actionGet();
    }

    private void setupJobAndDatafeed(String jobId, String datafeedId) throws Exception {
        Job.Builder job = createScheduledJob(jobId);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // A short frequency so the real-time datafeed polls promptly for freshly indexed data.
        DatafeedConfig config = createDatafeed(datafeedId, job.getId(), Collections.singletonList("data"), TimeValue.timeValueSeconds(1));
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(config)).actionGet();

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> assertThat(getJobStats(jobId).getState(), is(JobState.OPENED)), 30, TimeUnit.SECONDS);

        setMlIndicesDelayedNodeLeftTimeoutToZero();

        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(config.getId(), 0L)).actionGet();
    }
}

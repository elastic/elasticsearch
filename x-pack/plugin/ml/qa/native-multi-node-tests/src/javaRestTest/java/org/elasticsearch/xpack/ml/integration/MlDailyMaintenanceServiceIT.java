/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlAssignmentNotifier;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlDailyMaintenanceServiceIT extends MlNativeAutodetectIntegTestCase {

    private JobConfigProvider jobConfigProvider;
    private ThreadPool threadPool;

    @Before
    public void setUpMocks() {
        jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        threadPool = mock(ThreadPool.class);
    }

    public void testTriggerDeleteJobsInStateDeletingWithoutDeletionTask() throws InterruptedException {
        MlDailyMaintenanceService maintenanceService = new MlDailyMaintenanceService(
            settings(IndexVersion.current()).build(),
            ClusterName.DEFAULT,
            threadPool,
            client(),
            mock(ClusterService.class),
            mock(AnomalyDetectionAuditor.class),
            mock(MlAssignmentNotifier.class),
            mock(IndexNameExpressionResolver.class),
            true,
            true,
            true,
            true
        );

        putJob("maintenance-test-1");
        putJob("maintenance-test-2");
        putJob("maintenance-test-3");
        assertThat(getJobIds(), containsInAnyOrder("maintenance-test-1", "maintenance-test-2", "maintenance-test-3"));

        blockingCall(maintenanceService::triggerDeleteJobsInStateDeletingWithoutDeletionTask);
        assertThat(getJobIds(), containsInAnyOrder("maintenance-test-1", "maintenance-test-2", "maintenance-test-3"));

        this.<PutJobAction.Response>blockingCall(
            listener -> jobConfigProvider.updateJobBlockReason("maintenance-test-2", new Blocked(Blocked.Reason.DELETE, null), listener)
        );
        this.<PutJobAction.Response>blockingCall(
            listener -> jobConfigProvider.updateJobBlockReason("maintenance-test-3", new Blocked(Blocked.Reason.DELETE, null), listener)
        );
        assertThat(getJobIds(), containsInAnyOrder("maintenance-test-1", "maintenance-test-2", "maintenance-test-3"));
        assertThat(getJob("maintenance-test-1").get(0).isDeleting(), is(false));
        assertThat(getJob("maintenance-test-2").get(0).isDeleting(), is(true));
        assertThat(getJob("maintenance-test-3").get(0).isDeleting(), is(true));

        blockingCall(maintenanceService::triggerDeleteJobsInStateDeletingWithoutDeletionTask);
        assertThat(getJobIds(), containsInAnyOrder("maintenance-test-1"));
    }

    /**
     * Verifies that the idle job auto-close maintenance task closes an open job whose configured
     * datafeed is stopped and whose last data is older than the configured timeout, while leaving
     * an open job without a configured datafeed alone.
     */
    public void testTriggerCloseIdleJobsWithStoppedDatafeeds() throws Exception {
        String idleJobId = "idle-job-test";
        String activeJobId = "active-job-test";
        String dataIndex = "idle-job-data";

        client().admin().indices().prepareCreate(dataIndex).setMapping("time", "type=date,format=epoch_second", "value", "type=long").get();

        putJob(idleJobId);
        putJob(activeJobId);

        DatafeedConfig idleDatafeed = new DatafeedConfig.Builder("datafeed-" + idleJobId, idleJobId).setIndices(List.of(dataIndex)).build();
        putDatafeed(idleDatafeed);

        long nowSeconds = System.currentTimeMillis() / 1000;
        long threeDaysAgoSeconds = nowSeconds - TimeValue.timeValueHours(72).seconds();

        StringBuilder data = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            long ts = threeDaysAgoSeconds - TimeValue.timeValueHours(i).seconds();
            data.append("{\"time\":").append(ts).append(",\"value\":").append(randomIntBetween(1, 100)).append("}\n");
        }

        openJob(idleJobId);
        postData(idleJobId, data.toString());
        flushJob(idleJobId, false);

        openJob(activeJobId);

        assertThat(getJobState(idleJobId), equalTo(JobState.OPENED));
        assertThat(getJobState(activeJobId), equalTo(JobState.OPENED));

        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        ThreadPool realThreadPool = new TestThreadPool("idle-job-test");
        try {
            MlDailyMaintenanceService maintenanceService = new MlDailyMaintenanceService(
                Settings.builder().put(MachineLearning.IDLE_JOB_AUTO_CLOSE_TIMEOUT.getKey(), "24h").build(),
                ClusterName.DEFAULT,
                realThreadPool,
                client(),
                clusterService,
                mock(AnomalyDetectionAuditor.class),
                mock(MlAssignmentNotifier.class),
                mock(IndexNameExpressionResolver.class),
                true,
                true,
                true,
                true
            );

            blockingCall(maintenanceService::triggerCloseIdleJobsWithStoppedDatafeeds);
        } finally {
            terminate(realThreadPool);
        }

        assertThat(getJobState(idleJobId), equalTo(JobState.CLOSED));
        assertThat(getJobState(activeJobId), equalTo(JobState.OPENED));

        closeJob(activeJobId);
    }

    private JobState getJobState(String jobId) {
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
        return response.getResponse().results().get(0).getState();
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function) throws InterruptedException {
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> { latch.countDown(); }, e -> {
            exceptionHolder.set(e);
            latch.countDown();
        });
        function.accept(listener);
        latch.await();
        if (exceptionHolder.get() != null) {
            fail(exceptionHolder.get().getMessage());
        }
    }

    private void putJob(String jobId) {
        Job.Builder job = new Job.Builder(jobId).setAnalysisConfig(
            new AnalysisConfig.Builder((List<Detector>) null).setBucketSpan(TimeValue.timeValueHours(1))
                .setDetectors(Collections.singletonList(new Detector.Builder("count", null).setPartitionFieldName("user").build()))
        ).setDataDescription(new DataDescription.Builder().setTimeFormat("epoch"));

        putJob(job);
    }

    private Set<String> getJobIds() {
        return getJob("*").stream().map(Job::getId).collect(toSet());
    }
}

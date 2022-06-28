/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MlAssignmentNotifier;
import org.elasticsearch.xpack.ml.MlDailyMaintenanceService;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
        when(threadPool.executor(ThreadPool.Names.SAME)).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    public void testTriggerDeleteJobsInStateDeletingWithoutDeletionTask() throws InterruptedException {
        MlDailyMaintenanceService maintenanceService = new MlDailyMaintenanceService(
            settings(Version.CURRENT).build(),
            ClusterName.DEFAULT,
            threadPool,
            client(),
            mock(ClusterService.class),
            mock(MlAssignmentNotifier.class)
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

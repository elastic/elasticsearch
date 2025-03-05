/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryStore;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class SnapshotRetentionServiceTests extends ESTestCase {

    private static final ClusterSettings clusterSettings;
    static {
        Set<Setting<?>> internalSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        internalSettings.add(LifecycleSettings.SLM_RETENTION_SCHEDULE_SETTING);
        clusterSettings = new ClusterSettings(Settings.EMPTY, internalSettings);
    }

    public void testJobsAreScheduled() throws InterruptedException {
        ClockMock clock = new ClockMock();

        ThreadPool threadPool = new TestThreadPool("test");
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
            SnapshotRetentionService service = new SnapshotRetentionService(Settings.EMPTY, FakeRetentionTask::new, clock)
        ) {
            service.init(clusterService);
            assertThat(service.getScheduler().jobCount(), equalTo(0));

            service.onMaster();
            service.setUpdateSchedule(SnapshotLifecyclePolicyMetadataTests.randomCronSchedule());
            assertThat(service.getScheduler().scheduledJobIds(), containsInAnyOrder(SnapshotRetentionService.SLM_RETENTION_JOB_ID));

            service.offMaster();
            assertThat(service.getScheduler().jobCount(), equalTo(0));

            service.onMaster();
            assertThat(service.getScheduler().scheduledJobIds(), containsInAnyOrder(SnapshotRetentionService.SLM_RETENTION_JOB_ID));

            service.setUpdateSchedule("");
            assertThat(service.getScheduler().jobCount(), equalTo(0));

            // Service should not scheduled any jobs once closed
            service.close();
            service.onMaster();
            assertThat(service.getScheduler().jobCount(), equalTo(0));

            threadPool.shutdownNow();
        }
    }

    public void testManualTriggering() throws InterruptedException {
        ClockMock clock = new ClockMock();
        AtomicInteger invoked = new AtomicInteger(0);

        ThreadPool threadPool = new TestThreadPool("test");
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings);
            SnapshotRetentionService service = new SnapshotRetentionService(Settings.EMPTY, () -> new FakeRetentionTask(event -> {
                assertThat(event.jobName(), equalTo(SnapshotRetentionService.SLM_RETENTION_MANUAL_JOB_ID));
                invoked.incrementAndGet();
            }), clock)
        ) {
            service.init(clusterService);
            service.onMaster();
            service.triggerRetention();
            assertThat(invoked.get(), equalTo(1));

            service.offMaster();
            service.triggerRetention();
            assertThat(invoked.get(), equalTo(1));

            service.onMaster();
            service.triggerRetention();
            assertThat(invoked.get(), equalTo(2));
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static class FakeRetentionTask extends SnapshotRetentionTask {
        private final Consumer<SchedulerEngine.Event> onTrigger;

        FakeRetentionTask() {
            this(evt -> {});
        }

        FakeRetentionTask(Consumer<SchedulerEngine.Event> onTrigger) {
            super(mock(Client.class), null, System::nanoTime, mock(SnapshotHistoryStore.class));
            this.onTrigger = onTrigger;
        }

        @Override
        public void triggered(SchedulerEngine.Event event) {
            onTrigger.accept(event);
        }
    }
}

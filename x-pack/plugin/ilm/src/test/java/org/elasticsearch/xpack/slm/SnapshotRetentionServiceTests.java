/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

    public void testJobsAreScheduled() {
        final DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(), DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
        ClockMock clock = new ClockMock();

        try (ThreadPool threadPool = new TestThreadPool("test");
             ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);
             SnapshotRetentionService service = new SnapshotRetentionService(Settings.EMPTY,
                 FakeRetentionTask::new, clusterService, clock)) {
            assertThat(service.getScheduler().jobCount(), equalTo(0));

            service.onMaster();
            service.setUpdateSchedule(SnapshotLifecycleServiceTests.randomSchedule());
            assertThat(service.getScheduler().scheduledJobIds(), containsInAnyOrder(SnapshotRetentionService.SLM_RETENTION_JOB_ID));

            service.offMaster();
            assertThat(service.getScheduler().jobCount(), equalTo(0));

            service.onMaster();
            assertThat(service.getScheduler().scheduledJobIds(), containsInAnyOrder(SnapshotRetentionService.SLM_RETENTION_JOB_ID));

            service.setUpdateSchedule("");
            assertThat(service.getScheduler().jobCount(), equalTo(0));
            threadPool.shutdownNow();
        }
    }

    private static class FakeRetentionTask extends SnapshotRetentionTask {
        FakeRetentionTask() {
            super(mock(Client.class), null, System::nanoTime, mock(SnapshotHistoryStore.class), mock(ThreadPool.class));
        }

        @Override
        public void triggered(SchedulerEngine.Event event) {
            super.triggered(event);
        }
    }
}

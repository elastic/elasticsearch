/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link IndexBalanceMetricsTask.Task}.
 */
public class IndexBalanceMetricsTaskTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
        clusterSettings = clusterService.getClusterSettings();
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    public void testFindTaskReturnsNullWhenNoPersistentTasks() {
        final var state = ClusterState.builder(ClusterName.DEFAULT).build();
        assertThat(IndexBalanceMetricsTask.Task.findTask(state), nullValue());
    }

    public void testFindTaskReturnsNullWhenTaskNotPresent() {
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(
                "other-task-id",
                IndexBalanceMetricsTask.TASK_NAME,
                IndexBalanceMetricsTask.TaskParams.INSTANCE,
                new PersistentTasksCustomMetadata.Assignment("n1", "")
            );
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks.build()).build())
            .build();
        assertThat(IndexBalanceMetricsTask.Task.findTask(state), nullValue());
    }

    public void testFindTaskReturnsTaskWhenPresent() {
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(
                IndexBalanceMetricsTask.TASK_NAME,
                IndexBalanceMetricsTask.TASK_NAME,
                IndexBalanceMetricsTask.TaskParams.INSTANCE,
                new PersistentTasksCustomMetadata.Assignment("n1", "test")
            );
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks.build()).build())
            .build();
        final var found = IndexBalanceMetricsTask.Task.findTask(state);
        assertThat(found, notNullValue());
        assertThat(found.getId(), equalTo(IndexBalanceMetricsTask.TASK_NAME));
    }

    public void testDynamicIntervalUpdateReschedules() {
        final var task = new IndexBalanceMetricsTask.Task(
            1L,
            IndexBalanceMetricsTask.TASK_NAME,
            IndexBalanceMetricsTask.TASK_NAME,
            "test",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            threadPool,
            clusterSettings,
            clusterService,
            new IndexBalanceMetrics(),
            new AtomicReference<>(IndexBalanceMetrics.IndexBalanceState.EMPTY)
        );
        task.startScheduledRefresh();
        final var cancellableBefore = task.getScheduledRefresh();
        assertThat(cancellableBefore, notNullValue());
        clusterSettings.applySettings(
            Settings.builder()
                .put(IndexBalanceMetricsTask.INDEX_BALANCE_METRIC_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(45))
                .build()
        );
        assertThat("previous scheduled task should be cancelled", cancellableBefore.isCancelled(), equalTo(true));
        final var cancellableAfter = task.getScheduledRefresh();
        assertThat(cancellableAfter, notNullValue());
        assertThat("scheduled task should be replaced after interval change", cancellableAfter, not(sameInstance(cancellableBefore)));
        task.onCancelled();
    }
}

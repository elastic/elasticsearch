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
 * Unit tests for {@link IndexBalanceMetricsTaskExecutor.Task}.
 */
public class IndexBalanceMetricsTaskExecutorTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    public void testFindTaskReturnsNullWhenNoPersistentTasks() {
        final var state = ClusterState.builder(ClusterName.DEFAULT).build();
        assertThat(IndexBalanceMetricsTaskExecutor.Task.findTask(state), nullValue());
    }

    public void testFindTaskReturnsNullWhenTaskNotPresent() {
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(
                // This is not the correct name for the index balance metrics task, so the `findTask` call below will return nothing.
                "other-task-id",
                IndexBalanceMetricsTaskExecutor.TASK_NAME,
                IndexBalanceMetricsTaskExecutor.TaskParams.INSTANCE,
                new PersistentTasksCustomMetadata.Assignment("n1", "")
            );
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks.build()).build())
            .build();
        assertThat(IndexBalanceMetricsTaskExecutor.Task.findTask(state), nullValue());
    }

    public void testFindTaskReturnsTaskWhenPresent() {
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(
                IndexBalanceMetricsTaskExecutor.TASK_NAME,
                IndexBalanceMetricsTaskExecutor.TASK_NAME,
                IndexBalanceMetricsTaskExecutor.TaskParams.INSTANCE,
                new PersistentTasksCustomMetadata.Assignment("n1", "test")
            );
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks.build()).build())
            .build();
        final var found = IndexBalanceMetricsTaskExecutor.Task.findTask(state);
        assertThat(found, notNullValue());
        assertThat(found.getId(), equalTo(IndexBalanceMetricsTaskExecutor.TASK_NAME));
    }

    public void testDynamicIntervalUpdateReschedules() {
        final TimeValue initialComputationInterval = randomTimeValueGreaterThan(TimeValue.timeValueHours(1));
        final AtomicReference<TimeValue> currentInterval = new AtomicReference<>(initialComputationInterval);
        final var task = new IndexBalanceMetricsTaskExecutor.Task(
            1L,
            IndexBalanceMetricsTaskExecutor.TASK_NAME,
            IndexBalanceMetricsTaskExecutor.TASK_NAME,
            "test",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            threadPool,
            clusterService,
            currentInterval::get,
            new AtomicReference<>()
        );
        task.startScheduledComputation();
        final var cancellableBefore = task.getScheduledComputation();
        assertThat(cancellableBefore, notNullValue());
        currentInterval.set(randomTimeValueGreaterThan(initialComputationInterval));
        task.requestRecomputation();
        assertThat("previous scheduled task should be cancelled", cancellableBefore.isCancelled(), equalTo(true));
        final var cancellableAfter = task.getScheduledComputation();
        assertThat(cancellableAfter, notNullValue());
        assertThat("scheduled task should be replaced after interval change", cancellableAfter, not(sameInstance(cancellableBefore)));
        task.onCancelled();
    }
}

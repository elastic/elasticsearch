/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Checks that {@link TaskResultsCleanupService} removes task results whose task started longer ago than the retention period, and only
 * those.
 */
public class TaskResultsCleanupIT extends ESSingleNodeTestCase {

    private static final TimeValue RETENTION = TimeValue.timeValueHours(1);

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ReindexPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(TaskResultsCleanupService.TASK_RESULTS_RETENTION_PERIOD_SETTING.getKey(), RETENTION)
            // clean up frequently so the test does not have to wait for the one hour default
            .put(TaskResultsCleanupService.TASK_RESULTS_CLEANUP_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    public void testResultsOfTasksStartedBeforeRetentionAreDeleted() throws Exception {
        final long now = System.currentTimeMillis();

        // started and finished just now
        final TaskId recent = storeResult(1, now, TimeUnit.SECONDS.toNanos(1));
        // started 30 minutes ago and still within the retention period when it finished
        final TaskId recentLongRunning = storeResult(2, now - TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toNanos(5));
        // started two hours ago
        final TaskId expired = storeResult(3, now - TimeUnit.HOURS.toMillis(2), TimeUnit.SECONDS.toNanos(1));
        // The retention period is measured from the start of the task, not from its end: this task finished only 30 minutes ago, but it
        // started 90 minutes ago, so its result is deleted.
        final TaskId startedBeforeRetention = storeResult(4, now - TimeUnit.MINUTES.toMillis(90), TimeUnit.MINUTES.toNanos(60));

        assertBusy(() -> {
            assertTaskResultMissing(expired);
            assertTaskResultMissing(startedBeforeRetention);
        });
        // the cleanup has now covered every document, so the ones that must be retained are still there
        assertNotNull(clusterAdmin().prepareGetTask(recent).get().getTask());
        assertNotNull(clusterAdmin().prepareGetTask(recentLongRunning).get().getTask());
    }

    private TaskId storeResult(long id, long startTimeMillis, long runningTimeNanos) {
        final TaskId taskId = new TaskId(getInstanceFromNode(ClusterService.class).localNode().getId(), id);
        final TaskInfo info = new TaskInfo(
            taskId,
            "test",
            taskId.getNodeId(),
            "test:action",
            "task results cleanup test",
            null,
            startTimeMillis,
            runningTimeNanos,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        getInstanceFromNode(TaskResultsService.class).storeResult(new TaskResult(true, info), future);
        future.actionGet();
        return taskId;
    }

    private void assertTaskResultMissing(TaskId taskId) {
        expectThrows(ResourceNotFoundException.class, () -> clusterAdmin().prepareGetTask(taskId).get());
    }
}

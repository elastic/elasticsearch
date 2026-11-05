/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Checks that {@link TaskResultsCleanupService} removes task results whose completion time (start time plus running time) is older than
 * the retention period, and only those, including when there are more of them than fit in a single page.
 */
public class TaskResultsCleanupIT extends ESSingleNodeTestCase {

    private static final TimeValue RETENTION = TimeValue.timeValueHours(1);

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(TaskResultsCleanupService.TASK_RESULTS_RETENTION_PERIOD_SETTING.getKey(), RETENTION)
            // clean up frequently so the test does not have to wait for the one hour default
            .put(TaskResultsCleanupService.TASK_RESULTS_CLEANUP_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    public void testOnlyExpiredTaskResultsAreDeleted() throws Exception {
        final long now = System.currentTimeMillis();

        // finished just now
        final TaskId recent = storeResult(1, now, TimeUnit.SECONDS.toNanos(1));
        // started well before the retention cutoff, but only finished 30 minutes ago: a start time only check would wrongly delete it
        final TaskId longRunning = storeResult(2, now - TimeUnit.MINUTES.toMillis(90), TimeUnit.MINUTES.toNanos(60));
        // finished two hours ago
        final TaskId expired = storeResult(3, now - TimeUnit.HOURS.toMillis(3), TimeUnit.HOURS.toNanos(1));
        // started and finished long before the cutoff
        final TaskId expiredShort = storeResult(4, now - TimeUnit.HOURS.toMillis(2), TimeUnit.SECONDS.toNanos(1));

        assertBusy(() -> {
            assertTaskResultMissing(expired);
            assertTaskResultMissing(expiredShort);
        });
        // the cleanup has now covered every document, so the ones that must be retained are still there
        assertNotNull(clusterAdmin().prepareGetTask(recent).get().getTask());
        assertNotNull(clusterAdmin().prepareGetTask(longRunning).get().getTask());
    }

    /**
     * More expired results than one page, all with the same start time so that paging has to rely on the node and id tiebreakers, mixed
     * with results that must survive.
     */
    public void testExpiredTaskResultsSpanningSeveralPagesAreDeleted() throws Exception {
        final long now = System.currentTimeMillis();
        final long expiredStart = now - TimeUnit.HOURS.toMillis(2);
        final int expiredCount = TaskResultsCleanupService.PAGE_SIZE * 2 + 100;
        final int retainedCount = 5;

        for (int i = 0; i < expiredCount; i++) {
            storeResult(i, expiredStart, TimeUnit.SECONDS.toNanos(1));
        }
        // started before the cutoff but finishing after it, so these are candidates that must be examined and kept
        for (int i = 0; i < retainedCount; i++) {
            storeResult(expiredCount + i, expiredStart, TimeUnit.MINUTES.toNanos(150));
        }

        final var tasksClient = new OriginSettingClient(client(), TASKS_ORIGIN);
        assertBusy(() -> {
            tasksClient.admin().indices().prepareRefresh(TaskResultsService.TASK_INDEX).get();
            assertHitCount(tasksClient.prepareSearch(TaskResultsService.TASK_INDEX).setSize(0).setTrackTotalHits(true), retainedCount);
        });
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

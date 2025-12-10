/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.reindex.TransportCancelReindexAction.ReasonTaskCannotBeCancelled;
import static org.elasticsearch.reindex.TransportCancelReindexAction.reasonForTaskNotBeingEligibleForCancellation;

public class TransportCancelReindexActionTests extends ESTestCase {

    public void testMissingTaskNotEligibleForCancellation() {
        assertEquals(ReasonTaskCannotBeCancelled.MISSING, reasonForTaskNotBeingEligibleForCancellation(null, ProjectId.DEFAULT));
    }

    public void testNotReindexTaskIsNotEligibleForCancellation() {
        final var task = taskWithActionAndParentAndProjectId("indices:data/write/update", null, null);
        assertEquals(ReasonTaskCannotBeCancelled.NOT_REINDEX, reasonForTaskNotBeingEligibleForCancellation(task, ProjectId.DEFAULT));
    }

    public void testTaskWithParentIsNotEligibleForCancellation() {
        final var task = taskWithActionAndParentAndProjectId(ReindexAction.NAME, taskId(0), null);
        assertEquals(ReasonTaskCannotBeCancelled.IS_SUBTASK, reasonForTaskNotBeingEligibleForCancellation(task, ProjectId.DEFAULT));
    }

    public void testTaskWithNoProjectIdAndRequestWithNonDefaultProjectIdIsNotEligibleForCancellation() {
        final var task = taskWithActionAndParentAndProjectId(ReindexAction.NAME, TaskId.EMPTY_TASK_ID, null);
        final var requestedProjectId = ProjectId.fromId("project-b");

        assertEquals(
            ReasonTaskCannotBeCancelled.TASK_PROJECT_MISSING_REQUEST_NON_DEFAULT,
            reasonForTaskNotBeingEligibleForCancellation(task, requestedProjectId)
        );
    }

    public void testTaskWithDifferentProjectIdIsNotEligibleForCancellation() {
        final var task = taskWithActionAndParentAndProjectId(ReindexAction.NAME, TaskId.EMPTY_TASK_ID, "project-a");
        final var requestedProjectId = ProjectId.fromId("project-b");

        var reason = reasonForTaskNotBeingEligibleForCancellation(task, requestedProjectId);

        assertEquals(ReasonTaskCannotBeCancelled.TASK_PROJECT_MISMATCH, reason);
    }

    public void testTaskWithNoProjectIdAndRequestWithDefaultProjectIdIsEligibleForCancellation() {
        final var task = taskWithActionAndParentAndProjectId(ReindexAction.NAME, TaskId.EMPTY_TASK_ID, null);
        final var requestedProjectId = ProjectId.DEFAULT;

        assertNull(reasonForTaskNotBeingEligibleForCancellation(task, requestedProjectId));
    }

    public void testTaskWithRequestProjectIdIsEligibleForCancellation() {
        final var task = taskWithActionAndParentAndProjectId(ReindexAction.NAME, TaskId.EMPTY_TASK_ID, "project-a");
        final var requestedProjectId = ProjectId.fromId("project-a");

        assertNull(reasonForTaskNotBeingEligibleForCancellation(task, requestedProjectId));
    }

    private static TaskId taskId(final long id) {
        return new TaskId("node", id);
    }

    private static CancellableTask taskWithActionAndParentAndProjectId(final String action, final TaskId parent, final String projectId) {
        final Map<String, String> headers = projectId == null ? Map.of() : Map.of(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        return new CancellableTask(1L, "type", action, "desc", parent, headers) {
        };
    }
}

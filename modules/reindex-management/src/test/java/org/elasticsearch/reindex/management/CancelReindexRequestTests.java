/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class CancelReindexRequestTests extends ESTestCase {

    public void testNotReindexTaskIsNotEligibleForCancellation() {
        final CancelReindexRequest request = new CancelReindexRequest(randomBoolean());
        final CancellableTask task = taskWithActionAndParent("indices:data/write/update", null);
        assertFalse(request.match(task));
    }

    public void testTaskWithParentIsNotEligibleForCancellation() {
        final CancelReindexRequest request = new CancelReindexRequest(randomBoolean());
        final CancellableTask task = taskWithActionAndParent(ReindexAction.NAME, new TaskId("node", 0));
        assertFalse(request.match(task));
    }

    public void testReindexTaskWithNoParentIsEligibleForCancellation() {
        final CancelReindexRequest request = new CancelReindexRequest(randomBoolean());
        final CancellableTask task = taskWithActionAndParent(ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        assertTrue(request.match(task));
    }

    private static CancellableTask taskWithActionAndParent(final String action, final TaskId parent) {
        return new CancellableTask(1L, "type", action, "desc", parent, Map.of());
    }
}

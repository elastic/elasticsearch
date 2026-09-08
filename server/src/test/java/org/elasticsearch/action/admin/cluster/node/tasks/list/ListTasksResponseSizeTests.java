/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.lessThan;

/**
 * {@code GET /_tasks} has no way to bound its response. Every task on every node is collected into
 * a single in-memory list on the coordinating node before any bytes are written, so the response
 * grows with cluster activity and nothing stops it.
 *
 * <p>On a serverless search node that ran out of memory, a single such call built 586,403
 * {@link TaskInfo} objects retaining 564MB of a 992MB heap. The caller was Kibana's periodic
 * unfiltered poll; the indexing node it fanned out to happened to have 14k bulk tasks in flight at
 * the time. No request the cluster served was unusual — the task count was.
 *
 * <p>{@link ListTasksRequest} exposes no {@code size} or {@code max_tasks} parameter, and the
 * request circuit breaker does not apply because nothing is serialised until the list is complete.
 *
 * <p>This test asserts the property we want — that one API call cannot retain an unbounded slice
 * of the heap — so it fails today and would pass once a cap exists.
 */
public class ListTasksResponseSizeTests extends ESTestCase {

    /**
     * A ceiling a single management call should not exceed. Deliberately generous: this is about
     * the absence of any bound, not about the exact figure, which is a design decision.
     */
    private static final long RESPONSE_BUDGET_BYTES = 16 * 1024 * 1024;

    private static TaskInfo taskInfo(int i) {
        return new TaskInfo(
            new TaskId("node-1", i),
            "transport",
            "node-1",
            "indices:data/write/bulk[s]",
            "requests[1], indices[an-index-with-a-realistic-name]",
            null,
            System.nanoTime(),
            0L,
            true,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of("X-Opaque-Id", "kibana-" + i),
            new TaskId("node-1", i),
            System.currentTimeMillis()
        );
    }

    private static ListTasksResponse responseWith(int taskCount) {
        final List<TaskInfo> tasks = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            tasks.add(taskInfo(i));
        }
        return new ListTasksResponse(tasks, List.of(), List.of());
    }

    /** Tasks collected by the single {@code GET /_tasks} call on the node that ran out of memory. */
    private static final int OBSERVED_TASK_COUNT = 586_403;

    public void testResponseIsNotBoundedAtTheObservedTaskCount() {
        // Measured on a sample rather than allocated in full: building the observed count here
        // would need hundreds of megabytes, which is the point being made.
        final int sample = 10_000;
        final long perTask = RamUsageTester.ramUsed(responseWith(sample)) / sample;
        final long atObservedCount = perTask * OBSERVED_TASK_COUNT;

        logger.info(
            "list-tasks response: {} bytes per task; {} tasks would retain {} bytes",
            perTask,
            OBSERVED_TASK_COUNT,
            atObservedCount
        );

        // The task objects here are simpler than production's, which retained roughly 962 bytes
        // each, so this understates the real cost and still exceeds any sane ceiling. Growth is
        // linear in task count with nothing to stop it: the cost of one management call is set by
        // how busy the cluster happens to be.
        assertThat(atObservedCount, lessThan(RESPONSE_BUDGET_BYTES));
    }
}

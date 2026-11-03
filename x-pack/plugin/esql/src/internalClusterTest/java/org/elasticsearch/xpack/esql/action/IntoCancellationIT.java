/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;

/**
 * Cancelling a query while it writes. The rows of every bulk acknowledged before the cancel stay written, as they do
 * for reindex, the query reports the cancellation rather than a statistics row, and nothing lands once the task is
 * gone: the driver waits for the bulks it has in flight before it completes. Whether the write stops before the last
 * page depends on how fast the cancellation reaches the data node, so that is pinned by the writer's unit test.
 * <p>
 * Reads are paused by the {@code pause_me} runtime field of the base class, one permit per document, so the test
 * decides how many rows reach the destination before it cancels.
 */
public class IntoCancellationIT extends AbstractPausableIntegTestCase {

    private static final String DESTINATION = "dest-into";

    @Before
    public void requireInto() {
        assumeTrue("INTO is behind the esql_into feature flag", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testCancelledWriteKeepsWhatWasAcknowledged() throws Exception {
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setSettings(indexSettings(1, 0)).setMapping("""
            { "properties": { "foo": { "type": "long" }, "pause_me": { "type": "long" } } }
            """));
        scriptPermits.drainPermits();
        // One page's worth of rows gets through, so that a bulk reaches the destination before the cancel.
        scriptPermits.release(pageSize());
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put("data_partitioning", "shard").put("page_size", pageSize()).build());
        ActionFuture<EsqlQueryResponse> response = client().execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM test | KEEP foo, pause_me | INTO " + DESTINATION + " WITH { \"refresh\": false }").pragmas(pragmas)
        );
        try {
            assertBusy(() -> {
                refresh(DESTINATION);
                assertThat("the first page is written while the read is paused", count(), equalTo((long) pageSize()));
            });
            cancel(esqlTask().taskId());
            assertCancelled(response);
        } finally {
            scriptPermits.release(numberOfDocs());
        }

        refresh(DESTINATION);
        long written = count();
        assertThat("the rows acknowledged before the cancel stay written", written, greaterThanOrEqualTo((long) pageSize()));
        // The task completed only after its bulks had come back, so nothing lands after the fact.
        assertThat(tasks("indices:data/write/bulk*"), empty());
        refresh(DESTINATION);
        assertThat(count(), equalTo(written));
    }

    private long count() {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + DESTINATION + " | STATS c = COUNT(*)"))) {
            return (long) getValuesList(response).get(0).get(0);
        }
    }

    private TaskInfo esqlTask() throws Exception {
        List<TaskInfo> found = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = tasks(EsqlQueryAction.NAME);
            assertThat(tasks, not(empty()));
            found.addAll(tasks);
        });
        return found.get(0);
    }

    private List<TaskInfo> tasks(String action) {
        return client().admin().cluster().prepareListTasks().setActions(action).setDetailed(true).get().getTasks();
    }

    /** Cancels like {@link EsqlActionTaskIT}: the cancel is issued, the paused read is released, then the task is waited for. */
    private void cancel(TaskId taskId) {
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(false);
        client().admin().cluster().execute(TransportCancelTasksAction.TYPE, request).actionGet();
        scriptPermits.release(numberOfDocs());
        request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(true);
        client().admin().cluster().execute(TransportCancelTasksAction.TYPE, request).actionGet();
    }

    private void assertCancelled(ActionFuture<EsqlQueryResponse> response) throws Exception {
        Exception e = expectThrows(Exception.class, response);
        Throwable cancelled = ExceptionsHelper.unwrap(e, TaskCancelledException.class);
        assertNotNull("not a cancellation: " + e, cancelled);
        assertThat(
            cancelled.getMessage(),
            in(
                List.of(
                    "test cancel",
                    "task cancelled",
                    "request cancelled test cancel",
                    "parent task was cancelled [test cancel]",
                    "cancelled on failure",
                    "task cancelled [cancelled on failure]"
                )
            )
        );
        assertBusy(() -> assertThat(tasks(EsqlQueryAction.NAME), empty()));
    }
}

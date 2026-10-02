/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.After;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end coverage for <a href="https://github.com/elastic/elasticsearch/issues/159914">#159914</a>: a {@code LOAD_ALL} query must
 * be cancellable while the coordinator is expanding the {@code _unmapped_fields} column into per-field columns, not only before or
 * after that phase.
 * <p>
 * A cancellation that arrives during the compute phase already aborts the drivers, so it never reaches expansion. To land a
 * cancellation inside an in-progress expansion deterministically (mirroring the manual async-cancel test that motivated the fix),
 * the test installs {@link ExpandUnmappedFieldsPostProcessor#expansionStartedForTest} to block the coordinator at the start of
 * expansion. It then cancels the query task and releases the block, so the per-row cancellation poll added by the fix observes the
 * cancellation and aborts the expansion, releasing the partially built pages.
 */
public class LoadAllCancellationIT extends AbstractEsqlIntegTestCase {

    @After
    public void clearExpansionHook() {
        ExpandUnmappedFieldsPostProcessor.expansionStartedForTest = null;
    }

    public void testCancelDuringUnmappedFieldExpansion() throws Exception {
        assumeTrue("requires unmapped_fields=\"LOAD_ALL\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());

        // dynamic:false keeps the extra source fields out of the mapping, so LOAD_ALL is the only thing that surfaces them and the
        // coordinator has real expansion work to do.
        assertAcked(client().admin().indices().prepareCreate("load-all-cancel").setSettings(indexSettings(1, 0)).setMapping("""
            { "dynamic": false, "properties": { "mapped": { "type": "long" } } }"""));
        int docs = between(3, 20);
        for (int i = 0; i < docs; i++) {
            indexDoc("load-all-cancel", Integer.toString(i), "mapped", i, "unmapped_a", "a" + i, "unmapped_b", "b" + i);
        }
        refresh("load-all-cancel");

        CountDownLatch expansionStarted = new CountDownLatch(1);
        CountDownLatch proceedWithExpansion = new CountDownLatch(1);
        ExpandUnmappedFieldsPostProcessor.expansionStartedForTest = () -> {
            expansionStarted.countDown();
            try {
                assertTrue("expansion hook was never released", proceedWithExpansion.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };

        var request = syncEsqlQueryRequest("SET unmapped_fields=\"LOAD_ALL\"; FROM load-all-cancel | LIMIT 1000").pragmas(randomPragmas());
        ActionFuture<EsqlQueryResponse> future = client().execute(EsqlQueryAction.INSTANCE, request);

        try {
            // Wait until the coordinator has actually entered the expansion phase (compute is done and the drivers have finished).
            assertTrue("expansion never started", expansionStarted.await(30, TimeUnit.SECONDS));

            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks();
            assertThat(tasks, hasSize(1));
            cancelWithoutWaiting(tasks.get(0).taskId());
        } finally {
            // Let the (now cancelled) expansion resume; its per-row poll should observe the cancellation and throw.
            proceedWithExpansion.countDown();
        }

        Exception e = expectThrows(Exception.class, () -> future.actionGet(DEFAULT_REQUEST_TIMEOUT).close());
        assertThat("expected a TaskCancelledException", ExceptionsHelper.unwrap(e, TaskCancelledException.class), notNullValue());

        // The fix must release the partially built pages on the cancellation path: the request breaker returns to zero.
        ensureBlocksReleased();

        // Mirrors T4 of the manual "graceful termination" report: the node is healthy after a cancelled expansion. Clear the hook so
        // the next expansion runs unimpeded, confirm no ESQL task lingers, then run a fresh LOAD_ALL and check it expands cleanly -
        // proving a cancelled expansion left behind neither a stuck task nor corrupt breaker/seam state.
        ExpandUnmappedFieldsPostProcessor.expansionStartedForTest = null;
        assertBusy(
            () -> assertThat(client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks(), empty())
        );
        try (
            EsqlQueryResponse response = client().execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest("SET unmapped_fields=\"LOAD_ALL\"; FROM load-all-cancel | LIMIT 1000")
            ).actionGet(DEFAULT_REQUEST_TIMEOUT)
        ) {
            List<String> columns = response.columns().stream().map(ColumnInfo::name).toList();
            assertThat(columns, hasItems("mapped", "unmapped_a", "unmapped_b"));
        }
    }

    private void cancelWithoutWaiting(TaskId taskId) {
        // wait_for_completion=false so the cancel returns as soon as the task is marked cancelled — the expansion is still parked in the
        // hook at this point, so a waiting cancel would deadlock against proceedWithExpansion.
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(false);
        client().admin().cluster().execute(TransportCancelTasksAction.TYPE, request).actionGet();
    }
}

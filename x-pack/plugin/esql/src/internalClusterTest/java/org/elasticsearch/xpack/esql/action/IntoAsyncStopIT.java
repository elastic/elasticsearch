/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Stopping an async query while it writes. The stop reaches the writer before the reads wind down, so no page offered
 * after it is written: the destination holds exactly the rows acknowledged by then, the response is the statistics
 * row for those rows, and it is flagged partial. Reads are paused by the {@code pause_me} runtime field of the base
 * class, one permit per document, so the test decides how many rows reach the destination before it stops.
 */
public class IntoAsyncStopIT extends AbstractPausableIntegTestCase {

    private static final String DESTINATION = "dest-into";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        return plugins;
    }

    @Before
    public void requireInto() {
        assumeTrue("INTO is behind the esql_into feature flag", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testStopWritesNothingFurtherAndReportsWhatWasWritten() throws Exception {
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setSettings(indexSettings(1, 0)).setMapping("""
            { "properties": { "foo": { "type": "long" }, "pause_me": { "type": "long" } } }
            """));
        scriptPermits.drainPermits();
        // One page's worth of rows gets through, so that a bulk reaches the destination before the stop.
        scriptPermits.release(pageSize());
        QueryPragmas pragmas = new QueryPragmas(Settings.builder().put("data_partitioning", "shard").put("page_size", pageSize()).build());
        var request = asyncEsqlQueryRequest("FROM test | KEEP foo, pause_me | INTO " + DESTINATION).pragmas(pragmas)
            .waitForCompletionTimeout(TimeValue.timeValueNanos(1))
            .keepOnCompletion(false);
        String asyncId;
        try (EsqlQueryResponse initial = client().execute(EsqlQueryAction.INSTANCE, request).actionGet(60, TimeUnit.SECONDS)) {
            assertThat(initial.isRunning(), is(true));
            asyncId = initial.asyncExecutionId().get();
        }
        try {
            assertBusy(() -> {
                refresh(DESTINATION);
                assertThat("the first page is written while the read is paused", count(), equalTo((long) pageSize()));
            });
            var stop = client().execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(asyncId));
            // The stop fires the writer's hook before it closes the exchange, and the closed exchange lets the coordinator
            // driver finish: once its task is gone the write has been cut, and the paused read can be let go for the
            // query to wind down without a page of it reaching the destination.
            assertBusy(() -> assertThat(driverTasks("final"), empty()));
            scriptPermits.release(numberOfDocs());
            try (EsqlQueryResponse stopped = stop.actionGet(60, TimeUnit.SECONDS)) {
                assertThat(stopped.isRunning(), is(false));
                assertThat("a stopped write is a partial result", stopped.isPartial(), is(true));
                assertThat(
                    stopped.columns().stream().map(ColumnInfoImpl::name).toList(),
                    equalTo(List.of("total", "created", "updated", "version_conflicts", "batches", "failures", "failure_messages"))
                );
                List<Object> stats = getValuesList(stopped).get(0);
                assertThat("the rows written before the stop, and no more", stats.get(0), equalTo((long) pageSize()));
                assertThat(stats.get(1), equalTo((long) pageSize()));
                assertThat(stats.get(5), equalTo(0L));
            }
        } finally {
            scriptPermits.release(numberOfDocs());
        }
        assertBusy(() -> assertThat(tasks(EsqlQueryAction.NAME), empty()));
        assertThat(tasks("indices:data/write/bulk*"), empty());
        refresh(DESTINATION);
        assertThat("nothing landed after the stop", count(), equalTo((long) pageSize()));
    }

    private long count() {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + DESTINATION + " | STATS c = COUNT(*)"))) {
            return (long) getValuesList(response).get(0).get(0);
        }
    }

    private List<TaskInfo> driverTasks(String description) {
        return tasks(DriverTaskRunner.ACTION_NAME).stream()
            .filter(t -> ((DriverStatus) t.status()).description().equals(description))
            .toList();
    }

    private List<TaskInfo> tasks(String action) {
        return client().admin().cluster().prepareListTasks().setActions(action).setDetailed(true).get().getTasks();
    }
}

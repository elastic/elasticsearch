/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.core.IsEqual;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Individual tests for specific aspects of the async query API.
 */
public class AsyncEsqlQueryActionIT extends AbstractPausableIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> actions = new ArrayList<>(super.nodePlugins());
        actions.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        actions.add(InternalExchangePlugin.class);
        return Collections.unmodifiableList(actions);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    public void testBasicAsyncExecution() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertThat(initialResponse.isRunning(), is(true));
            String id = initialResponse.asyncExecutionId().get();

            if (randomBoolean()) {
                // let's timeout first
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueMillis(10));
                getResultsRequest.setKeepAlive(randomKeepAlive());
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                try (var responseWithTimeout = future.get()) {
                    assertThat(initialResponse.asyncExecutionId(), isPresent());
                    assertThat(responseWithTimeout.asyncExecutionId().get(), equalTo(id));
                    assertThat(responseWithTimeout.isRunning(), is(true));
                }
            }

            // Now we wait
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            getResultsRequest.setKeepAlive(randomKeepAlive());
            var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);

            // release the permits to allow the query to proceed
            scriptPermits.release(numberOfDocs());

            try (var finalResponse = future.get()) {
                assertThat(finalResponse, notNullValue());
                assertThat(finalResponse.isRunning(), is(false));
                assertThat(finalResponse.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
                assertThat(getValuesList(finalResponse).size(), equalTo(1));
            }

            // Get the stored result (again)
            var again = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
            try (var finalResponse = again.get()) {
                assertThat(finalResponse, notNullValue());
                assertThat(finalResponse.isRunning(), is(false));
                assertThat(finalResponse.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
                assertThat(getValuesList(finalResponse).size(), equalTo(1));
            }

            AcknowledgedResponse deleteResponse = deleteAsyncId(id);
            assertThat(deleteResponse.isAcknowledged(), equalTo(true));
            // the stored response should no longer be retrievable
            var e = expectThrows(ResourceNotFoundException.class, () -> deleteAsyncId(id));
            assertThat(e.getMessage(), IsEqual.equalTo(id));
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testGetAsyncWhileQueryTaskIsBeingCancelled() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertThat(initialResponse.isRunning(), is(true));
            String id = initialResponse.asyncExecutionId().get();
            // ensure we have started Lucene operators
            assertBusy(() -> {
                var tasks = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks()
                    .stream()
                    .filter(t -> t.description().contains("_LuceneSourceOperator"))
                    .toList();
                assertThat(tasks.size(), greaterThanOrEqualTo(1));
            });
            client().admin()
                .cluster()
                .prepareCancelTasks()
                .setActions(EsqlQueryAction.NAME + AsyncTaskManagementService.ASYNC_ACTION_SUFFIX)
                .get();
            assertBusy(() -> {
                List<TaskInfo> tasks = getEsqlQueryTasks().stream().filter(TaskInfo::cancelled).toList();
                assertThat(tasks, not(empty()));
            });
            // get the result while the query is being cancelled
            {
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueMillis(10));
                getResultsRequest.setKeepAlive(randomKeepAlive());
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                try (var resp = future.get()) {
                    assertThat(initialResponse.asyncExecutionId(), isPresent());
                    assertThat(resp.asyncExecutionId().get(), equalTo(id));
                    assertThat(resp.isRunning(), is(true));
                }
            }
            // release the permits to allow the query to proceed
            scriptPermits.release(numberOfDocs());
            // get the result after the cancellation is done
            {
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(10));
                getResultsRequest.setKeepAlive(randomKeepAlive());
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                TaskCancelledException error = expectThrows(TaskCancelledException.class, future::actionGet);
                assertThat(error.getMessage(), equalTo("by user request"));
            }
            assertTrue(deleteAsyncId(id).isAcknowledged());
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testAsyncCancellation() throws Exception {
        try (var initialResponse = sendAsyncQuery()) {
            assertThat(initialResponse.asyncExecutionId(), isPresent());
            assertThat(initialResponse.isRunning(), is(true));
            String id = initialResponse.asyncExecutionId().get();

            DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
            var future = client().execute(TransportDeleteAsyncResultAction.TYPE, request);

            // there should be just one task
            List<TaskInfo> tasks = getEsqlQueryTasks();
            assertThat(tasks.size(), is(1));

            // release the permits to allow the query to proceed
            scriptPermits.release(numberOfDocs());

            var deleteResponse = future.actionGet(timeValueSeconds(60));
            assertThat(deleteResponse.isAcknowledged(), equalTo(true));

            // there should be no tasks after delete
            tasks = getEsqlQueryTasks();
            assertThat(tasks.size(), is(0));

            // the stored response should no longer be retrievable
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setKeepAlive(timeValueMinutes(10));
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
            var e = expectThrows(
                ResourceNotFoundException.class,
                () -> client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).actionGet()
            );
            assertThat(e.getMessage(), equalTo(id));
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testFinishingBeforeTimeoutKeep() {
        testFinishingBeforeTimeout(true);
    }

    public void testFinishingBeforeTimeoutDoNotKeep() {
        testFinishingBeforeTimeout(false);
    }

    private void testFinishingBeforeTimeout(boolean keepOnCompletion) {
        // don't block the query execution at all
        scriptPermits.drainPermits();
        assert scriptPermits.availablePermits() == 0;

        scriptPermits.release(numberOfDocs());

        var request = EsqlQueryRequestBuilder.newAsyncEsqlQueryRequestBuilder(client())
            .query("from test | stats sum(pause_me)")
            .pragmas(queryPragmas())
            .waitForCompletionTimeout(TimeValue.timeValueSeconds(60))
            .keepOnCompletion(keepOnCompletion)
            .keepAlive(randomKeepAlive());

        try (var response = request.execute().actionGet(60, TimeUnit.SECONDS)) {
            assertThat(response.isRunning(), is(false));
            assertThat(response.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
            assertThat(getValuesList(response).size(), equalTo(1));

            if (keepOnCompletion) {
                assertThat(response.asyncExecutionId(), isPresent());
                // we should be able to retrieve the response by id, since it has been kept
                String id = response.asyncExecutionId().get();
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
                var future = client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest);
                try (var resp = future.actionGet(60, TimeUnit.SECONDS)) {
                    assertThat(resp.asyncExecutionId().get(), equalTo(id));
                    assertThat(resp.isRunning(), is(false));
                    assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("sum(pause_me)", "long", null))));
                    assertThat(getValuesList(resp).size(), equalTo(1));
                }
            } else {
                assertThat(response.asyncExecutionId(), isEmpty());
            }
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testUpdateKeepAlive() throws Exception {
        long nowInMillis = System.currentTimeMillis();
        TimeValue keepAlive = timeValueSeconds(between(30, 60));
        var request = EsqlQueryRequestBuilder.newAsyncEsqlQueryRequestBuilder(client())
            .query("from test | stats sum(pause_me)")
            .pragmas(queryPragmas())
            .waitForCompletionTimeout(TimeValue.timeValueMillis(between(1, 10)))
            .keepOnCompletion(randomBoolean())
            .keepAlive(keepAlive);
        final String asyncId;
        long currentExpiration;
        try {
            try (EsqlQueryResponse initialResponse = request.execute().actionGet(60, TimeUnit.SECONDS)) {
                assertThat(initialResponse.isRunning(), is(true));
                assertTrue(initialResponse.asyncExecutionId().isPresent());
                asyncId = initialResponse.asyncExecutionId().get();
            }
            currentExpiration = getExpirationFromTask(asyncId);
            assertThat(currentExpiration, greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
            // update the expiration while the task is still running
            int iters = iterations(1, 5);
            for (int i = 0; i < iters; i++) {
                long extraKeepAlive = randomIntBetween(30, 60);
                keepAlive = TimeValue.timeValueSeconds(keepAlive.seconds() + extraKeepAlive);
                GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId).setKeepAlive(keepAlive);
                try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                    assertThat(resp.asyncExecutionId(), isPresent());
                    assertThat(resp.asyncExecutionId().get(), equalTo(asyncId));
                    assertTrue(resp.isRunning());
                }
                long updatedExpiration = getExpirationFromTask(asyncId);
                assertThat(updatedExpiration, greaterThanOrEqualTo(currentExpiration + extraKeepAlive));
                assertThat(updatedExpiration, greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
                currentExpiration = updatedExpiration;
            }
        } finally {
            scriptPermits.release(numberOfDocs());
        }
        // allow the query to complete, then update the expiration with the result is being stored in the async index
        assertBusy(() -> {
            GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId);
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertThat(resp.isRunning(), is(false));
            }
        });
        assertThat(getExpirationFromDoc(asyncId), greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
        // update the keepAlive after the query has completed
        int iters = between(1, 5);
        for (int i = 0; i < iters; i++) {
            long extraKeepAlive = randomIntBetween(30, 60);
            keepAlive = TimeValue.timeValueSeconds(keepAlive.seconds() + extraKeepAlive);
            GetAsyncResultRequest getRequest = new GetAsyncResultRequest(asyncId).setKeepAlive(keepAlive);
            try (var resp = client().execute(EsqlAsyncGetResultAction.INSTANCE, getRequest).actionGet()) {
                assertThat(resp.isRunning(), is(false));
            }
            long updatedExpiration = getExpirationFromDoc(asyncId);
            assertThat(updatedExpiration, greaterThanOrEqualTo(currentExpiration + extraKeepAlive));
            assertThat(updatedExpiration, greaterThanOrEqualTo(nowInMillis + keepAlive.getMillis()));
            currentExpiration = updatedExpiration;
        }
    }

    private static long getExpirationFromTask(String asyncId) {
        List<EsqlQueryTask> tasks = new ArrayList<>();
        for (TransportService ts : internalCluster().getInstances(TransportService.class)) {
            for (CancellableTask task : ts.getTaskManager().getCancellableTasks().values()) {
                if (task instanceof EsqlQueryTask queryTask) {
                    EsqlQueryResponse result = queryTask.getCurrentResult();
                    if (result.isAsync() && result.asyncExecutionId().get().equals(asyncId)) {
                        tasks.add(queryTask);
                    }
                }
            }
        }
        assertThat(tasks, hasSize(1));
        return tasks.getFirst().getExpirationTimeMillis();
    }

    private static long getExpirationFromDoc(String asyncId) {
        String docId = AsyncExecutionId.decode(asyncId).getDocId();
        GetResponse doc = client().prepareGet().setIndex(XPackPlugin.ASYNC_RESULTS_INDEX).setId(docId).get();
        assertTrue(doc.isExists());
        return ((Number) doc.getSource().get(AsyncTaskIndexService.EXPIRATION_TIME_FIELD)).longValue();
    }

    private List<TaskInfo> getEsqlQueryTasks() throws Exception {
        List<TaskInfo> foundTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME + "[a]")
                .setDetailed(true)
                .get()
                .getTasks();
            foundTasks.addAll(tasks);
        });
        return foundTasks;
    }

    private EsqlQueryResponse sendAsyncQuery() {
        scriptPermits.drainPermits();
        assert scriptPermits.availablePermits() == 0;

        scriptPermits.release(between(1, 5));
        var pragmas = queryPragmas();
        return EsqlQueryRequestBuilder.newAsyncEsqlQueryRequestBuilder(client())
            .query("from test | stats sum(pause_me)")
            .pragmas(pragmas)
            // deliberately small timeout, to frequently trigger incomplete response
            .waitForCompletionTimeout(TimeValue.timeValueNanos(randomIntBetween(1, 20)))
            .keepOnCompletion(randomBoolean())
            .keepAlive(randomKeepAlive())
            .execute()
            .actionGet(60, TimeUnit.SECONDS);
    }

    private QueryPragmas queryPragmas() {
        return new QueryPragmas(
            Settings.builder()
                // Force shard partitioning because that's all the tests know how to match. It is easier to reason about too.
                .put("data_partitioning", "shard")
                // Limit the page size to something small so we do more than one page worth of work, so we get more status updates.
                .put("page_size", pageSize())
                .build()
        );
    }

    private AcknowledgedResponse deleteAsyncId(String id) {
        DeleteAsyncResultRequest request = new DeleteAsyncResultRequest(id);
        return client().execute(TransportDeleteAsyncResultAction.TYPE, request).actionGet(timeValueSeconds(60));
    }

    TimeValue randomKeepAlive() {
        return randomTimeValue(1, 5, TimeUnit.DAYS);
    }

    public static class LocalStateEsqlAsync extends LocalStateCompositeXPackPlugin {
        public LocalStateEsqlAsync(final Settings settings, final Path configPath) {
            super(settings, configPath);
        }
    }
}

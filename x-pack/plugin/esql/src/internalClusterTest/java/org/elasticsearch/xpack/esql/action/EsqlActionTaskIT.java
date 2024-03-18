/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests that we expose a reasonable task status.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE",
    reason = "These tests were failing frequently, let's learn as much as we can"
)
public class EsqlActionTaskIT extends AbstractPausableIntegTestCase {

    private static final Logger LOGGER = LogManager.getLogger(EsqlActionTaskIT.class);

    private String READ_DESCRIPTION;
    private String MERGE_DESCRIPTION;
    private String REDUCE_DESCRIPTION;

    @Before
    public void setup() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        READ_DESCRIPTION = """
            \\_LuceneSourceOperator[dataPartitioning = SHARD, maxPageSize = pageSize(), limit = 2147483647]
            \\_ValuesSourceReaderOperator[fields = [pause_me]]
            \\_AggregationOperator[mode = INITIAL, aggs = sum of longs]
            \\_ExchangeSinkOperator""".replace("pageSize()", Integer.toString(pageSize()));
        MERGE_DESCRIPTION = """
            \\_ExchangeSourceOperator[]
            \\_AggregationOperator[mode = FINAL, aggs = sum of longs]
            \\_ProjectOperator[projection = [0]]
            \\_LimitOperator[limit = 1000]
            \\_OutputOperator[columns = [sum(pause_me)]]""";
        REDUCE_DESCRIPTION = """
            \\_ExchangeSourceOperator[]
            \\_ExchangeSinkOperator""";
    }

    public void testTaskContents() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql();
        try {
            getTasksStarting();
            scriptPermits.release(pageSize());
            List<TaskInfo> foundTasks = getTasksRunning();
            int luceneSources = 0;
            int valuesSourceReaders = 0;
            int exchangeSources = 0;
            int exchangeSinks = 0;
            for (TaskInfo task : foundTasks) {
                DriverStatus status = (DriverStatus) task.status();
                assertThat(status.sessionId(), not(emptyOrNullString()));
                for (DriverStatus.OperatorStatus o : status.activeOperators()) {
                    logger.info("status {}", o);
                    if (o.operator().startsWith("LuceneSourceOperator[maxPageSize=" + pageSize())) {
                        LuceneSourceOperator.Status oStatus = (LuceneSourceOperator.Status) o.status();
                        assertThat(oStatus.processedSlices(), lessThanOrEqualTo(oStatus.totalSlices()));
                        assertThat(oStatus.processedQueries(), equalTo(Set.of("*:*")));
                        assertThat(oStatus.processedShards(), equalTo(Set.of("test:0")));
                        assertThat(oStatus.sliceIndex(), lessThanOrEqualTo(oStatus.totalSlices()));
                        assertThat(oStatus.sliceMin(), greaterThanOrEqualTo(0));
                        assertThat(oStatus.sliceMax(), greaterThanOrEqualTo(oStatus.sliceMin()));
                        if (oStatus.sliceMin() != 0 && oStatus.sliceMax() != 0) {
                            assertThat(
                                oStatus.current(),
                                either(both(greaterThanOrEqualTo(oStatus.sliceMin())).and(lessThanOrEqualTo(oStatus.sliceMax()))).or(
                                    equalTo(DocIdSetIterator.NO_MORE_DOCS)
                                )
                            );
                        }
                        luceneSources++;
                        continue;
                    }
                    if (o.operator().equals("ValuesSourceReaderOperator[fields = [pause_me]]")) {
                        ValuesSourceReaderOperator.Status oStatus = (ValuesSourceReaderOperator.Status) o.status();
                        assertMap(
                            oStatus.readersBuilt(),
                            matchesMap().entry("pause_me:column_at_a_time:ScriptLongs", greaterThanOrEqualTo(1))
                        );
                        assertThat(oStatus.pagesProcessed(), greaterThanOrEqualTo(1));
                        valuesSourceReaders++;
                        continue;
                    }
                    if (o.operator().equals("ExchangeSourceOperator")) {
                        ExchangeSourceOperator.Status oStatus = (ExchangeSourceOperator.Status) o.status();
                        assertThat(oStatus.pagesWaiting(), greaterThanOrEqualTo(0));
                        assertThat(oStatus.pagesEmitted(), greaterThanOrEqualTo(0));
                        exchangeSources++;
                        continue;
                    }
                    if (o.operator().equals("ExchangeSinkOperator")) {
                        ExchangeSinkOperator.Status oStatus = (ExchangeSinkOperator.Status) o.status();
                        assertThat(oStatus.pagesAccepted(), greaterThanOrEqualTo(0));
                        exchangeSinks++;
                    }
                }
            }
            assertThat(luceneSources, greaterThanOrEqualTo(1));
            assertThat(valuesSourceReaders, equalTo(1));
            assertThat(exchangeSinks, greaterThanOrEqualTo(1));
            assertThat(exchangeSources, equalTo(2));
        } finally {
            scriptPermits.release(numberOfDocs());
            try (EsqlQueryResponse esqlResponse = response.get()) {
                assertThat(Iterators.flatMap(esqlResponse.values(), i -> i).next(), equalTo((long) numberOfDocs()));
            }
        }
    }

    public void testCancelRead() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql();
        try {
            List<TaskInfo> infos = getTasksStarting();
            TaskInfo running = infos.stream().filter(t -> t.description().equals(READ_DESCRIPTION)).findFirst().get();
            cancelTask(running.taskId());
            assertCancelled(response);
        } finally {
            scriptPermits.release(numberOfDocs());
        }
    }

    public void testCancelMerge() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql();
        try {
            List<TaskInfo> infos = getTasksStarting();
            TaskInfo running = infos.stream().filter(t -> t.description().equals(MERGE_DESCRIPTION)).findFirst().get();
            cancelTask(running.taskId());
            assertCancelled(response);
        } finally {
            scriptPermits.release(numberOfDocs());
        }
    }

    public void testCancelEsqlTask() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql();
        try {
            getTasksStarting();
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks();
            cancelTask(tasks.get(0).taskId());
            assertCancelled(response);
        } finally {
            scriptPermits.release(numberOfDocs());
        }
    }

    private ActionFuture<EsqlQueryResponse> startEsql() {
        scriptPermits.drainPermits();
        scriptPermits.release(between(1, 5));
        var pragmas = new QueryPragmas(
            Settings.builder()
                // Force shard partitioning because that's all the tests know how to match. It is easier to reason about too.
                .put("data_partitioning", "shard")
                // Limit the page size to something small so we do more than one page worth of work, so we get more status updates.
                .put("page_size", pageSize())
                // Report the status after every action
                .put("status_interval", "0ms")
                .build()
        );
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client())
            .query("from test | stats sum(pause_me)")
            .pragmas(pragmas)
            .execute();
    }

    private void cancelTask(TaskId taskId) {
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(false);
        LOGGER.debug("--> cancelling task [{}] without waiting for completion", taskId);
        client().admin().cluster().execute(CancelTasksAction.INSTANCE, request).actionGet();
        scriptPermits.release(numberOfDocs());
        request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(true);
        LOGGER.debug("--> cancelling task [{}] with waiting for completion", taskId);
        client().admin().cluster().execute(CancelTasksAction.INSTANCE, request).actionGet();
    }

    /**
     * Fetches tasks until it finds all of them are "starting" or "async".
     * The "async" part is because the coordinating task almost immediately goes async
     * because there isn't any data for it to process.
     */
    private List<TaskInfo> getTasksStarting() throws Exception {
        List<TaskInfo> foundTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(DriverTaskRunner.ACTION_NAME)
                .setDetailed(true)
                .get()
                .getTasks();
            assertThat(tasks, hasSize(equalTo(3)));
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(DriverTaskRunner.ACTION_NAME));
                DriverStatus status = (DriverStatus) task.status();
                logger.info("task {} {}", task.description(), status);
                assertThat(task.description(), anyOf(equalTo(READ_DESCRIPTION), equalTo(MERGE_DESCRIPTION), equalTo(REDUCE_DESCRIPTION)));
                /*
                 * Accept tasks that are either starting or have gone
                 * immediately async. The coordinating task is likely
                 * to have done the latter and the reading task should
                 * have done the former.
                 */
                assertThat(status.status(), either(equalTo(DriverStatus.Status.STARTING)).or(equalTo(DriverStatus.Status.ASYNC)));
            }
            foundTasks.addAll(tasks);
        });
        return foundTasks;
    }

    /**
     * Fetches tasks until it finds at least one running.
     */
    private List<TaskInfo> getTasksRunning() throws Exception {
        List<TaskInfo> foundTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(DriverTaskRunner.ACTION_NAME)
                .setDetailed(true)
                .get()
                .getTasks();
            assertThat(tasks, hasSize(equalTo(3)));
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(DriverTaskRunner.ACTION_NAME));
                DriverStatus status = (DriverStatus) task.status();
                assertThat(task.description(), anyOf(equalTo(READ_DESCRIPTION), equalTo(MERGE_DESCRIPTION), equalTo(REDUCE_DESCRIPTION)));
                if (task.description().equals(READ_DESCRIPTION)) {
                    assertThat(status.status(), equalTo(DriverStatus.Status.RUNNING));
                } else {
                    assertThat(status.status(), equalTo(DriverStatus.Status.ASYNC));
                }
            }
            foundTasks.addAll(tasks);
        });
        return foundTasks;
    }

    private void assertCancelled(ActionFuture<EsqlQueryResponse> response) throws Exception {
        Exception e = expectThrows(Exception.class, response);
        Throwable cancelException = ExceptionsHelper.unwrap(e, TaskCancelledException.class);
        assertNotNull(cancelException);
        /*
         * Either the task was cancelled by out request and has "test cancel"
         * or the cancellation chained from another cancellation and has
         * "task cancelled".
         */
        assertThat(cancelException.getMessage(), either(equalTo("test cancel")).or(equalTo("task cancelled")));
        assertBusy(
            () -> assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get()
                    .getTasks(),
                emptyIterable()
            )
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
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

    private boolean nodeLevelReduction;

    /**
     * Number of docs released by {@link #startEsql}.
     */
    private int prereleasedDocs;

    @Before
    public void setup() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        nodeLevelReduction = randomBoolean();
    }

    public void testTaskContents() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql();
        try {
            getTasksStarting();
            logger.info("unblocking script");
            scriptPermits.release(pageSize());
            List<TaskInfo> foundTasks = getTasksRunning();
            int luceneSources = 0;
            int valuesSourceReaders = 0;
            int exchangeSources = 0;
            int exchangeSinks = 0;
            for (TaskInfo task : foundTasks) {
                DriverStatus status = (DriverStatus) task.status();
                assertThat(status.sessionId(), not(emptyOrNullString()));
                String description = status.description();
                for (OperatorStatus o : status.activeOperators()) {
                    logger.info("status {}", o);
                    if (o.operator().startsWith("LuceneSourceOperator[")) {
                        assertThat(description, equalTo("data"));
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
                        assertThat(description, equalTo("data"));
                        ValuesSourceReaderOperator.Status oStatus = (ValuesSourceReaderOperator.Status) o.status();
                        assertMap(
                            oStatus.readersBuilt(),
                            matchesMap().entry("pause_me:column_at_a_time:ScriptLongs", greaterThanOrEqualTo(1))
                        );
                        assertThat(oStatus.pagesProcessed(), greaterThanOrEqualTo(1));
                        assertThat(oStatus.valuesLoaded(), greaterThanOrEqualTo(1L));
                        valuesSourceReaders++;
                        continue;
                    }
                    if (o.operator().equals("ExchangeSourceOperator")) {
                        assertThat(description, either(equalTo("node_reduce")).or(equalTo("final")));
                        ExchangeSourceOperator.Status oStatus = (ExchangeSourceOperator.Status) o.status();
                        assertThat(oStatus.pagesWaiting(), greaterThanOrEqualTo(0));
                        assertThat(oStatus.pagesEmitted(), greaterThanOrEqualTo(0));
                        exchangeSources++;
                        continue;
                    }
                    if (o.operator().equals("ExchangeSinkOperator")) {
                        assertThat(description, either(equalTo("data")).or(equalTo("node_reduce")));
                        ExchangeSinkOperator.Status oStatus = (ExchangeSinkOperator.Status) o.status();
                        assertThat(oStatus.pagesReceived(), greaterThanOrEqualTo(0));
                        exchangeSinks++;
                    }
                }
            }
            assertThat(luceneSources, greaterThanOrEqualTo(1));
            assertThat(valuesSourceReaders, equalTo(1));
            assertThat(exchangeSinks, greaterThanOrEqualTo(1));
            assertThat(exchangeSources, equalTo(2));
            assertThat(
                dataTasks(foundTasks).get(0).description(),
                equalTo(
                    """
                        \\_LuceneSourceOperator[sourceStatus]
                        \\_ValuesSourceReaderOperator[fields = [pause_me]]
                        \\_AggregationOperator[mode = INITIAL, aggs = sum of longs]
                        \\_ExchangeSinkOperator""".replace(
                        "sourceStatus",
                        "dataPartitioning = SHARD, maxPageSize = " + pageSize() + ", limit = 2147483647, needsScore = false"
                    )
                )
            );
            assertThat(
                nodeReduceTasks(foundTasks).get(0).description(),
                nodeLevelReduceDescriptionMatcher(foundTasks, "\\_AggregationOperator[mode = INTERMEDIATE, aggs = sum of longs]\n")
            );
            assertThat(coordinatorTasks(foundTasks).get(0).description(), equalTo("""
                \\_ExchangeSourceOperator[]
                \\_AggregationOperator[mode = FINAL, aggs = sum of longs]
                \\_ProjectOperator[projection = [0]]
                \\_LimitOperator[limit = 1000]
                \\_OutputOperator[columns = [sum(pause_me)]]"""));

            for (TaskInfo task : dataTasks(foundTasks)) {
                assertThat(((DriverStatus) task.status()).documentsFound(), greaterThan(0L));
                assertThat(((DriverStatus) task.status()).valuesLoaded(), greaterThan(0L));
            }
            for (TaskInfo task : nodeReduceTasks(foundTasks)) {
                assertThat(((DriverStatus) task.status()).documentsFound(), equalTo(0L));
                assertThat(((DriverStatus) task.status()).valuesLoaded(), equalTo(0L));
            }
            for (TaskInfo task : coordinatorTasks(foundTasks)) {
                assertThat(((DriverStatus) task.status()).documentsFound(), equalTo(0L));
                assertThat(((DriverStatus) task.status()).valuesLoaded(), equalTo(0L));
            }
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
            TaskInfo running = infos.stream().filter(t -> ((DriverStatus) t.status()).description().equals("data")).findFirst().get();
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
            TaskInfo running = infos.stream().filter(t -> ((DriverStatus) t.status()).description().equals("final")).findFirst().get();
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
        return startEsql("from test | stats sum(pause_me)");
    }

    /**
     * Start an ESQL query, releasing a few docs from the {@code pause_me}
     * script so it'll actually start but won't finish it's first page.
     */
    private ActionFuture<EsqlQueryResponse> startEsql(String query) {
        scriptPermits.drainPermits();
        // Allow a few docs to calculate os the query gets "started"
        prereleasedDocs = between(1, pageSize() / 2);
        scriptPermits.release(prereleasedDocs);
        var settingsBuilder = Settings.builder()
            // Force shard partitioning because that's all the tests know how to match. It is easier to reason about too.
            .put("data_partitioning", "shard")
            // Limit the page size to something small so we do more than one page worth of work, so we get more status updates.
            .put("page_size", pageSize())
            // Report the status after every action
            .put("status_interval", "0ms");

        if (nodeLevelReduction) {
            // explicitly set the default (true) or don't
            if (randomBoolean()) {
                settingsBuilder.put("node_level_reduction", true);
            }
        } else {
            settingsBuilder.put("node_level_reduction", false);
        }

        var pragmas = new QueryPragmas(settingsBuilder.build());
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client()).query(query).pragmas(pragmas).execute();
    }

    private void cancelTask(TaskId taskId) {
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(false);
        LOGGER.debug("--> cancelling task [{}] without waiting for completion", taskId);
        client().admin().cluster().execute(TransportCancelTasksAction.TYPE, request).actionGet();
        scriptPermits.release(numberOfDocs());
        request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(true);
        LOGGER.debug("--> cancelling task [{}] with waiting for completion", taskId);
        client().admin().cluster().execute(TransportCancelTasksAction.TYPE, request).actionGet();
    }

    /**
     * Fetches tasks until it finds all of them are "starting" or "async".
     * The "async" part is because the coordinating task almost immediately goes async
     * because there isn't any data for it to process.
     */
    private List<TaskInfo> getTasksStarting() throws Exception {
        List<TaskInfo> foundTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = getDriverTasks();
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(DriverTaskRunner.ACTION_NAME));
                DriverStatus status = (DriverStatus) task.status();
                logger.info("task {} {} {}", status.description(), task.description(), status);
                assertThat(status.description(), anyOf(equalTo("data"), equalTo("node_reduce"), equalTo("final")));
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
            List<TaskInfo> tasks = getDriverTasks();
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(DriverTaskRunner.ACTION_NAME));
                DriverStatus status = (DriverStatus) task.status();
                assertThat(status.description(), anyOf(equalTo("data"), equalTo("node_reduce"), equalTo("final")));
                if (status.description().equals("data")) {
                    assertThat(status.status(), equalTo(DriverStatus.Status.RUNNING));
                } else {
                    assertThat(status.status(), equalTo(DriverStatus.Status.ASYNC));
                }
            }
            foundTasks.addAll(tasks);
        });
        return foundTasks;
    }

    /**
     * Fetches tasks until all three driver tasks have started
     */
    private List<TaskInfo> getDriverTasks() throws Exception {
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
            assertThat(dataTasks(tasks), hasSize(1));
            assertThat(nodeReduceTasks(tasks), hasSize(1));
            assertThat(coordinatorTasks(tasks), hasSize(1));
            foundTasks.addAll(tasks);
        });
        return foundTasks;
    }

    private List<TaskInfo> dataTasks(List<TaskInfo> tasks) {
        return tasks.stream().filter(t -> ((DriverStatus) t.status()).description().equals("data")).toList();
    }

    private List<TaskInfo> nodeReduceTasks(List<TaskInfo> tasks) {
        return tasks.stream().filter(t -> ((DriverStatus) t.status()).description().equals("node_reduce")).toList();
    }

    private List<TaskInfo> coordinatorTasks(List<TaskInfo> tasks) {
        return tasks.stream().filter(t -> ((DriverStatus) t.status()).description().equals("final")).toList();
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
        assertThat(
            cancelException.getMessage(),
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

    /**
     * Ensure that when some exchange requests fail, we cancel the ESQL request, and complete all
     * exchange sinks with the failure, despite having outstanding pages in the buffer.
     */
    public void testCancelRequestWhenFailingFetchingPages() throws Exception {
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String dataNode = internalCluster().startDataOnlyNode();
        // block, then fail exchange requests when we have outstanding pages
        var transportService = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNode);
        CountDownLatch fetchingStarted = new CountDownLatch(1);
        CountDownLatch allowedFetching = new CountDownLatch(1);
        transportService.addRequestHandlingBehavior(ExchangeService.EXCHANGE_ACTION_NAME, (handler, request, channel, task) -> {
            AbstractRunnable runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    channel.sendResponse(e);
                }

                @Override
                protected void doRun() throws Exception {
                    fetchingStarted.countDown();
                    assertTrue(allowedFetching.await(1, TimeUnit.MINUTES));
                    onFailure(new IOException("failed to fetch pages"));
                }
            };
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(runnable);
        });
        try {
            scriptPermits.release(numberOfDocs()); // do not block Lucene operators
            Client client = client(coordinator);
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", dataNode).build())
                .get();
            ensureYellowAndNoInitializingShards("test");
            request.query("FROM test | LIMIT 10");
            QueryPragmas pragmas = randomPragmas();
            request.pragmas(pragmas);
            PlainActionFuture<EsqlQueryResponse> future = new PlainActionFuture<>();
            client.execute(EsqlQueryAction.INSTANCE, request, future);
            ExchangeService exchangeService = internalCluster().getInstance(ExchangeService.class, dataNode);
            final boolean waitedForPages;
            final String exchangeId;
            try {
                List<TaskInfo> foundTasks = new ArrayList<>();
                assertBusy(() -> {
                    List<TaskInfo> tasks = client().admin()
                        .cluster()
                        .prepareListTasks()
                        .setActions(EsqlQueryAction.NAME)
                        .setDetailed(true)
                        .get()
                        .getTasks();
                    assertThat(tasks, hasSize(1));
                    foundTasks.addAll(tasks);
                });
                final String sessionId = foundTasks.get(0).taskId().toString();
                assertTrue(fetchingStarted.await(1, TimeUnit.MINUTES));
                List<String> sinkKeys = exchangeService.sinkKeys()
                    .stream()
                    .filter(
                        s -> s.startsWith(sessionId)
                            // exclude the node-level reduction sink
                            && s.endsWith("[n]") == false
                    )
                    .toList();
                assertThat(sinkKeys.toString(), sinkKeys.size(), equalTo(1));
                exchangeId = sinkKeys.get(0);
                ExchangeSinkHandler exchangeSink = exchangeService.getSinkHandler(exchangeId);
                waitedForPages = randomBoolean();
                if (waitedForPages) {
                    // do not fail exchange requests until we have some pages.
                    assertBusy(() -> assertThat(exchangeSink.bufferSize(), greaterThan(0)));
                }
            } finally {
                allowedFetching.countDown();
            }
            Exception failure = expectThrows(Exception.class, () -> future.actionGet().close());
            EsqlTestUtils.assertEsqlFailure(failure);
            Throwable cause = ExceptionsHelper.unwrap(failure, IOException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), containsString("failed to fetch pages"));
            // If we proceed without waiting for pages, we might cancel the main request before starting the data-node request.
            // As a result, the exchange sinks on data-nodes won't be removed until the inactive_timeout elapses, which is
            // longer than the assertBusy timeout.
            if (waitedForPages == false) {
                exchangeService.finishSinkHandler(exchangeId, failure);
            }
        } finally {
            transportService.clearAllRules();
        }
    }

    public void testTaskContentsForTopNQuery() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql("from test | sort pause_me | keep pause_me");
        try {
            getTasksStarting();
            logger.info("unblocking script");
            scriptPermits.release(pageSize());
            List<TaskInfo> tasks = getTasksRunning();
            String sortStatus = """
                [{"pause_me":{"order":"asc","missing":"_last","unmapped_type":"long"}}]""";
            String sourceStatus = "dataPartitioning = SHARD, maxPageSize = "
                + pageSize()
                + ", limit = 1000, needsScore = false, sorts = "
                + sortStatus;
            assertThat(dataTasks(tasks).get(0).description(), equalTo("""
                \\_LuceneTopNSourceOperator[sourceStatus]
                \\_ValuesSourceReaderOperator[fields = [pause_me]]
                \\_ProjectOperator[projection = [1]]
                \\_ExchangeSinkOperator""".replace("sourceStatus", sourceStatus)));
            assertThat(
                nodeReduceTasks(tasks).get(0).description(),
                nodeLevelReduceDescriptionMatcher(
                    tasks,
                    "\\_TopNOperator[count=1000, elementTypes=[LONG], encoders=[DefaultSortable], "
                        + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]]]\n"
                )
            );
            assertThat(
                coordinatorTasks(tasks).get(0).description(),
                equalTo(
                    "\\_ExchangeSourceOperator[]\n"
                        + "\\_TopNOperator[count=1000, elementTypes=[LONG], encoders=[DefaultSortable], "
                        + "sortOrders=[SortOrder[channel=0, asc=true, nullsFirst=false]]]\n"
                        + "\\_ProjectOperator[projection = [0]]\n"
                        + "\\_OutputOperator[columns = [pause_me]]"
                )
            );
        } finally {
            // each scripted field "emit" is called by LuceneTopNSourceOperator and by ValuesSourceReaderOperator
            scriptPermits.release(2 * numberOfDocs());
            try (EsqlQueryResponse esqlResponse = response.get()) {
                assertThat(Iterators.flatMap(esqlResponse.values(), i -> i).next(), equalTo(1L));
            }
        }
    }

    public void testTaskContentsForLimitQuery() throws Exception {
        String limit = Integer.toString(randomIntBetween(pageSize() + 1, 2 * numberOfDocs()));
        ActionFuture<EsqlQueryResponse> response = startEsql("from test | keep pause_me | limit " + limit);
        try {
            getTasksStarting();
            logger.info("unblocking script");
            scriptPermits.release(pageSize() - prereleasedDocs);
            List<TaskInfo> tasks = getTasksRunning();
            assertThat(dataTasks(tasks).get(0).description(), equalTo("""
                \\_LuceneSourceOperator[dataPartitioning = SHARD, maxPageSize = pageSize(), limit = limit(), needsScore = false]
                \\_ValuesSourceReaderOperator[fields = [pause_me]]
                \\_ProjectOperator[projection = [1]]
                \\_ExchangeSinkOperator""".replace("pageSize()", Integer.toString(pageSize())).replace("limit()", limit)));
            assertThat(
                nodeReduceTasks(tasks).get(0).description(),
                nodeLevelReduceDescriptionMatcher(tasks, "\\_LimitOperator[limit = " + limit + "]\n")
            );
            assertThat(coordinatorTasks(tasks).get(0).description(), equalTo("""
                \\_ExchangeSourceOperator[]
                \\_LimitOperator[limit = limit()]
                \\_ProjectOperator[projection = [0]]
                \\_OutputOperator[columns = [pause_me]]""".replace("limit()", limit)));
        } finally {
            scriptPermits.release(numberOfDocs());
            try (EsqlQueryResponse esqlResponse = response.get()) {
                assertThat(Iterators.flatMap(esqlResponse.values(), i -> i).next(), equalTo(1L));
            }
        }
    }

    public void testTaskContentsForGroupingStatsQuery() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql("from test | stats max(foo) by pause_me");
        try {
            getTasksStarting();
            logger.info("unblocking script");
            scriptPermits.release(pageSize());
            List<TaskInfo> tasks = getTasksRunning();
            String sourceStatus = "dataPartitioning = SHARD, maxPageSize = pageSize(), limit = 2147483647, needsScore = false".replace(
                "pageSize()",
                Integer.toString(pageSize())
            );
            assertThat(
                dataTasks(tasks).get(0).description(),
                equalTo(
                    """
                        \\_LuceneSourceOperator[sourceStatus]
                        \\_ValuesSourceReaderOperator[fields = [foo]]
                        \\_OrdinalsGroupingOperator(aggs = max of longs)
                        \\_ExchangeSinkOperator""".replace("sourceStatus", sourceStatus)

                )
            );
            assertThat(
                nodeReduceTasks(tasks).get(0).description(),
                nodeLevelReduceDescriptionMatcher(tasks, "\\_HashAggregationOperator[mode = <not-needed>, aggs = max of longs]\n")
            );
            assertThat(coordinatorTasks(tasks).get(0).description(), equalTo("""
                \\_ExchangeSourceOperator[]
                \\_HashAggregationOperator[mode = <not-needed>, aggs = max of longs]
                \\_ProjectOperator[projection = [1, 0]]
                \\_LimitOperator[limit = 1000]
                \\_OutputOperator[columns = [max(foo), pause_me]]"""));
        } finally {
            scriptPermits.release(numberOfDocs());
            try (EsqlQueryResponse esqlResponse = response.get()) {
                var it = Iterators.flatMap(esqlResponse.values(), i -> i);
                assertThat(it.next(), equalTo(numberOfDocs() - 1L)); // max of numberOfDocs() generated int values
                assertThat(it.next(), equalTo(1L)); // pause_me always emits 1
            }
        }
    }

    private Matcher<String> nodeLevelReduceDescriptionMatcher(List<TaskInfo> tasks, String nodeReduce) {
        boolean matchNodeReduction = nodeLevelReduction
            // If the data node and the coordinator are the same node then we don't reduce aggs in it.
            && false == dataTasks(tasks).get(0).node().equals(coordinatorTasks(tasks).get(0).node());
        return equalTo("\\_ExchangeSourceOperator[]\n" + (matchNodeReduction ? nodeReduce : "") + "\\_ExchangeSinkOperator");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }
}

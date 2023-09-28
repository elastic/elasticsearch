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
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
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
public class EsqlActionTaskIT extends AbstractEsqlIntegTestCase {
    private static int PAGE_SIZE;
    private static int NUM_DOCS;

    private static String READ_DESCRIPTION;
    private static String MERGE_DESCRIPTION;
    private static final Logger LOGGER = LogManager.getLogger(EsqlActionTaskIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), PausableFieldPlugin.class);
    }

    @Before
    public void setupIndex() throws IOException {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        PAGE_SIZE = between(10, 100);
        NUM_DOCS = between(4 * PAGE_SIZE, 5 * PAGE_SIZE);
        READ_DESCRIPTION = """
            \\_LuceneSourceOperator[dataPartitioning = SHARD, maxPageSize = PAGE_SIZE, limit = 2147483647]
            \\_ValuesSourceReaderOperator[field = pause_me]
            \\_AggregationOperator[mode = INITIAL, aggs = sum of longs]
            \\_ExchangeSinkOperator""".replace("PAGE_SIZE", Integer.toString(PAGE_SIZE));
        MERGE_DESCRIPTION = """
            \\_ExchangeSourceOperator[]
            \\_AggregationOperator[mode = FINAL, aggs = sum of longs]
            \\_ProjectOperator[projection = [0]]
            \\_LimitOperator[limit = 500]
            \\_OutputOperator[columns = sum(pause_me)]""";

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("pause_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "pause").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Map.of("number_of_shards", 1, "number_of_replicas", 0))
            .setMapping(mapping.endObject())
            .get();

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        bulk.get();
        /*
         * forceMerge so we can be sure that we don't bump into tiny
         * segments that finish super quickly and cause us to report strange
         * statuses when we expect "starting".
         */
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).get();
        /*
         * Double super extra paranoid check that force merge worked. It's
         * failed to reduce the index to a single segment and caused this test
         * to fail in very difficult to debug ways. If it fails again, it'll
         * trip here. Or maybe it won't! And we'll learn something. Maybe
         * it's ghosts.
         */
        SegmentsStats stats = client().admin().indices().prepareStats("test").get().getPrimaries().getSegments();
        if (stats.getCount() != 1L) {
            fail(Strings.toString(stats));
        }
    }

    public void testTaskContents() throws Exception {
        ActionFuture<EsqlQueryResponse> response = startEsql();
        try {
            getTasksStarting();
            scriptPermits.release(PAGE_SIZE);
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
                    if (o.operator().startsWith("LuceneSourceOperator[maxPageSize=" + PAGE_SIZE)) {
                        LuceneSourceOperator.Status oStatus = (LuceneSourceOperator.Status) o.status();
                        assertThat(oStatus.processedSlices(), lessThanOrEqualTo(oStatus.totalSlices()));
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
                    if (o.operator().equals("ValuesSourceReaderOperator[field = pause_me]")) {
                        ValuesSourceReaderOperator.Status oStatus = (ValuesSourceReaderOperator.Status) o.status();
                        assertMap(oStatus.readersBuilt(), matchesMap().entry("LongValuesReader", greaterThanOrEqualTo(1)));
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
            assertThat(exchangeSources, equalTo(1));
        } finally {
            scriptPermits.release(NUM_DOCS);
            assertThat(Iterators.flatMap(response.get().values(), i -> i).next(), equalTo((long) NUM_DOCS));
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
            scriptPermits.release(NUM_DOCS);
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
            scriptPermits.release(NUM_DOCS);
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
            scriptPermits.release(NUM_DOCS);
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
                .put("page_size", PAGE_SIZE)
                // Report the status after every action
                .put("status_interval", "0ms")
                .build()
        );
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query("from test | stats sum(pause_me)")
            .pragmas(pragmas)
            .execute();
    }

    private void cancelTask(TaskId taskId) {
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(taskId).setReason("test cancel");
        request.setWaitForCompletion(false);
        LOGGER.debug("--> cancelling task [{}] without waiting for completion", taskId);
        client().admin().cluster().execute(CancelTasksAction.INSTANCE, request).actionGet();
        scriptPermits.release(NUM_DOCS);
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
            assertThat(tasks, hasSize(equalTo(2)));
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(DriverTaskRunner.ACTION_NAME));
                DriverStatus status = (DriverStatus) task.status();
                logger.info("task {} {}", task.description(), status);
                assertThat(task.description(), either(equalTo(READ_DESCRIPTION)).or(equalTo(MERGE_DESCRIPTION)));
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
            assertThat(tasks, hasSize(equalTo(2)));
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(DriverTaskRunner.ACTION_NAME));
                DriverStatus status = (DriverStatus) task.status();
                assertThat(task.description(), either(equalTo(READ_DESCRIPTION)).or(equalTo(MERGE_DESCRIPTION)));
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
        Exception e = expectThrows(Exception.class, response::actionGet);
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

    private static final Semaphore scriptPermits = new Semaphore(0);

    public static class PausableFieldPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "pause";
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    return (FactoryType) new LongFieldScript.Factory() {
                        @Override
                        public LongFieldScript.LeafFactory newFactory(
                            String fieldName,
                            Map<String, Object> params,
                            SearchLookup searchLookup,
                            OnScriptError onScriptError
                        ) {
                            return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                @Override
                                public void execute() {
                                    try {
                                        assertTrue(scriptPermits.tryAcquire(1, TimeUnit.MINUTES));
                                    } catch (Exception e) {
                                        throw new AssertionError(e);
                                    }
                                    LOGGER.debug("--> emitting value");
                                    emit(1);
                                }
                            };
                        }
                    };
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }

}

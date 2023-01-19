/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plugin.EsqlComputeEngineAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests that we expose a reasonable task status.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false) // ESQL is single node
public class EsqlActionTaskIT extends ESIntegTestCase {
    private static final int COUNT = LuceneSourceOperator.PAGE_SIZE * 5;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPlugin.class, PausableFieldPlugin.class);
    }

    public void testTask() throws Exception {
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
        client().admin().indices().prepareCreate("test").setSettings(Map.of("number_of_shards", 1)).setMapping(mapping.endObject()).get();

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < COUNT; i++) {
            bulk.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        bulk.get();
        ActionFuture<EsqlQueryResponse> response = new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(
            "from test | stats sum(pause_me)"
        ).pragmas(Settings.builder().put("data_partitioning", "shard").build()).execute();

        String readDescription = """
            \\_LuceneSourceOperator(dataPartitioning = SHARD)
            \\_ValuesSourceReaderOperator(field = pause_me)
            \\_AggregationOperator(mode = INITIAL, aggs = sum of longs)
            \\_ExchangeSinkOperator""";
        String mergeDescription = """
            \\_ExchangeSourceOperator(partitioning = SINGLE_DISTRIBUTION)
            \\_AggregationOperator(mode = FINAL, aggs = sum of longs)
            \\_LimitOperator(limit = 10000)
            \\_OutputOperator (columns = sum(pause_me))""";

        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlComputeEngineAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks();
            assertThat(tasks, hasSize(equalTo(2)));
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(EsqlComputeEngineAction.NAME));
                assertThat(task.description(), either(equalTo(readDescription)).or(equalTo(mergeDescription)));
                DriverStatus status = (DriverStatus) task.status();
                assertThat(status.status(), equalTo(DriverStatus.Status.STARTING));
            }
        });

        start.await();
        List<TaskInfo> foundTasks = new ArrayList<>();
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlComputeEngineAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks();
            assertThat(tasks, hasSize(equalTo(2)));
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(EsqlComputeEngineAction.NAME));
                assertThat(task.description(), either(equalTo(readDescription)).or(equalTo(mergeDescription)));
                DriverStatus status = (DriverStatus) task.status();
                assertThat(
                    status.status(),
                    equalTo(task.description().equals(readDescription) ? DriverStatus.Status.RUNNING : DriverStatus.Status.STARTING)
                );
            }
            foundTasks.addAll(tasks);
        });
        int luceneSources = 0;
        int valuesSourceReaders = 0;
        int exchangeSources = 0;
        int exchangeSinks = 0;
        for (TaskInfo task : foundTasks) {
            DriverStatus status = (DriverStatus) task.status();
            for (DriverStatus.OperatorStatus o : status.activeOperators()) {
                if (o.operator().equals("LuceneSourceOperator[shardId=0]")) {
                    LuceneSourceOperator.Status oStatus = (LuceneSourceOperator.Status) o.status();
                    assertThat(oStatus.currentLeaf(), lessThanOrEqualTo(oStatus.totalLeaves()));
                    assertThat(oStatus.leafPosition(), lessThanOrEqualTo(oStatus.leafSize()));
                    luceneSources++;
                    continue;
                }
                if (o.operator().equals("ValuesSourceReaderOperator")) {
                    ValuesSourceReaderOperator.Status oStatus = (ValuesSourceReaderOperator.Status) o.status();
                    assertThat(oStatus.readersBuilt(), equalTo(Map.of("LongValuesReader", 1)));
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

        drain.await();
        assertThat(response.get().values(), equalTo(List.of(List.of((long) COUNT))));
    }

    private static final CyclicBarrier start = new CyclicBarrier(2);
    private static final CyclicBarrier drain = new CyclicBarrier(2);

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
                        int permits = 0;
                        boolean started = false;
                        boolean draining = false;

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
                                    if (permits > 0) {
                                        permits--;
                                    } else {
                                        try {
                                            if (false == started) {
                                                start.await();
                                                started = true;
                                                permits = LuceneSourceOperator.PAGE_SIZE * 2;
                                                // Sleeping so when we finish this run we'll be over the limit on this thread
                                                Thread.sleep(Driver.DEFAULT_TIME_BEFORE_YIELDING.millis());
                                            } else if (false == draining) {
                                                drain.await();
                                                draining = true;
                                                permits = Integer.MAX_VALUE;
                                            }
                                        } catch (InterruptedException | BrokenBarrierException e) {
                                            throw new AssertionError("ooff", e);
                                        }
                                    }
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

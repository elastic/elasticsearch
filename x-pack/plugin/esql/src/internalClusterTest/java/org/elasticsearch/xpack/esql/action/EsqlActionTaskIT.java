/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests that we expose a reasonable task status.
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false) // ESQL is single node
public class EsqlActionTaskIT extends ESIntegTestCase {
    private static final int COUNT = 100;

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

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < COUNT; i++) {
            indexRequests.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", i));
        }
        indexRandom(true, indexRequests);
        ActionFuture<EsqlQueryResponse> response = new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(
            "from test | stats sum(pause_me)"
        ).pragmas(Settings.builder().put("data_partitioning", "shard").build()).execute();

        {
            List<TaskInfo> tasks = new ArrayList<>();
            assertBusy(() -> {
                List<TaskInfo> fetched = client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlComputeEngineAction.NAME)
                    .get()
                    .getTasks();
                assertThat(fetched, hasSize(greaterThan(0)));
                tasks.addAll(fetched);
            });
            for (TaskInfo task : tasks) {
                assertThat(task.action(), equalTo(EsqlComputeEngineAction.NAME));
                assertThat(task.description(), nullValue());
            }
        }

        List<TaskInfo> tasks = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(EsqlComputeEngineAction.NAME)
            .setDetailed(true)
            .get()
            .getTasks();
        assertThat(tasks, hasSize(greaterThan(0)));
        for (TaskInfo task : tasks) {
            assertThat(task.action(), equalTo(EsqlComputeEngineAction.NAME));
            assertThat(task.description(), either(containsString("\\_LuceneSourceOperator")).or(containsString("\\_OutputOperator")));
        }

        for (int i = 0; i < COUNT; i++) {
            barrier.await();
        }
        assertThat(response.get().values(), equalTo(List.of(List.of((long) COUNT))));
    }

    private static final CyclicBarrier barrier = new CyclicBarrier(2);

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
                                        barrier.await();
                                    } catch (InterruptedException | BrokenBarrierException e) {
                                        throw new RuntimeException("ooff", e);
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

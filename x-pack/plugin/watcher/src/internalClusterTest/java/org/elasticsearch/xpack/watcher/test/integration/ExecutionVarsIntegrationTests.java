/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherMockScriptPlugin;
import org.hamcrest.Matcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.scriptTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ExecutionVarsIntegrationTests extends AbstractWatcherIntegrationTestCase {

    private String watchId = randomAlphaOfLength(20);

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(CustomScriptPlugin.class);
        return types;
    }

    public static class CustomScriptPlugin extends WatcherMockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("ctx.vars.condition_value = ctx.payload.value + 5; return ctx.vars.condition_value > 5;", vars -> {
                int value = (int) XContentMapValues.extractValue("ctx.payload.value", vars);

                Map<String, Object> ctxVars = (Map<String, Object>) XContentMapValues.extractValue("ctx.vars", vars);
                ctxVars.put("condition_value", value + 5);

                return (int) XContentMapValues.extractValue("condition_value", ctxVars) > 5;
            });

            scripts.put("ctx.vars.watch_transform_value = ctx.vars.condition_value + 5; return ctx.payload;", vars -> {
                Map<String, Object> ctxVars = (Map<String, Object>) XContentMapValues.extractValue("ctx.vars", vars);
                ctxVars.put("watch_transform_value", (int) XContentMapValues.extractValue("condition_value", ctxVars) + 5);

                return XContentMapValues.extractValue("ctx.payload", vars);
            });

            // Transforms the value of a1, equivalent to:
            // ctx.vars.a1_transform_value = ctx.vars.watch_transform_value + 10;
            // ctx.payload.a1_transformed_value = ctx.vars.a1_transform_value;
            // return ctx.payload;
            scripts.put("transform a1", vars -> {
                Map<String, Object> ctxVars = (Map<String, Object>) XContentMapValues.extractValue("ctx.vars", vars);
                Map<String, Object> ctxPayload = (Map<String, Object>) XContentMapValues.extractValue("ctx.payload", vars);

                int value = (int) XContentMapValues.extractValue("watch_transform_value", ctxVars);
                ctxVars.put("a1_transform_value", value + 10);

                value = (int) XContentMapValues.extractValue("a1_transform_value", ctxVars);
                ctxPayload.put("a1_transformed_value", value);

                return XContentMapValues.extractValue("ctx.payload", vars);
            });

            // Transforms the value of a2, equivalent to:
            // ctx.vars.a2_transform_value = ctx.vars.watch_transform_value + 20;
            // ctx.payload.a2_transformed_value = ctx.vars.a2_transform_value;
            // return ctx.payload;
            scripts.put("transform a2", vars -> {
                Map<String, Object> ctxVars = (Map<String, Object>) XContentMapValues.extractValue("ctx.vars", vars);
                Map<String, Object> ctxPayload = (Map<String, Object>) XContentMapValues.extractValue("ctx.payload", vars);

                int value = (int) XContentMapValues.extractValue("watch_transform_value", ctxVars);
                ctxVars.put("a2_transform_value", value + 20);

                value = (int) XContentMapValues.extractValue("a2_transform_value", ctxVars);
                ctxPayload.put("a2_transformed_value", value);

                return XContentMapValues.extractValue("ctx.payload", vars);
            });

            return scripts;
        }
    }

    public void testVars() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId(watchId)
            .setSource(
                watchBuilder().trigger(schedule(interval("1h")))
                    .input(simpleInput("value", 5))
                    .condition(
                        new ScriptCondition(
                            mockScript("ctx.vars.condition_value = ctx.payload.value + 5; return ctx.vars.condition_value > 5;")
                        )
                    )
                    .transform(
                        scriptTransform(mockScript("ctx.vars.watch_transform_value = ctx.vars.condition_value + 5; return ctx.payload;"))
                    )
                    .addAction("a1", scriptTransform(mockScript("transform a1")), loggingAction("_text"))
                    .addAction("a2", scriptTransform(mockScript("transform a2")), loggingAction("_text"))
            )
            .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().trigger(watchId);

        flush();
        refresh();

        SearchResponse searchResponse = searchWatchRecords(builder -> {
            // defaults to match all;
        });

        assertHitCount(searchResponse, 1L);

        Map<String, Object> source = searchResponse.getHits().getAt(0).getSourceAsMap();

        assertValue(source, "watch_id", is(watchId));
        assertValue(source, "state", is("executed"));

        // we don't store the computed vars in history
        assertValue(source, "vars", nullValue());

        assertValue(source, "result.condition.status", is("success"));
        assertValue(source, "result.transform.status", is("success"));

        List<Map<String, Object>> actions = ObjectPath.eval("result.actions", source);
        for (Map<String, Object> action : actions) {
            String id = (String) action.get("id");
            switch (id) {
                case "a1" -> {
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a1_transformed_value", equalTo(25));
                }
                case "a2" -> {
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a2_transformed_value", equalTo(35));
                }
                default -> fail("there should not be an action result for action with an id other than a1 or a2");
            }
        }
    }

    public void testVarsManual() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId(watchId)
            .setSource(
                watchBuilder().trigger(schedule(cron("0/1 * * * * ? 2020")))
                    .input(simpleInput("value", 5))
                    .condition(
                        new ScriptCondition(
                            mockScript("ctx.vars.condition_value = ctx.payload.value + 5; return ctx.vars.condition_value > 5;")
                        )
                    )
                    .transform(
                        scriptTransform(mockScript("ctx.vars.watch_transform_value = ctx.vars.condition_value + 5; return ctx.payload;"))
                    )
                    .addAction("a1", scriptTransform(mockScript("transform a1")), loggingAction("_text"))
                    .addAction("a2", scriptTransform(mockScript("transform a2")), loggingAction("_text"))
            )
            .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        boolean debug = randomBoolean();

        ExecuteWatchResponse executeWatchResponse = new ExecuteWatchRequestBuilder(client()).setId(watchId).setDebug(debug).get();
        assertThat(executeWatchResponse.getRecordId(), notNullValue());
        XContentSource source = executeWatchResponse.getRecordSource();

        assertValue(source, "watch_id", is(watchId));
        assertValue(source, "state", is("executed"));

        if (debug) {
            assertValue(source, "vars.condition_value", is(10));
            assertValue(source, "vars.watch_transform_value", is(15));
            assertValue(source, "vars.a1_transform_value", is(25));
            assertValue(source, "vars.a2_transform_value", is(35));
        }

        assertValue(source, "result.condition.status", is("success"));
        assertValue(source, "result.transform.status", is("success"));

        List<Map<String, Object>> actions = source.getValue("result.actions");
        for (Map<String, Object> action : actions) {
            String id = (String) action.get("id");
            switch (id) {
                case "a1" -> {
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a1_transformed_value", equalTo(25));
                }
                case "a2" -> {
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a2_transformed_value", equalTo(35));
                }
                default -> fail("there should not be an action result for action with an id other than a1 or a2");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertValue(Map<String, Object> map, String path, Matcher<?> matcher) {
        assertThat(ObjectPath.eval(path, map), (Matcher<Object>) matcher);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.scriptCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.transform.TransformBuilders.scriptTransform;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class ExecutionVarsIT extends AbstractWatcherIntegrationTestCase {

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(GroovyPlugin.class);
        return types;
    }

    @Override
    protected boolean timeWarped() {
        return true;
    }

    public void testVars() throws Exception {
        WatcherClient watcherClient = watcherClient();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(cron("0/1 * * * * ?")))
                .input(simpleInput("value", 5))
                .condition(scriptCondition("ctx.vars.condition_value = ctx.payload.value + 5; return ctx.vars.condition_value > 5;"))
                .transform(scriptTransform("ctx.vars.watch_transform_value = ctx.vars.condition_value + 5; return ctx.payload;"))
                .addAction(
                        "a1",
                        scriptTransform("ctx.vars.a1_transform_value = ctx.vars.watch_transform_value + 10; ctx.payload" +
                                ".a1_transformed_value = ctx.vars.a1_transform_value; return ctx.payload;"),
                        loggingAction("_text"))
                .addAction(
                        "a2",
                        scriptTransform("ctx.vars.a2_transform_value = ctx.vars.watch_transform_value + 20; ctx.payload" +
                                ".a2_transformed_value = ctx.vars.a2_transform_value; return ctx.payload;"),
                        loggingAction("_text")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("_id");

        flush();
        refresh();

        SearchResponse searchResponse = searchWatchRecords(new Callback<SearchRequestBuilder>() {
            @Override
            public void handle(SearchRequestBuilder builder) {
                // defaults to match all;
            }
        });

        assertThat(searchResponse.getHits().getTotalHits(), is(1L));

        Map<String, Object> source = searchResponse.getHits().getAt(0).getSource();

        assertValue(source, "watch_id", is("_id"));
        assertValue(source, "state", is("executed"));

        // we don't store the computed vars in history
        assertValue(source, "vars", nullValue());

        assertValue(source, "result.condition.status", is("success"));
        assertValue(source, "result.transform.status", is("success"));

        List<Map<String, Object>> actions = ObjectPath.eval("result.actions", source);
        for (Map<String, Object> action : actions) {
            String id = (String) action.get("id");
            switch (id) {
                case "a1":
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a1_transformed_value", equalTo(25));
                    break;
                case "a2":
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a2_transformed_value", equalTo(35));
                    break;
                default:
                    fail("there should not be an action result for action with an id other than a1 or a2");
            }
        }
    }

    public void testVarsManual() throws Exception {
        WatcherClient watcherClient = watcherClient();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(cron("0/1 * * * * ? 2020")))
                .input(simpleInput("value", 5))
                .condition(scriptCondition("ctx.vars.condition_value = ctx.payload.value + 5; return ctx.vars.condition_value > 5;"))
                .transform(scriptTransform("ctx.vars.watch_transform_value = ctx.vars.condition_value + 5; return ctx.payload;"))
                .addAction(
                        "a1",
                        scriptTransform("ctx.vars.a1_transform_value = ctx.vars.watch_transform_value + 10; ctx.payload" +
                                ".a1_transformed_value = ctx.vars.a1_transform_value; return ctx.payload;"),
                        loggingAction("_text"))
                .addAction(
                        "a2",
                        scriptTransform("ctx.vars.a2_transform_value = ctx.vars.watch_transform_value + 20; ctx.payload" +
                                ".a2_transformed_value = ctx.vars.a2_transform_value; return ctx.payload;"),
                        loggingAction("_text")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        boolean debug = randomBoolean();

        ExecuteWatchResponse executeWatchResponse = watcherClient
                .prepareExecuteWatch("_id")
                .setDebug(debug)
                .get();
        assertThat(executeWatchResponse.getRecordId(), notNullValue());
        XContentSource source = executeWatchResponse.getRecordSource();

        assertValue(source, "watch_id", is("_id"));
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
                case "a1":
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a1_transformed_value", equalTo(25));
                    break;
                case "a2":
                    assertValue(action, "status", is("success"));
                    assertValue(action, "transform.status", is("success"));
                    assertValue(action, "transform.payload.a2_transformed_value", equalTo(35));
                    break;
                default:
                    fail("there should not be an action result for action with an id other than a1 or a2");
            }
        }
    }
}

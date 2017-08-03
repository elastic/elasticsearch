/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.transform.TransformBuilders.scriptTransform;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

/**
 * This test makes sure that the http host and path fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
@TestLogging("org.elasticsearch.xpack.watcher:DEBUG,org.elasticsearch.xpack.watcher.WatcherIndexingListener:TRACE")
public class HistoryTemplateTransformMappingsTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(CustomScriptPlugin.class);
        return types;
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("return [ 'key' : 'value1' ];", vars -> singletonMap("key", "value1"));
            scripts.put("return [ 'key' : 'value2' ];", vars -> singletonMap("key", "value2"));
            scripts.put("return [ 'key' : [ 'key1' : 'value1' ] ];", vars -> singletonMap("key", singletonMap("key1", "value1")));
            scripts.put("return [ 'key' : [ 'key1' : 'value2' ] ];", vars -> singletonMap("key", singletonMap("key1", "value2")));

            return scripts;
        }

        @Override
        public String pluginScriptLang() {
            return WATCHER_LANG;
        }
    }

    @Override
    protected boolean timeWarped() {
        return true; // just to have better control over the triggers
    }

    public void testTransformFields() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id1").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .transform(scriptTransform("return [ 'key' : 'value1' ];"))
                .addAction("logger", scriptTransform("return [ 'key' : 'value2' ];"), loggingAction("indexed")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id1");

        // adding another watch which with a transform that should conflict with the preview watch. Since the
        // mapping for the transform construct is disabled, there should be nor problems.
        putWatchResponse = watcherClient().preparePutWatch("_id2").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(simpleInput())
                .condition(AlwaysCondition.INSTANCE)
                .transform(scriptTransform("return [ 'key' : [ 'key1' : 'value1' ] ];"))
                .addAction("logger", scriptTransform("return [ 'key' : [ 'key1' : 'value2' ] ];"), loggingAction("indexed")))
                .get();
        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id2");

        flush();
        refresh();

        assertWatchWithMinimumActionsCount("_id1", ExecutionState.EXECUTED, 1);
        assertWatchWithMinimumActionsCount("_id2", ExecutionState.EXECUTED, 1);

        refresh();

        assertBusy(() -> {
            GetFieldMappingsResponse response = client().admin().indices()
                                                                .prepareGetFieldMappings(".watcher-history*")
                                                                .setFields("result.actions.transform.payload")
                                                                .setTypes("doc")
                                                                .includeDefaults(true)
                                                                .get();

            // time might have rolled over to a new day, thus we need to check that this field exists only in one of the history indices
            List<Boolean> payloadNulls = response.mappings().values().stream()
                    .map(map -> map.get("doc"))
                    .map(map -> map.get("result.actions.transform.payload"))
                    .filter(Objects::nonNull)
                    .map(GetFieldMappingsResponse.FieldMappingMetaData::isNull)
                    .collect(Collectors.toList());

            assertThat(payloadNulls, hasItem(true));
        });
    }
}

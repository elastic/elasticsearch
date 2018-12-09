/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.test.WatcherMockScriptPlugin;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class ManualExecutionTests extends AbstractWatcherIntegrationTestCase {

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

            scripts.put("sleep", vars -> {
                Number millis = (Number) XContentMapValues.extractValue("params.millis", vars);
                if (millis != null) {
                    try {
                        Thread.sleep(millis.longValue());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new RuntimeException("Unable to sleep, [millis] parameter is missing!");
                }
                return true;
            });
            return scripts;
        }

        @Override
        public String pluginScriptLang() {
            return WATCHER_LANG;
        }
    }

    public void testExecuteWatch() throws Exception {
        boolean ignoreCondition = randomBoolean();
        boolean recordExecution = randomBoolean();
        boolean conditionAlwaysTrue = randomBoolean();
        String action = randomFrom("_all", "log");

        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(conditionAlwaysTrue ? AlwaysCondition.INSTANCE : NeverCondition.INSTANCE)
                .addAction("log", loggingAction("foobar"));

        BytesReference bytesReference = watchBuilder.buildAsBytes(XContentType.JSON);
        watcherClient().putWatch(new PutWatchRequest("_id", bytesReference, XContentType.JSON)).actionGet();

        ExecuteWatchRequestBuilder executeWatchRequestBuilder = watcherClient().prepareExecuteWatch("_id");
        executeWatchRequestBuilder.setIgnoreCondition(ignoreCondition);
        executeWatchRequestBuilder.setRecordExecution(recordExecution);
        executeWatchRequestBuilder.setActionMode(action, ActionExecutionMode.SIMULATE);

        refresh();
        long oldRecordCount = docCount(HistoryStoreField.INDEX_PREFIX_WITH_TEMPLATE + "*", HistoryStoreField.DOC_TYPE, matchAllQuery());

        ExecuteWatchResponse executeWatchResponse = executeWatchRequestBuilder.get();
        Map<String, Object> responseMap = executeWatchResponse.getRecordSource().getAsMap();

        refresh();
        long newRecordCount = docCount(HistoryStoreField.INDEX_PREFIX_WITH_TEMPLATE + "*", HistoryStoreField.DOC_TYPE, matchAllQuery());
        long expectedCount = oldRecordCount + (recordExecution ? 1 : 0);

        assertThat("the expected count of history records should be [" + expectedCount + "]", newRecordCount, equalTo(expectedCount));

        List<Map<String, Object>> actions = ObjectPath.eval("result.actions", responseMap);
        if (ignoreCondition) {
            assertThat("The action should have run", actions.size(), equalTo(1));
        } else if (!conditionAlwaysTrue) {
            assertThat("The action should not have run", actions.size(), equalTo(0));
        }

        if (ignoreCondition || conditionAlwaysTrue) {
            assertThat("The action should have run simulated", actions.get(0).get("status"), is("simulated"));
        }

        if (recordExecution) {
            GetWatchResponse response =  watcherClient().getWatch(new GetWatchRequest("_id")).actionGet();
            if (ignoreCondition || conditionAlwaysTrue) {
                assertThat(response.getStatus().actionStatus("log").ackStatus().state(), is(ActionStatus.AckStatus.State.ACKABLE));
            } else {
                assertThat(response.getStatus().actionStatus("log").ackStatus().state(),
                        is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
            }
        } else {
            String ackState = executeWatchResponse.getRecordSource().getValue("status.actions.log.ack.state");
            if (ignoreCondition || conditionAlwaysTrue) {
                assertThat(ackState, is(ActionStatus.AckStatus.State.ACKABLE.toString().toLowerCase(Locale.ROOT)));
            } else {
                assertThat(ackState, is(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION.toString().toLowerCase(Locale.ROOT)));
            }
        }
    }

    public void testExecutionWithInlineWatch() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(AlwaysCondition.INSTANCE)
                .addAction("log", loggingAction("foobar"));

        ExecuteWatchRequestBuilder builder = watcherClient().prepareExecuteWatch()
                .setWatchSource(watchBuilder);
        if (randomBoolean()) {
            builder.setRecordExecution(false);
        }
        if (randomBoolean()) {
            DateTime now = new DateTime(DateTimeZone.UTC);
            builder.setTriggerEvent(new ScheduleTriggerEvent(now, now));
        }

        ExecuteWatchResponse executeWatchResponse = builder.get();
        assertThat(executeWatchResponse.getRecordId(), startsWith(ExecuteWatchRequest.INLINE_WATCH_ID));
        assertThat(executeWatchResponse.getRecordSource().getValue("watch_id").toString(), equalTo(ExecuteWatchRequest.INLINE_WATCH_ID));
        assertThat(executeWatchResponse.getRecordSource().getValue("state").toString(), equalTo("executed"));
        assertThat(executeWatchResponse.getRecordSource().getValue("trigger_event.type").toString(), equalTo("manual"));
    }

    public void testExecutionWithInlineWatchWithRecordExecutionEnabled() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(AlwaysCondition.INSTANCE)
                .addAction("log", loggingAction("foobar"));

        ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, () ->
                watcherClient().prepareExecuteWatch()
                .setWatchSource(watchBuilder)
                .setRecordExecution(true)
                .setTriggerEvent(new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)))
                .get());
        assertThat(e.getMessage(), containsString("the execution of an inline watch cannot be recorded"));
    }

    public void testExecutionWithInlineWatch_withWatchId() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(AlwaysCondition.INSTANCE)
                .addAction("log", loggingAction("foobar"));

        try {
            watcherClient().prepareExecuteWatch()
                    .setId("_id")
                    .setWatchSource(watchBuilder)
                    .setRecordExecution(false)
                    .setTriggerEvent(new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)))
                    .get();
            fail();
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(),
                    containsString("a watch execution request must either have a watch id or an inline watch source but not both"));
        }
    }

    public void testDifferentAlternativeInputs() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .addAction("log", loggingAction("foobar"));

        BytesReference bytesReference = watchBuilder.buildAsBytes(XContentType.JSON);
        watcherClient().putWatch(new PutWatchRequest("_id", bytesReference, XContentType.JSON)).actionGet();
        refresh(Watch.INDEX);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("foo", "bar");

        Map<String, Object> map2 = new HashMap<>();
        map2.put("foo", map1);

        ExecuteWatchResponse firstResponse = watcherClient().prepareExecuteWatch("_id")
                .setActionMode("_all", ActionExecutionMode.SIMULATE)
                .setAlternativeInput(map1)
                .setRecordExecution(true)
                .setTriggerEvent(new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)))
                .get();

        ExecuteWatchResponse secondResponse = watcherClient().prepareExecuteWatch("_id")
                .setActionMode("_all", ActionExecutionMode.SIMULATE)
                .setAlternativeInput(map2)
                .setRecordExecution(true)
                .setTriggerEvent(new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)))
                .get();

        String firstPayload = ObjectPath.eval("result.input.payload.foo", firstResponse.getRecordSource().getAsMap());
        assertThat(firstPayload, is("bar"));
        Map<String, String> secondPayload = ObjectPath.eval("result.input.payload", secondResponse.getRecordSource().getAsMap());
        assertThat(secondPayload, instanceOf(Map.class));
    }

    public void testExecutionRequestDefaults() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(NeverCondition.INSTANCE)
                .defaultThrottlePeriod(TimeValue.timeValueHours(1))
                .addAction("log", loggingAction("foobar"));

        BytesReference bytesReference = watchBuilder.buildAsBytes(XContentType.JSON);
        watcherClient().putWatch(new PutWatchRequest("_id", bytesReference, XContentType.JSON)).actionGet();

        DateTime now = new DateTime(Clock.systemUTC().millis());
        TriggerEvent triggerEvent = new ScheduleTriggerEvent(now, now);

        Map<String, Object> executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id")
                .setTriggerEvent(triggerEvent)
                .get().getRecordSource().getAsMap();

        assertThat(ObjectPath.eval("state", executeWatchResult), equalTo(ExecutionState.EXECUTION_NOT_NEEDED.toString()));
        assertThat(ObjectPath.eval("result.input.payload.foo", executeWatchResult), equalTo("bar"));

        watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(AlwaysCondition.INSTANCE)
                .defaultThrottlePeriod(TimeValue.timeValueHours(1))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder.buildAsBytes(XContentType.JSON), XContentType.JSON)).actionGet();

        executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id").setTriggerEvent(triggerEvent).setRecordExecution(true)
                .get().getRecordSource().getAsMap();

        assertThat(ObjectPath.eval("state", executeWatchResult), equalTo(ExecutionState.EXECUTED.toString()));
        assertThat(ObjectPath.eval("result.input.payload.foo", executeWatchResult), equalTo("bar"));
        assertThat(ObjectPath.eval("result.actions.0.id", executeWatchResult), equalTo("log"));

        executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id").setTriggerEvent(triggerEvent)
                .get().getRecordSource().getAsMap();

        assertThat(ObjectPath.eval("state", executeWatchResult), equalTo(ExecutionState.THROTTLED.toString()));
    }

    public void testWatchExecutionDuration() throws Exception {
        Script script = new Script(ScriptType.INLINE, WATCHER_LANG, "sleep", singletonMap("millis", 100L));
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition(script))
                .addAction("log", loggingAction("foobar"));

        watcherClient().preparePutWatch("_id").setSource(watchBuilder).get();
        refresh(Watch.INDEX);

        ScheduleTriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC));
        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch("_id").setTriggerEvent(triggerEvent).get();
        Integer duration = ObjectPath.eval("result.execution_duration", executeWatchResponse.getRecordSource().getAsMap());

        assertThat(duration, greaterThanOrEqualTo(100));
    }
}

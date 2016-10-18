/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.support.clock.SystemClock;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.condition.ScriptCondition;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class ManualExecutionTests extends AbstractWatcherIntegrationTestCase {

    @Override
    protected boolean enableSecurity() {
        return false;
    }

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

            scripts.put("sleep", vars -> {
                Number millis = (Number) XContentMapValues.extractValue("millis", vars);
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

        ManualExecutionContext.Builder ctxBuilder;
        Watch parsedWatch = null;
        ManualTriggerEvent triggerEvent = new ManualTriggerEvent("_id",
                new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)));
        if (recordExecution) {
            PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();
            assertThat(putWatchResponse.getVersion(), greaterThan(0L));
            refresh();
            assertThat(watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().isFound(), equalTo(true));
            //If we are persisting the state we need to use the exact watch that is in memory
            ctxBuilder = ManualExecutionContext.builder(watchService().getWatch("_id"), true, triggerEvent, timeValueSeconds(5));
        } else {
            parsedWatch = watchParser().parse("_id", false, watchBuilder.buildAsBytes(XContentType.JSON));
            ctxBuilder = ManualExecutionContext.builder(parsedWatch, false, triggerEvent, timeValueSeconds(5));
        }

        if (ignoreCondition) {
            ctxBuilder.withCondition(AlwaysCondition.RESULT_INSTANCE);
        }

        ctxBuilder.recordExecution(recordExecution);


        if ("_all".equals(action)) {
            ctxBuilder.allActionsMode(ActionExecutionMode.SIMULATE);
        } else {
            ctxBuilder.actionMode(action, ActionExecutionMode.SIMULATE);
        }

        ManualExecutionContext ctx = ctxBuilder.build();

        refresh();
        long oldRecordCount = docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*", HistoryStore.DOC_TYPE, matchAllQuery());

        WatchRecord watchRecord = executionService().execute(ctx);

        refresh();

        long newRecordCount = docCount(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*", HistoryStore.DOC_TYPE, matchAllQuery());
        long expectedCount = oldRecordCount + (recordExecution ? 1 : 0);

        assertThat("the expected count of history records should be [" + expectedCount + "]", newRecordCount, equalTo(expectedCount));

        if (ignoreCondition) {
            assertThat("The action should have run", watchRecord.result().actionsResults().size(), equalTo(1));
        } else if (!conditionAlwaysTrue) {
            assertThat("The action should not have run", watchRecord.result().actionsResults().size(), equalTo(0));
        }

        if ((ignoreCondition || conditionAlwaysTrue) && action == null) {
            assertThat("The action should have run non simulated", watchRecord.result().actionsResults().get("log").action(),
            not(instanceOf(LoggingAction.Result.Simulated.class)) );
        }

        if ((ignoreCondition || conditionAlwaysTrue) && action != null ) {
            assertThat("The action should have run simulated",
                    watchRecord.result().actionsResults().get("log").action(), instanceOf(LoggingAction.Result.Simulated.class));
        }

        Watch testWatch = watchService().getWatch("_id");
        if (recordExecution) {
            refresh();
            if (ignoreCondition || conditionAlwaysTrue) {
                assertThat(testWatch.status().actionStatus("log").ackStatus().state(), equalTo(ActionStatus.AckStatus.State.ACKABLE));
                GetWatchResponse response =  watcherClient().getWatch(new GetWatchRequest("_id")).actionGet();
                assertThat(response.getStatus().actionStatus("log").ackStatus().state(), equalTo(ActionStatus.AckStatus.State.ACKABLE));
            } else {
                assertThat(testWatch.status().actionStatus("log").ackStatus().state(),
                        equalTo(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
            }
        } else {
            assertThat(parsedWatch.status().actionStatus("log").ackStatus().state(),
                    equalTo(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
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
            builder.setTriggerEvent(new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)));
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

        try {
            watcherClient().prepareExecuteWatch()
                    .setWatchSource(watchBuilder)
                    .setRecordExecution(true)
                    .setTriggerEvent(new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)))
                    .get();
            fail();
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("the execution of an inline watch cannot be recorded"));
        }
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

        PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();
        assertThat(putWatchResponse.getVersion(), greaterThan(0L));
        refresh();
        assertThat(watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().isFound(), equalTo(true));

        Map<String, Object> map1 = new HashMap<>();
        map1.put("foo", "bar");

        Map<String, Object> map2 = new HashMap<>();
        map2.put("foo", map1);

        ManualTriggerEvent triggerEvent = new ManualTriggerEvent("_id",
                new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)));
        ManualExecutionContext.Builder ctxBuilder1 = ManualExecutionContext.builder(watchService().getWatch("_id"), true, triggerEvent,
                timeValueSeconds(5));
        ctxBuilder1.actionMode("_all", ActionExecutionMode.SIMULATE);

        ctxBuilder1.withInput(new SimpleInput.Result(new Payload.Simple(map1)));
        ctxBuilder1.recordExecution(true);

        WatchRecord watchRecord1 = executionService().execute(ctxBuilder1.build());

        ManualExecutionContext.Builder ctxBuilder2 = ManualExecutionContext.builder(watchService().getWatch("_id"), true, triggerEvent,
                timeValueSeconds(5));
        ctxBuilder2.actionMode("_all", ActionExecutionMode.SIMULATE);

        ctxBuilder2.withInput(new SimpleInput.Result(new Payload.Simple(map2)));
        ctxBuilder2.recordExecution(true);

        WatchRecord watchRecord2 = executionService().execute(ctxBuilder2.build());

        assertThat(watchRecord1.result().inputResult().payload().data().get("foo").toString(), equalTo("bar"));
        assertThat(watchRecord2.result().inputResult().payload().data().get("foo"), instanceOf(Map.class));
    }

    public void testExecutionRequestDefaults() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(NeverCondition.INSTANCE)
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(SystemClock.INSTANCE.nowUTC(), SystemClock.INSTANCE.nowUTC());

        Map<String, Object> executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id")
                .setTriggerEvent(triggerEvent)
                .get().getRecordSource().getAsMap();


        assertThat(ObjectPath.<String>eval("state", executeWatchResult), equalTo(ExecutionState.EXECUTION_NOT_NEEDED.toString()));
        assertThat(ObjectPath.<String>eval("result.input.payload.foo", executeWatchResult), equalTo("bar"));

        watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(AlwaysCondition.INSTANCE)
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();

        executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id").setTriggerEvent(triggerEvent).setRecordExecution(true)
                .get().getRecordSource().getAsMap();

        assertThat(ObjectPath.<String>eval("state", executeWatchResult), equalTo(ExecutionState.EXECUTED.toString()));
        assertThat(ObjectPath.<String>eval("result.input.payload.foo", executeWatchResult), equalTo("bar"));
        assertThat(ObjectPath.<String>eval("result.actions.0.id", executeWatchResult), equalTo("log"));

        executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id").setTriggerEvent(triggerEvent)
                .get().getRecordSource().getAsMap();

        assertThat(ObjectPath.<String>eval("state", executeWatchResult), equalTo(ExecutionState.THROTTLED.toString()));
    }

    public void testWatchExecutionDuration() throws Exception {
        Script script = new Script("sleep", ScriptService.ScriptType.INLINE, null, singletonMap("millis", 100L));
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition(script))
                .addAction("log", loggingAction("foobar"));

        Watch watch = watchParser().parse("_id", false, watchBuilder.buildAsBytes(XContentType.JSON));
        ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(watch, false, new ManualTriggerEvent("_id",
                        new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC))),
                new TimeValue(1, TimeUnit.HOURS));
        WatchRecord record = executionService().execute(ctxBuilder.build());
        assertThat(record.result().executionDurationMs(), greaterThanOrEqualTo(100L));
    }

    public void testForceDeletionOfLongRunningWatch() throws Exception {
        Script script = new Script("sleep", ScriptService.ScriptType.INLINE,  null, singletonMap("millis", 10000L));
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition(script))
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));

        PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();
        assertThat(putWatchResponse.getVersion(), greaterThan(0L));
        refresh();
        assertThat(watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().isFound(), equalTo(true));

        int numberOfThreads = scaledRandomIntBetween(1, 5);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; ++i) {
            threads.add(new Thread(new ExecutionRunner(watchService(), executionService(), "_id", startLatch)));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        DeleteWatchResponse deleteWatchResponse = watcherClient().prepareDeleteWatch("_id").get();
        assertThat(deleteWatchResponse.isFound(), is(true));

        deleteWatchResponse = watcherClient().prepareDeleteWatch("_id").get();
        assertThat(deleteWatchResponse.isFound(), is(false));

        startLatch.countDown();

        long startJoin = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.join(30_000L);
            assertFalse(thread.isAlive());
        }
        long endJoin = System.currentTimeMillis();
        TimeValue tv = new TimeValue(10 * (numberOfThreads+1), TimeUnit.SECONDS);
        assertThat("Shouldn't take longer than [" + tv.getSeconds() + "] seconds for all the threads to stop", (endJoin - startJoin),
                lessThan(tv.getMillis()));
    }

    public static class ExecutionRunner implements Runnable {

        final WatcherService watcherService;
        final ExecutionService executionService;
        final String watchId;
        final CountDownLatch startLatch;
        final ManualExecutionContext.Builder ctxBuilder;

        public ExecutionRunner(WatcherService watcherService, ExecutionService executionService, String watchId,
                               CountDownLatch startLatch) {
            this.watcherService = watcherService;
            this.executionService = executionService;
            this.watchId = watchId;
            this.startLatch = startLatch;
            ManualTriggerEvent triggerEvent = new ManualTriggerEvent(watchId,
                    new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC)));
            ctxBuilder = ManualExecutionContext.builder(watcherService.getWatch(watchId), true, triggerEvent, timeValueSeconds(5));
            ctxBuilder.recordExecution(true);
            ctxBuilder.actionMode("_all", ActionExecutionMode.FORCE_EXECUTE);
        }

        @Override
        public void run() {
            try {
                startLatch.await();
                WatchRecord record = executionService.execute(ctxBuilder.build());
                assertThat(record, notNullValue());
                assertThat(record.state(), is(ExecutionState.NOT_EXECUTED_WATCH_MISSING));
            } catch (Exception e) {
                throw new ElasticsearchException("Failure mode execution of [{}] failed in an unexpected way", e, watchId);
            }
        }
    }

}

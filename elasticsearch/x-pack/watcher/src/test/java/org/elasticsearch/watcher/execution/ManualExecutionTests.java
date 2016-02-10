/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.condition.ConditionBuilders.neverCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;


public class ManualExecutionTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean enableShield() {
        return false;
    }

    public void testExecuteWatch() throws Exception {
        boolean ignoreCondition = randomBoolean();
        boolean recordExecution = randomBoolean();
        boolean conditionAlwaysTrue = randomBoolean();
        String action = randomFrom("_all", "log");

        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(conditionAlwaysTrue ? alwaysCondition() : neverCondition())
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
            ctxBuilder.withCondition(AlwaysCondition.Result.INSTANCE);
        }

        ctxBuilder.recordExecution(recordExecution);


        if ("_all".equals(action)) {
            ctxBuilder.allActionsMode(ActionExecutionMode.SIMULATE);
        } else {
            ctxBuilder.actionMode(action, ActionExecutionMode.SIMULATE);
        }

        ManualExecutionContext ctx = ctxBuilder.build();

        refresh();
        long oldRecordCount = docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE, matchAllQuery());

        WatchRecord watchRecord = executionService().execute(ctx);

        refresh();

        long newRecordCount = docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE, matchAllQuery());
        long expectedCount = oldRecordCount + (recordExecution ? 1 : 0);

        assertThat("the expected count of history records should be [" + expectedCount + "]", newRecordCount, equalTo(expectedCount));

        if (ignoreCondition) {
            assertThat("The action should have run", watchRecord.result().actionsResults().count(), equalTo(1));
        } else if (!conditionAlwaysTrue) {
            assertThat("The action should not have run", watchRecord.result().actionsResults().count(), equalTo(0));
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
                .condition(alwaysCondition())
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
                .condition(alwaysCondition())
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
                .condition(alwaysCondition())
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
                .condition(neverCondition())
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(SystemClock.INSTANCE.nowUTC(), SystemClock.INSTANCE.nowUTC());

        Wid wid = new Wid("_watchId",1, SystemClock.INSTANCE.nowUTC());


        Map<String, Object> executeWatchResult = watcherClient().prepareExecuteWatch()
                .setId("_id")
                .setTriggerEvent(triggerEvent)
                .get().getRecordSource().getAsMap();


        assertThat(ObjectPath.<String>eval("state", executeWatchResult), equalTo(ExecutionState.EXECUTION_NOT_NEEDED.toString()));
        assertThat(ObjectPath.<String>eval("result.input.payload.foo", executeWatchResult), equalTo("bar"));

        watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(alwaysCondition())
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
            } catch (Throwable t) {
                throw new ElasticsearchException("Failure mode execution of [{}] failed in an unexpected way", t, watchId);
            }
        }
    }

}

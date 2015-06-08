/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.delete.DeleteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.condition.ConditionBuilders.neverCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.*;

public class ManualExecutionTests extends AbstractWatcherIntegrationTests {

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Test @Repeat(iterations = 10)
    public void testExecuteWatch() throws Exception {
        ensureWatcherStarted();
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
        ManualTriggerEvent triggerEvent = new ManualTriggerEvent("_id", new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC)));
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
            assertThat("The action should have run simulated", watchRecord.result().actionsResults().get("log").action(), instanceOf(LoggingAction.Result.Simulated.class));
        }

        Watch testWatch = watchService().getWatch("_id");
        if (recordExecution) {
            refresh();
            Watch persistedWatch = watchParser().parse("_id", true, watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().getSource().getBytes());
            if (ignoreCondition || conditionAlwaysTrue) {
                assertThat(testWatch.status().actionStatus("log").ackStatus().state(), equalTo(ActionStatus.AckStatus.State.ACKABLE));
                assertThat(persistedWatch.status().actionStatus("log").ackStatus().state(), equalTo(ActionStatus.AckStatus.State.ACKABLE));
            } else {
                assertThat(testWatch.status().actionStatus("log").ackStatus().state(), equalTo(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
            }
        } else {
            assertThat(parsedWatch.status().actionStatus("log").ackStatus().state(), equalTo(ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        }
    }

    @Test
    public void testDifferentAlternativeInputs() throws Exception {
        ensureWatcherStarted();
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

        ManualTriggerEvent triggerEvent = new ManualTriggerEvent("_id", new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC)));
        ManualExecutionContext.Builder ctxBuilder1 = ManualExecutionContext.builder(watchService().getWatch("_id"), true, triggerEvent, timeValueSeconds(5));
        ctxBuilder1.actionMode("_all", ActionExecutionMode.SIMULATE);

        ctxBuilder1.withInput(new SimpleInput.Result(new Payload.Simple(map1)));
        ctxBuilder1.recordExecution(true);

        WatchRecord watchRecord1 = executionService().execute(ctxBuilder1.build());

        ManualExecutionContext.Builder ctxBuilder2 = ManualExecutionContext.builder(watchService().getWatch("_id"), true, triggerEvent, timeValueSeconds(5));
        ctxBuilder2.actionMode("_all", ActionExecutionMode.SIMULATE);

        ctxBuilder2.withInput(new SimpleInput.Result(new Payload.Simple(map2)));
        ctxBuilder2.recordExecution(true);

        WatchRecord watchRecord2 = executionService().execute(ctxBuilder2.build());

        assertThat(watchRecord1.result().inputResult().payload().data().get("foo").toString(), equalTo("bar"));
        assertThat(watchRecord2.result().inputResult().payload().data().get("foo"), instanceOf(Map.class));
    }

    @Test
    public void testExecutionRequestDefaults() throws Exception {
        ensureWatcherStarted();

        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(neverCondition())
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC));

        Wid wid = new Wid("_watchId",1,new DateTime());


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

    @Test
    public void testWatchExecutionDuration() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition((new Script.Builder.Inline("sleep 100; return true")).build()))
                .addAction("log", loggingAction("foobar"));

        Watch watch = watchParser().parse("_id", false, watchBuilder.buildAsBytes(XContentType.JSON));
        ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(watch, false, new ManualTriggerEvent("_id", new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC))), new TimeValue(1, TimeUnit.HOURS));
        WatchRecord record = executionService().execute(ctxBuilder.build());
        assertThat(record.result().executionDurationMs(), greaterThanOrEqualTo(100L));
    }

    @Test @Slow
    public void testForceDeletionOfLongRunningWatch() throws Exception {
        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(new ScriptCondition((new Script.Builder.Inline("sleep 10000; return true")).build()))
                .defaultThrottlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));

        int numberOfThreads = scaledRandomIntBetween(1, 5);
        PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();
        assertThat(putWatchResponse.getVersion(), greaterThan(0L));
        refresh();
        assertThat(watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().isFound(), equalTo(true));

        CountDownLatch startLatch = new CountDownLatch(1);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < numberOfThreads; ++i) {
            threads.add(new Thread(new ExecutionRunner(watchService(), executionService(), "_id", startLatch)));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        DeleteWatchResponse deleteWatchResponse = watcherClient().prepareDeleteWatch("_id").setForce(true).get();
        assertThat(deleteWatchResponse.isFound(), is(true));

        deleteWatchResponse = watcherClient().prepareDeleteWatch("_id").get();
        assertThat(deleteWatchResponse.isFound(), is(false));

        startLatch.countDown();

        long startJoin = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.join();
        }
        long endJoin = System.currentTimeMillis();
        TimeValue tv = new TimeValue(10 * (numberOfThreads+1), TimeUnit.SECONDS);
        assertThat("Shouldn't take longer than [" + tv.getSeconds() + "] seconds for all the threads to stop", (endJoin - startJoin), lessThan(tv.getMillis()));
    }

    private static class ExecutionRunner implements Runnable {

        final WatcherService watcherService;
        final ExecutionService executionService;
        final String watchId;
        final CountDownLatch startLatch;
        final ManualExecutionContext.Builder ctxBuilder;

        private ExecutionRunner(WatcherService watcherService, ExecutionService executionService, String watchId, CountDownLatch startLatch) {
            this.watcherService = watcherService;
            this.executionService = executionService;
            this.watchId = watchId;
            this.startLatch = startLatch;
            ManualTriggerEvent triggerEvent = new ManualTriggerEvent(watchId, new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC)));
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
                throw new WatcherException("Failure mode execution of [{}] failed in an unexpected way", t, watchId);
            }
        }
    }

}

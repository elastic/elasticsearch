/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        String actionIdToSimulate = randomFrom("_all", "log", null);

        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(conditionAlwaysTrue ? alwaysCondition() : neverCondition())
                .addAction("log", loggingAction("foobar"));

        ManualExecutionContext.Builder ctxBuilder;
        Watch parsedWatch = null;
        if (recordExecution) {
            PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();
            assertThat(putWatchResponse.getVersion(), greaterThan(0L));
            refresh();
            assertThat(watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().isFound(), equalTo(true));
            ctxBuilder = ManualExecutionContext.builder(watchService().getWatch("_id")); //If we are persisting the state we need to use the exact watch that is in memory
        } else {
            parsedWatch = watchParser().parse("_id", false, watchBuilder.buildAsBytes(XContentType.JSON));
            ctxBuilder = ManualExecutionContext.builder(parsedWatch);
        }

        if (ignoreCondition) {
            ctxBuilder.withCondition(AlwaysCondition.Result.INSTANCE);
        }

        ctxBuilder.recordExecution(recordExecution);

        if (actionIdToSimulate != null) {
            if ("_all".equals(actionIdToSimulate)) {
                ctxBuilder.simulateAllActions();
            } else {
                ctxBuilder.simulateActions(actionIdToSimulate);
            }
        }

        refresh();
        long oldRecordCount = docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE, matchAllQuery());

        WatchRecord watchRecord = executionService().execute(ctxBuilder.build());

        refresh();

        long newRecordCount = docCount(HistoryStore.INDEX_PREFIX + "*", HistoryStore.DOC_TYPE, matchAllQuery());
        long expectedCount = oldRecordCount + (recordExecution ? 1 : 0);

        assertThat("the expected count of history records should be [" + expectedCount + "]", newRecordCount, equalTo(expectedCount));

        if (ignoreCondition) {
            assertThat("The action should have run", watchRecord.execution().actionsResults().count(), equalTo(1));
        } else if (!conditionAlwaysTrue) {
            assertThat("The action should not have run", watchRecord.execution().actionsResults().count(), equalTo(0));
        }

        if ((ignoreCondition || conditionAlwaysTrue) && actionIdToSimulate == null) {
            assertThat("The action should have run non simulated", watchRecord.execution().actionsResults().get("log").action(),
            not(instanceOf(LoggingAction.Result.Simulated.class)) );
        }

        if ((ignoreCondition || conditionAlwaysTrue) && actionIdToSimulate != null ) {
            assertThat("The action should have run simulated", watchRecord.execution().actionsResults().get("log").action(), instanceOf(LoggingAction.Result.Simulated.class));
        }

        Watch testWatch = watchService().getWatch("_id");
        if (recordExecution) {
            refresh();
            Watch persistedWatch = watchParser().parse("_id", true, watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().getSource().getBytes());
            if (ignoreCondition || conditionAlwaysTrue) {
                assertThat(testWatch.status().ackStatus().state(), equalTo(Watch.Status.AckStatus.State.ACKABLE));
                assertThat(persistedWatch.status().ackStatus().state(), equalTo(Watch.Status.AckStatus.State.ACKABLE));
            } else {
                assertThat(testWatch.status().ackStatus().state(), equalTo(Watch.Status.AckStatus.State.AWAITS_EXECUTION));
            }
        } else {
            assertThat(parsedWatch.status().ackStatus().state(), equalTo(Watch.Status.AckStatus.State.AWAITS_EXECUTION));
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

        ManualExecutionContext.Builder ctxBuilder1 = ManualExecutionContext.builder(watchService().getWatch("_id"));
        ctxBuilder1.simulateActions("_all");
        ctxBuilder1.withInput(new SimpleInput.Result(new Payload.Simple(map1)));
        ctxBuilder1.recordExecution(true);

        WatchRecord watchRecord1 = executionService().execute(ctxBuilder1.build());

        ManualExecutionContext.Builder ctxBuilder2 = ManualExecutionContext.builder(watchService().getWatch("_id"));
        ctxBuilder2.simulateActions("_all");
        ctxBuilder2.withInput(new SimpleInput.Result(new Payload.Simple(map2)));
        ctxBuilder2.recordExecution(true);

        WatchRecord watchRecord2 = executionService().execute(ctxBuilder2.build());

        assertThat(watchRecord1.execution().inputResult().payload().data().get("foo").toString(), equalTo("bar"));
        assertThat(watchRecord2.execution().inputResult().payload().data().get("foo"), instanceOf(Map.class));
    }

    @Test
    public void testExecutionRequestDefaults() throws Exception {
        ensureWatcherStarted();
        WatchRecord.Parser watchRecordParser = internalTestCluster().getInstance(WatchRecord.Parser.class);

        WatchSourceBuilder watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(neverCondition())
                .throttlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();

        Wid wid = new Wid("_watchId",1,new DateTime());
        ExecuteWatchResponse executeWatchResponse = watcherClient().prepareExecuteWatch().setId("_id").get();
        WatchRecord watchRecord = watchRecordParser.parse(wid.value(), 1, executeWatchResponse.getSource().getBytes());

        assertThat(watchRecord.state(), equalTo(WatchRecord.State.EXECUTION_NOT_NEEDED));
        assertThat(watchRecord.execution().inputResult().payload().data().get("foo").toString(), equalTo("bar"));

        watchBuilder = watchBuilder()
                .trigger(schedule(cron("0 0 0 1 * ? 2099")))
                .input(simpleInput("foo", "bar"))
                .condition(alwaysCondition())
                .throttlePeriod(new TimeValue(1, TimeUnit.HOURS))
                .addAction("log", loggingAction("foobar"));
        watcherClient().putWatch(new PutWatchRequest("_id", watchBuilder)).actionGet();

        executeWatchResponse = watcherClient().prepareExecuteWatch().setId("_id").setRecordExecution(true).get();
        watchRecord = watchRecordParser.parse(wid.value(), 1, executeWatchResponse.getSource().getBytes());

        assertThat(watchRecord.state(), equalTo(WatchRecord.State.EXECUTED));
        assertThat(watchRecord.execution().inputResult().payload().data().get("foo").toString(), equalTo("bar"));
        assertThat(watchRecord.execution().actionsResults().get("log"), not(instanceOf(LoggingAction.Result.Simulated.class)));

        executeWatchResponse = watcherClient().prepareExecuteWatch().setId("_id").get();
        watchRecord = watchRecordParser.parse(wid.value(), 1, executeWatchResponse.getSource().getBytes());

        assertThat(watchRecord.state(), equalTo(WatchRecord.State.THROTTLED));
    }
}

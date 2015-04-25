/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

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
            Watch persistedWatch = watchParser().parse("_id", true, watcherClient().getWatch(new GetWatchRequest("_id")).actionGet().getSource());
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
}

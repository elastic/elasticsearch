/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.client.WatchSourceBuilder;
import org.elasticsearch.watcher.condition.simple.AlwaysFalseCondition;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.simple.SimpleInput;
import org.elasticsearch.watcher.support.template.ScriptTemplate;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.elasticsearch.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.watch.Watch;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.SUITE, randomDynamicTemplates = false)
public class ManualExecutionTests extends AbstractWatcherIntegrationTests {

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Test
    @Repeat(iterations = 10)
    public void testExecuteWatch() throws Exception {
        ensureWatcherStarted();
        boolean ignoreCondition = randomBoolean();
        boolean persistRecord = randomBoolean();
        boolean conditionAlwaysTrue = randomBoolean();
        boolean storeWatch = randomBoolean();
        String actionIdToSimulate = randomFrom("_all", "logging", null);

        LoggingAction.SourceBuilder loggingAction = new LoggingAction.SourceBuilder("logging");
        loggingAction.text(new ScriptTemplate.SourceBuilder("foobar"));
        WatchSourceBuilder testWatchBuilder = new WatchSourceBuilder();
        testWatchBuilder.trigger(new ScheduleTrigger.SourceBuilder(new IntervalSchedule(new IntervalSchedule.Interval(100, IntervalSchedule.Interval.Unit.DAYS))));
        testWatchBuilder.condition(conditionAlwaysTrue ? new AlwaysTrueCondition.SourceBuilder() : new AlwaysFalseCondition.SourceBuilder());
        testWatchBuilder.addAction(loggingAction);
        testWatchBuilder.input(new SimpleInput.SourceBuilder((new Payload.Simple("foo", "bar").data())));

        if (storeWatch) {
            PutWatchResponse putWatchResponse = watcherClient().putWatch(new PutWatchRequest("testrun", testWatchBuilder)).actionGet();
            assertThat(putWatchResponse.getVersion(), greaterThan(0L));
            refresh();
            assertThat(watcherClient().getWatch(new GetWatchRequest("testrun")).actionGet().isFound(), equalTo(true));
        }

        Watch testWatch = watchParser().parse("testwatch", false, testWatchBuilder.buildAsBytes(XContentType.JSON));

        ManualExecutionContext.Builder ctxBuilder = ManualExecutionContext.builder(testWatch);

        if (ignoreCondition) {
            ctxBuilder.withCondition(AlwaysTrueCondition.RESULT);
        }

        ctxBuilder.recordInHistory(persistRecord);

        if (actionIdToSimulate != null) {
            if ("_all".equals(actionIdToSimulate)) {
                ctxBuilder.simulateAllActions();
            } else {
                ctxBuilder.simulateActions(actionIdToSimulate);
            }
        }

        refresh();
        long oldRecordCount = 0;
        oldRecordCount = client().count(new CountRequest(HistoryStore.INDEX_PREFIX + "*")).actionGet().getCount();

        WatchRecord watchRecord = executionService().execute(ctxBuilder.build());

        refresh();
        long newRecordCount = client().count(new CountRequest(HistoryStore.INDEX_PREFIX + "*")).actionGet().getCount();
        long expectedCount = oldRecordCount + (persistRecord ? 1 : 0);
        assertThat("the expected count of history records should be [" + expectedCount + "]", newRecordCount, equalTo(expectedCount));

        if (ignoreCondition) {
            assertThat("The action should have run", watchRecord.execution().actionsResults().count(), equalTo(1));
        } else if (!conditionAlwaysTrue) {
            assertThat("The action should not have run", watchRecord.execution().actionsResults().count(), equalTo(0));
        }

        if ((ignoreCondition || conditionAlwaysTrue) && actionIdToSimulate == null) {
            assertThat("The action should have run non simulated", watchRecord.execution().actionsResults().get("logging").action(),
            not(instanceOf(LoggingAction.Result.Simulated.class)) );
        }

        if ((ignoreCondition || conditionAlwaysTrue) && actionIdToSimulate != null ) {
            assertThat("The action should have run simulated", watchRecord.execution().actionsResults().get("logging").action(), instanceOf(LoggingAction.Result.Simulated.class));
        }
    }
}

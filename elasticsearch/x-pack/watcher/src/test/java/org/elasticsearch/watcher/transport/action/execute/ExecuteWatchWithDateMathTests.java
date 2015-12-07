/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.action.execute;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
public class ExecuteWatchWithDateMathTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean timeWarped() {
        return true;
    }

    public void testExecuteCustomTriggerData() throws Exception {
        WatcherClient watcherClient = watcherClient();

        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch()
                .setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0/5 * * * * ? 2099")))
                        .input(simpleInput("foo", "bar"))
                        .condition(alwaysCondition())
                        .addAction("log", loggingAction("_text")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        DateTime triggeredTime = timeWarp().clock().nowUTC();
        DateTime scheduledTime = triggeredTime.plusMinutes(1);

        Map<String, Object> triggerData = new HashMap<>();
        triggerData.put("triggered_time", "now");
        triggerData.put("scheduled_time", "now+1m");

        ExecuteWatchResponse response = watcherClient.prepareExecuteWatch("_id").setTriggerData(triggerData).get();

        assertThat(response, notNullValue());
        assertThat(response.getRecordId(), notNullValue());
        Wid wid = new Wid(response.getRecordId());
        assertThat(wid.watchId(), is("_id"));

        XContentSource record = response.getRecordSource();
        assertValue(record, "watch_id", is("_id"));
        assertValue(record, "trigger_event.type", is("manual"));
        assertValue(record, "trigger_event.triggered_time", is(WatcherDateTimeUtils.formatDate(triggeredTime)));
        assertValue(record, "trigger_event.manual.schedule.scheduled_time", is(WatcherDateTimeUtils.formatDate(scheduledTime)));
        assertValue(record, "state", is("executed"));
        assertValue(record, "input.simple.foo", is("bar"));
        assertValue(record, "condition.always", notNullValue());
        assertValue(record, "result.execution_time", notNullValue());
        assertValue(record, "result.execution_duration", notNullValue());
        assertValue(record, "result.input.type", is("simple"));
        assertValue(record, "result.input.payload.foo", is("bar"));
        assertValue(record, "result.condition.type", is("always"));
        assertValue(record, "result.condition.met", is(true));
        assertValue(record, "result.actions.0.id", is("log"));
        assertValue(record, "result.actions.0.type", is("logging"));
        assertValue(record, "result.actions.0.status", is("success"));
        assertValue(record, "result.actions.0.logging.logged_text", is("_text"));
    }
}

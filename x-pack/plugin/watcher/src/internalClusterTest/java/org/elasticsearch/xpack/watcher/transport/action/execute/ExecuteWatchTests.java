/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transport.action.execute;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ExecuteWatchTests extends AbstractWatcherIntegrationTestCase {

    public void testExecuteAllDefaults() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(
                watchBuilder().trigger(schedule(cron("0/5 * * * * ? 2099")))
                    .input(simpleInput("foo", "bar"))
                    .condition(InternalAlwaysCondition.INSTANCE)
                    .addAction("log", loggingAction("_text"))
            )
            .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        ExecuteWatchResponse response = new ExecuteWatchRequestBuilder(client(), "_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getRecordId(), notNullValue());
        Wid wid = new Wid(response.getRecordId());
        assertThat(wid.watchId(), is("_id"));

        XContentSource record = response.getRecordSource();
        assertValue(record, "watch_id", is("_id"));
        assertValue(record, "trigger_event.type", is("manual"));
        assertValue(record, "trigger_event.triggered_time", notNullValue());
        String triggeredTime = record.getValue("trigger_event.triggered_time");
        assertValue(record, "trigger_event.manual.schedule.scheduled_time", is(triggeredTime));
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
        assertValue(record, "status.actions.log.ack.state", is("ackable"));
    }

    public void testExecuteActionMode() throws Exception {
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(
                watchBuilder().trigger(schedule(interval("1s"))) // run every second so we can ack it
                    .input(simpleInput("foo", "bar"))
                    .defaultThrottlePeriod(TimeValue.timeValueMillis(0))
                    .condition(InternalAlwaysCondition.INSTANCE)
                    .addAction("log", loggingAction("_text"))
            )
            .get();

        assertThat(putWatchResponse.isCreated(), is(true));

        boolean execute = randomBoolean();
        boolean force = randomBoolean();
        ActionExecutionMode mode;
        if (randomBoolean()) {
            mode = ActionExecutionMode.SKIP;
        } else {
            if (execute && force) {
                mode = ActionExecutionMode.FORCE_EXECUTE;
            } else if (execute) {
                mode = ActionExecutionMode.EXECUTE;
            } else if (force) {
                mode = ActionExecutionMode.FORCE_SIMULATE;
            } else {
                mode = ActionExecutionMode.SIMULATE;
            }
        }

        if (mode.force()) {
            // since we're forcing, lets ack the action, such that it'd supposed to be throttled
            // but forcing will ignore the throttling

            // lets wait for the watch to be ackable
            timeWarp().trigger("_id");

            String[] actionIds = randomFrom(new String[] { "_all" }, new String[] { "log" }, new String[] { "foo", "_all" }, null);
            AckWatchRequestBuilder ackWatchRequestBuilder = new AckWatchRequestBuilder(client(), "_id");
            if (actionIds != null) {
                ackWatchRequestBuilder.setActionIds(actionIds);
            }
            AckWatchResponse ackWatchResponse = ackWatchRequestBuilder.get();
            assertThat(ackWatchResponse, notNullValue());
            WatchStatus status = ackWatchResponse.getStatus();
            assertThat(status, notNullValue());
            ActionStatus actionStatus = status.actionStatus("log");
            assertThat(actionStatus, notNullValue());
            assertThat(actionStatus.ackStatus().state(), is(ActionStatus.AckStatus.State.ACKED));
        }

        ExecuteWatchResponse response = new ExecuteWatchRequestBuilder(client(), "_id").setActionMode(
            randomBoolean() ? "log" : "_all",
            mode
        ).get();
        assertThat(response, notNullValue());
        assertThat(response.getRecordId(), notNullValue());
        Wid wid = new Wid(response.getRecordId());
        assertThat(wid.watchId(), is("_id"));

        XContentSource record = response.getRecordSource();
        assertValue(record, "watch_id", is("_id"));
        assertValue(record, "trigger_event.type", is("manual"));
        assertValue(record, "trigger_event.triggered_time", notNullValue());
        String triggeredTime = record.getValue("trigger_event.triggered_time");
        assertValue(record, "trigger_event.manual.schedule.scheduled_time", is(triggeredTime));
        if (mode == ActionExecutionMode.SKIP) {
            assertValue(record, "state", is("throttled"));
        } else {
            assertValue(record, "state", is("executed"));
        }
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
        switch (mode) {
            case SKIP -> { // the action should be manually skipped/throttled
                assertValue(record, "result.actions.0.status", is("throttled"));
                assertValue(record, "result.actions.0.reason", is("manually skipped"));
            }
            default -> {
                if (mode.simulate()) {
                    assertValue(record, "result.actions.0.status", is("simulated"));
                } else {
                    assertValue(record, "result.actions.0.status", is("success"));
                }
                assertValue(record, "result.actions.0.logging.logged_text", is("_text"));
            }
        }
    }
}

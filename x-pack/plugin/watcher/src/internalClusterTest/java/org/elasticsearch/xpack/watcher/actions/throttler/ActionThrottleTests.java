/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.throttler;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.common.http.HttpMethod;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.notification.email.EmailTemplate;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.equalTo;

public class ActionThrottleTests extends AbstractWatcherIntegrationTestCase {

    public void testSingleActionAckThrottle() throws Exception {
        WatchSourceBuilder watchSourceBuilder = watchBuilder().trigger(schedule(interval("60m")));

        AvailableAction availableAction = randomFrom(AvailableAction.values());
        Action.Builder<?> action = availableAction.action();
        watchSourceBuilder.addAction("test_id", action);

        new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(watchSourceBuilder.buildAsBytes(XContentType.JSON), XContentType.JSON)
            .get();
        refresh(Watch.INDEX);

        ExecuteWatchRequestBuilder executeWatchRequestBuilder = new ExecuteWatchRequestBuilder(client()).setId("_id")
            .setRecordExecution(true)
            .setActionMode("test_id", ActionExecutionMode.SIMULATE);

        Map<String, Object> responseMap = executeWatchRequestBuilder.get().getRecordSource().getAsMap();
        String status = ObjectPath.eval("result.actions.0.status", responseMap);
        assertThat(status, equalTo(Action.Result.Status.SIMULATED.toString().toLowerCase(Locale.ROOT)));

        timeWarp().clock().fastForward(TimeValue.timeValueSeconds(15));

        boolean ack = randomBoolean();
        if (ack) {
            new AckWatchRequestBuilder(client(), "_id").setActionIds("test_id").get();
        }

        executeWatchRequestBuilder = new ExecuteWatchRequestBuilder(client()).setId("_id")
            .setRecordExecution(true)
            .setActionMode("test_id", ActionExecutionMode.SIMULATE);
        responseMap = executeWatchRequestBuilder.get().getRecordSource().getAsMap();
        status = ObjectPath.eval("result.actions.0.status", responseMap);
        if (ack) {
            assertThat(status, equalTo(Action.Result.Status.ACKNOWLEDGED.toString().toLowerCase(Locale.ROOT)));
        } else {
            assertThat(status, equalTo(Action.Result.Status.SIMULATED.toString().toLowerCase(Locale.ROOT)));
        }
    }

    public void testRandomMultiActionAckThrottle() throws Exception {
        WatchSourceBuilder watchSourceBuilder = watchBuilder().trigger(schedule(interval("60m")));

        Set<String> ackingActions = new HashSet<>();
        for (int i = 0; i < scaledRandomIntBetween(5, 10); ++i) {
            AvailableAction availableAction = randomFrom(AvailableAction.values());
            Action.Builder<?> action = availableAction.action();
            watchSourceBuilder.addAction("test_id" + i, action);
            if (randomBoolean()) {
                ackingActions.add("test_id" + i);
            }
        }

        new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(watchSourceBuilder.buildAsBytes(XContentType.JSON), XContentType.JSON)
            .get();
        refresh(Watch.INDEX);
        executeWatch("_id");

        for (String actionId : ackingActions) {
            new AckWatchRequestBuilder(client(), "_id").setActionIds(actionId).get();
        }

        timeWarp().clock().fastForwardSeconds(15);

        Map<String, Object> responseMap = executeWatch("_id");
        List<Map<String, String>> actions = ObjectPath.eval("result.actions", responseMap);
        for (Map<String, String> result : actions) {
            if (ackingActions.contains(result.get("id"))) {
                assertThat(result.get("status"), equalTo(Action.Result.Status.ACKNOWLEDGED.toString().toLowerCase(Locale.ROOT)));
            } else {
                assertThat(result.get("status"), equalTo(Action.Result.Status.SIMULATED.toString().toLowerCase(Locale.ROOT)));
            }
        }
    }

    private Map<String, Object> executeWatch(String id) {
        return new ExecuteWatchRequestBuilder(client()).setId(id)
            .setRecordExecution(true)
            .setActionMode("_all", ActionExecutionMode.SIMULATE)
            .get()
            .getRecordSource()
            .getAsMap();
    }

    public void testDifferentThrottlePeriods() throws Exception {
        timeWarp().clock().setTime(ZonedDateTime.now(ZoneOffset.UTC));
        WatchSourceBuilder watchSourceBuilder = watchBuilder().trigger(schedule(interval("60m")));

        watchSourceBuilder.addAction(
            "ten_sec_throttle",
            new TimeValue(10, TimeUnit.SECONDS),
            randomFrom(AvailableAction.values()).action()
        );
        watchSourceBuilder.addAction(
            "fifteen_sec_throttle",
            new TimeValue(15, TimeUnit.SECONDS),
            randomFrom(AvailableAction.values()).action()
        );

        new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(watchSourceBuilder.buildAsBytes(XContentType.JSON), XContentType.JSON)
            .get();
        refresh(Watch.INDEX);

        timeWarp().clock().fastForwardSeconds(1);
        Map<String, Object> responseMap = executeWatch("_id");
        List<Map<String, String>> actions = ObjectPath.eval("result.actions", responseMap);
        for (Map<String, String> result : actions) {
            assertThat(result.get("status"), equalTo(Action.Result.Status.SIMULATED.toString().toLowerCase(Locale.ROOT)));
        }
        timeWarp().clock().fastForwardSeconds(1);

        responseMap = executeWatch("_id");
        actions = ObjectPath.eval("result.actions", responseMap);
        for (Map<String, String> result : actions) {
            assertThat(result.get("status"), equalTo(Action.Result.Status.THROTTLED.toString().toLowerCase(Locale.ROOT)));
        }

        timeWarp().clock().fastForwardSeconds(10);

        responseMap = executeWatch("_id");
        actions = ObjectPath.eval("result.actions", responseMap);
        for (Map<String, String> result : actions) {
            if ("ten_sec_throttle".equals(result.get("id"))) {
                assertThat(result.get("status"), equalTo(Action.Result.Status.SIMULATED.toString().toLowerCase(Locale.ROOT)));
            } else {
                assertThat(result.get("status"), equalTo(Action.Result.Status.THROTTLED.toString().toLowerCase(Locale.ROOT)));
            }
        }
    }

    public void testDefaultThrottlePeriod() throws Exception {
        WatchSourceBuilder watchSourceBuilder = watchBuilder().trigger(schedule(interval("60m")));

        AvailableAction availableAction = randomFrom(AvailableAction.values());
        watchSourceBuilder.addAction("default_global_throttle", availableAction.action());

        new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(watchSourceBuilder.buildAsBytes(XContentType.JSON), XContentType.JSON)
            .get();
        refresh(Watch.INDEX);

        timeWarp().clock().setTime(ZonedDateTime.now(ZoneOffset.UTC));

        ExecuteWatchResponse executeWatchResponse = new ExecuteWatchRequestBuilder(client()).setId("_id")
            .setTriggerEvent(
                new ManualTriggerEvent(
                    "execute_id",
                    new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
                )
            )
            .setActionMode("default_global_throttle", ActionExecutionMode.SIMULATE)
            .setRecordExecution(true)
            .get();

        String status = ObjectPath.eval("result.actions.0.status", executeWatchResponse.getRecordSource().getAsMap());
        assertThat(status, equalTo("simulated"));

        timeWarp().clock().fastForwardSeconds(1);

        executeWatchResponse = new ExecuteWatchRequestBuilder(client()).setId("_id")
            .setTriggerEvent(
                new ManualTriggerEvent(
                    "execute_id",
                    new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
                )
            )
            .setActionMode("default_global_throttle", ActionExecutionMode.SIMULATE)
            .setRecordExecution(true)
            .get();
        status = ObjectPath.eval("result.actions.0.status", executeWatchResponse.getRecordSource().getAsMap());
        assertThat(status, equalTo("throttled"));

        timeWarp().clock().fastForwardSeconds(5);

        assertBusy(() -> {
            try {
                ExecuteWatchResponse executeWatchResponse1 = new ExecuteWatchRequestBuilder(client()).setId("_id")
                    .setTriggerEvent(
                        new ManualTriggerEvent(
                            "execute_id",
                            new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
                        )
                    )
                    .setActionMode("default_global_throttle", ActionExecutionMode.SIMULATE)
                    .setRecordExecution(true)
                    .get();
                String currentStatus = ObjectPath.eval("result.actions.0.status", executeWatchResponse1.getRecordSource().getAsMap());
                assertThat(currentStatus, equalTo("simulated"));
            } catch (IOException ioe) {
                throw new ElasticsearchException("failed to execute", ioe);
            }
        }, 6, TimeUnit.SECONDS);
    }

    public void testWatchThrottlePeriod() throws Exception {
        WatchSourceBuilder watchSourceBuilder = watchBuilder().trigger(schedule(interval("60m")))
            .defaultThrottlePeriod(new TimeValue(20, TimeUnit.SECONDS));

        AvailableAction availableAction = randomFrom(AvailableAction.values());
        watchSourceBuilder.addAction("default_global_throttle", availableAction.action());

        new PutWatchRequestBuilder(client()).setId("_id")
            .setSource(watchSourceBuilder.buildAsBytes(XContentType.JSON), XContentType.JSON)
            .get();
        refresh(Watch.INDEX);

        timeWarp().clock().setTime(ZonedDateTime.now(ZoneOffset.UTC));

        ExecuteWatchResponse executeWatchResponse = new ExecuteWatchRequestBuilder(client()).setId("_id")
            .setTriggerEvent(
                new ManualTriggerEvent(
                    "execute_id",
                    new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
                )
            )
            .setActionMode("default_global_throttle", ActionExecutionMode.SIMULATE)
            .setRecordExecution(true)
            .get();
        String status = ObjectPath.eval("result.actions.0.status", executeWatchResponse.getRecordSource().getAsMap());
        assertThat(status, equalTo("simulated"));

        timeWarp().clock().fastForwardSeconds(1);

        executeWatchResponse = new ExecuteWatchRequestBuilder(client()).setId("_id")
            .setTriggerEvent(
                new ManualTriggerEvent(
                    "execute_id",
                    new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
                )
            )
            .setActionMode("default_global_throttle", ActionExecutionMode.SIMULATE)
            .setRecordExecution(true)
            .get();
        status = ObjectPath.eval("result.actions.0.status", executeWatchResponse.getRecordSource().getAsMap());
        assertThat(status, equalTo("throttled"));

        timeWarp().clock().fastForwardSeconds(20);

        assertBusy(() -> {
            try {
                // Since the default throttle period is 5 seconds but we have overridden the period in the watch this should trigger
                ExecuteWatchResponse executeWatchResponse1 = new ExecuteWatchRequestBuilder(client()).setId("_id")
                    .setTriggerEvent(
                        new ManualTriggerEvent(
                            "execute_id",
                            new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
                        )
                    )
                    .setActionMode("default_global_throttle", ActionExecutionMode.SIMULATE)
                    .setRecordExecution(true)
                    .get();
                String status1 = ObjectPath.eval("result.actions.0.status", executeWatchResponse1.getRecordSource().getAsMap());
                assertThat(status1, equalTo("simulated"));
            } catch (IOException ioe) {
                throw new ElasticsearchException("failed to execute", ioe);
            }
        }, 20, TimeUnit.SECONDS);
    }

    public void testFailingActionDoesGetThrottled() throws Exception {
        // create a mapping with a wrong @timestamp field, so that the index action of the watch below will fail
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("@timestamp")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
        );

        indicesAdmin().prepareCreate("foo").setMapping(mapping).get();

        TimeValue throttlePeriod = new TimeValue(60, TimeUnit.MINUTES);

        new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(
                new ScheduleTrigger(new IntervalSchedule(new IntervalSchedule.Interval(60, IntervalSchedule.Interval.Unit.MINUTES)))
            )
                .defaultThrottlePeriod(throttlePeriod)
                .addAction("logging", loggingAction("test out"))
                .addAction("failing_hook", indexAction("foo").setExecutionTimeField("@timestamp"))
        ).get();
        refresh(Watch.INDEX);

        {
            Map<String, Object> responseMap = new ExecuteWatchRequestBuilder(client()).setId("_id")
                .setRecordExecution(true)
                .get()
                .getRecordSource()
                .getAsMap();

            String state = ObjectPath.eval("state", responseMap);

            String firstId = ObjectPath.eval("result.actions.0.id", responseMap);
            String statusLogging, statusFailingHook;
            if ("logging".equals(firstId)) {
                statusLogging = ObjectPath.eval("result.actions.0.status", responseMap);
                statusFailingHook = ObjectPath.eval("result.actions.1.status", responseMap);
            } else {
                statusFailingHook = ObjectPath.eval("result.actions.0.status", responseMap);
                statusLogging = ObjectPath.eval("result.actions.1.status", responseMap);
            }

            assertThat(state, equalTo(ExecutionState.EXECUTED.toString().toLowerCase(Locale.ROOT)));
            assertThat(statusLogging, equalTo(Action.Result.Status.SUCCESS.toString().toLowerCase(Locale.ROOT)));
            assertThat(statusFailingHook, equalTo(Action.Result.Status.FAILURE.toString().toLowerCase(Locale.ROOT)));
        }

        {
            Map<String, Object> responseMap = new ExecuteWatchRequestBuilder(client()).setId("_id")
                .setRecordExecution(true)
                .get()
                .getRecordSource()
                .getAsMap();
            String state = ObjectPath.eval("state", responseMap);

            String firstId = ObjectPath.eval("result.actions.0.id", responseMap);
            String statusLogging, statusFailingHook;
            if ("logging".equals(firstId)) {
                statusLogging = ObjectPath.eval("result.actions.0.status", responseMap);
                statusFailingHook = ObjectPath.eval("result.actions.1.status", responseMap);
            } else {
                statusFailingHook = ObjectPath.eval("result.actions.0.status", responseMap);
                statusLogging = ObjectPath.eval("result.actions.1.status", responseMap);
            }

            assertThat(state, equalTo(ExecutionState.THROTTLED.toString().toLowerCase(Locale.ROOT)));
            assertThat(statusLogging, equalTo(Action.Result.Status.THROTTLED.toString().toLowerCase(Locale.ROOT)));
            assertThat(statusFailingHook, equalTo(Action.Result.Status.FAILURE.toString().toLowerCase(Locale.ROOT)));
        }
    }

    enum AvailableAction {
        EMAIL {
            @Override
            public Action.Builder<EmailAction> action() throws Exception {
                EmailTemplate.Builder emailBuilder = EmailTemplate.builder();
                emailBuilder.from("test@test.com");
                emailBuilder.to("test@test.com");
                emailBuilder.subject("test subject");
                return EmailAction.builder(emailBuilder.build());
            }

            @Override
            public String type() {
                return EmailAction.TYPE;
            }
        },
        WEBHOOK {
            @Override
            public Action.Builder<WebhookAction> action() throws Exception {
                HttpRequestTemplate.Builder requestBuilder = HttpRequestTemplate.builder("localhost", 1234)
                    .path("/")
                    .method(HttpMethod.GET);
                return WebhookAction.builder(requestBuilder.build());
            }

            @Override
            public String type() {
                return WebhookAction.TYPE;
            }
        },
        LOGGING {
            @Override
            public Action.Builder<LoggingAction> action() throws Exception {
                return LoggingAction.builder(new TextTemplate("_logging"));
            }

            @Override
            public String type() {
                return LoggingAction.TYPE;
            }
        },
        INDEX {
            @Override
            public Action.Builder<IndexAction> action() throws Exception {
                return IndexAction.builder("test_index");
            }

            @Override
            public String type() {
                return IndexAction.TYPE;
            }
        };

        public abstract Action.Builder<? extends Action> action() throws Exception;

        public abstract String type();
    }
}

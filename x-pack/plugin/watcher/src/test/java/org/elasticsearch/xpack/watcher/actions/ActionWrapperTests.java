/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus.AckStatus.State;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.logging.ExecutableLoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionWrapperTests extends ESTestCase {

    private ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    private Watch watch = mock(Watch.class);
    @SuppressWarnings("unchecked")
    private ExecutableAction<Action> executableAction = mock(ExecutableAction.class);
    private ActionWrapper actionWrapper = new ActionWrapper("_action", null, NeverCondition.INSTANCE, null, executableAction, null, null);

    public void testThatUnmetActionConditionResetsAckStatus() throws Exception {
        WatchStatus watchStatus = new WatchStatus(now, Collections.singletonMap("_action", createActionStatus(State.ACKED)));
        when(watch.status()).thenReturn(watchStatus);

        ActionWrapperResult result = actionWrapper.execute(mockExecutionContent(watch));
        assertThat(result.condition().met(), is(false));
        assertThat(result.action().status(), is(Action.Result.Status.CONDITION_FAILED));
        assertThat(watch.status().actionStatus("_action").ackStatus().state(), is(State.AWAITS_SUCCESSFUL_EXECUTION));
    }

    public void testOtherActionsAreNotAffectedOnActionConditionReset() throws Exception {
        Map<String, ActionStatus> statusMap = new HashMap<>();
        statusMap.put("_action", createActionStatus(State.ACKED));
        State otherState = randomFrom(State.ACKABLE, State.AWAITS_SUCCESSFUL_EXECUTION);
        statusMap.put("other", createActionStatus(otherState));

        WatchStatus watchStatus = new WatchStatus(now, statusMap);
        when(watch.status()).thenReturn(watchStatus);

        actionWrapper.execute(mockExecutionContent(watch));
        assertThat(watch.status().actionStatus("other").ackStatus().state(), is(otherState));
    }

    public void testThatMultipleResultsCanBeReturned() throws Exception {
        final LoggingAction loggingAction = new LoggingAction(new TextTemplate("{{key}}"), null, null);
        final ExecutableAction<LoggingAction> executableAction =
            new ExecutableLoggingAction(loggingAction, logger, new MockTextTemplateEngine());
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction,
            "ctx.payload.my_path", null);

        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path",
            List.of(
                Map.of("key", "first"),
                Map.of("key", "second"),
                Map.of("key", "third")
            )));
        when(ctx.payload()).thenReturn(payload);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.SUCCESS));
        // check that action toXContent contains all the results
        try (XContentBuilder builder = jsonBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            final String json = Strings.toString(builder);
            final Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true);
            assertThat(map, hasKey("foreach"));
            assertThat(map.get("foreach"), instanceOf(List.class));
            List<Map<String, Object>> actions = (List) map.get("foreach");
            assertThat(actions, hasSize(3));
        }
    }

    public void testThatSpecifiedPathIsNotCollection() {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction,
            "ctx.payload.my_path", null);
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path", "not a map"));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.FAILURE));
        assertThat(result.action(), instanceOf(Action.Result.FailureWithException.class));
        Action.Result.FailureWithException failureWithException = (Action.Result.FailureWithException) result.action();
        assertThat(failureWithException.getException().getMessage(),
            is("specified foreach object was not a an array/collection: [ctx.payload.my_path]"));
    }

    public void testEmptyCollection() {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction,
            "ctx.payload.my_path", null);
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path", Collections.emptyList()));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.FAILURE));
        assertThat(result.action(), instanceOf(Action.Result.FailureWithException.class));
        Action.Result.FailureWithException failureWithException = (Action.Result.FailureWithException) result.action();
        assertThat(failureWithException.getException().getMessage(),
            is("foreach object [ctx.payload.my_path] was an empty list, could not run any action"));
    }

    public void testPartialFailure() throws Exception {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction,
            "ctx.payload.my_path", null);
        WatchExecutionContext ctx = mockExecutionContent(watch);
        Payload.Simple payload = new Payload.Simple(Map.of("my_path",
            List.of(
                Map.of("key", "first"),
                Map.of("key", "second")
            )));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        final Action.Result firstResult = new LoggingAction.Result.Success("log_message");;
        final Payload firstPayload = new Payload.Simple(Map.of("key", "first"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(firstPayload))).thenReturn(firstResult);

        final Action.Result secondResult = new Action.Result.Failure("MY_TYPE", "second reason");
        final Payload secondPayload = new Payload.Simple(Map.of("key", "second"));
        when(executableAction.execute(eq("_action"), eq(ctx), eq(secondPayload))).thenReturn(secondResult);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.PARTIAL_FAILURE));
    }

    public void testDefaultLimitOfNumberOfActionsExecuted() throws Exception {
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction,
            "ctx.payload.my_path", null);
        WatchExecutionContext ctx = mockExecutionContent(watch);
        List<Map<String, String>> itemsPayload = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            final Action.Result actionResult = new LoggingAction.Result.Success("log_message " + i);;
            final Payload singleItemPayload = new Payload.Simple(Map.of("key", String.valueOf(i)));
            itemsPayload.add(Map.of("key", String.valueOf(i)));
            when(executableAction.execute(eq("_action"), eq(ctx), eq(singleItemPayload))).thenReturn(actionResult);
        }

        Payload.Simple payload = new Payload.Simple(Map.of("my_path", itemsPayload));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.SUCCESS));

        // check that action toXContent contains all the results
        try (XContentBuilder builder = jsonBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            final String json = Strings.toString(builder);
            final Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true);
            assertThat(map, hasKey("foreach"));
            assertThat(map.get("foreach"), instanceOf(List.class));
            List<Map<String, Object>> actions = (List) map.get("foreach");
            assertThat(actions, hasSize(100));
            assertThat(map, hasKey("max_iterations"));
            assertThat(map.get("max_iterations"), is(100));
            assertThat(map, hasKey("number_of_actions_executed"));
            assertThat(map.get("number_of_actions_executed"), is(100));
        }
    }

    public void testConfiguredLimitOfNumberOfActionsExecuted() throws Exception {
        int randomMaxIterations = randomIntBetween(1, 1000);
        ActionWrapper wrapper = new ActionWrapper("_action", null, InternalAlwaysCondition.INSTANCE, null, executableAction,
            "ctx.payload.my_path", randomMaxIterations);
        WatchExecutionContext ctx = mockExecutionContent(watch);
        List<Map<String, String>> itemsPayload = new ArrayList<>();
        for (int i = 0; i < randomMaxIterations + 1; i++) {
            final Action.Result actionResult = new LoggingAction.Result.Success("log_message " + i);;
            final Payload singleItemPayload = new Payload.Simple(Map.of("key", String.valueOf(i)));
            itemsPayload.add(Map.of("key", String.valueOf(i)));
            when(executableAction.execute(eq("_action"), eq(ctx), eq(singleItemPayload))).thenReturn(actionResult);
        }

        Payload.Simple payload = new Payload.Simple(Map.of("my_path", itemsPayload));
        when(ctx.payload()).thenReturn(payload);
        when(executableAction.logger()).thenReturn(logger);

        ActionWrapperResult result = wrapper.execute(ctx);
        assertThat(result.action().status(), is(Action.Result.Status.SUCCESS));

        // check that action toXContent contains all the results
        try (XContentBuilder builder = jsonBuilder()) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            final String json = Strings.toString(builder);
            final Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, true);
            assertThat(map, hasKey("foreach"));
            assertThat(map.get("foreach"), instanceOf(List.class));
            List<Map<String, Object>> actions = (List) map.get("foreach");
            assertThat(actions, hasSize(randomMaxIterations));
            assertThat(map, hasKey("max_iterations"));
            assertThat(map.get("max_iterations"), is(randomMaxIterations));
            assertThat(map, hasKey("number_of_actions_executed"));
            assertThat(map.get("number_of_actions_executed"), is(randomMaxIterations)); 
        }
    }

    private WatchExecutionContext mockExecutionContent(Watch watch) {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(watch.id()).thenReturn("watchId");
        when(ctx.watch()).thenReturn(watch);
        final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        final Wid wid = new Wid("watchId", now);
        when(ctx.id()).thenReturn(wid);
        when(ctx.executionTime()).thenReturn(now);
        final TriggerEvent triggerEvent = new ScheduleTriggerEvent("watchId", now, now);
        when(ctx.triggerEvent()).thenReturn(triggerEvent);
        when(ctx.skipThrottling(eq("_action"))).thenReturn(true);
        return ctx;
    }

    private ActionStatus createActionStatus(State state) {
        ActionStatus.AckStatus ackStatus = new ActionStatus.AckStatus(now, state);
        ActionStatus.Execution execution = ActionStatus.Execution.successful(now);
        return new ActionStatus(ackStatus, execution, execution, null);
    }
}

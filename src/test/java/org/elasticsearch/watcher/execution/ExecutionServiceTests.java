/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.*;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.never.NeverCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.watcher.actions.throttler.Throttler;
import org.elasticsearch.watcher.support.validation.WatcherSettingsValidation;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 */
public class ExecutionServiceTests extends ElasticsearchTestCase {

    private Payload payload;
    private ExecutableInput input;
    private Input.Result inputResult;

    private ExecutionService executionService;

    @Before
    public void init() throws Exception {
        payload = mock(Payload.class);
        input = mock(ExecutableInput.class);
        inputResult = mock(Input.Result.class);
        when(inputResult.payload()).thenReturn(payload);
        when(input.execute(any(WatchExecutionContext.class))).thenReturn(inputResult);

        HistoryStore historyStore = mock(HistoryStore.class);
        TriggeredWatchStore triggeredWatchStore = mock(TriggeredWatchStore.class);
        WatchExecutor executor = mock(WatchExecutor.class);
        WatchStore watchStore = mock(WatchStore.class);
        WatchLockService watchLockService = mock(WatchLockService.class);
        WatcherSettingsValidation settingsValidator = mock(WatcherSettingsValidation.class);
        Clock clock = new ClockMock();
        executionService = new ExecutionService(ImmutableSettings.EMPTY, historyStore, triggeredWatchStore, executor, watchStore, watchLockService, clock, settingsValidator);
    }

    @Test
    public void testExecute() throws Exception {
        DateTime now = DateTime.now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.Result.INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        Transform.Result watchTransformResult = mock(Transform.Result.class);
        when(watchTransformResult.payload()).thenReturn(payload);
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);
        when(watchTransform.execute(context, payload)).thenReturn(watchTransformResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action level transform
        Transform.Result actionTransformResult = mock(Transform.Result.class);
        when(actionTransformResult.payload()).thenReturn(payload);
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);
        when(actionTransform.execute(context, payload)).thenReturn(actionTransformResult);

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);


        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionTransform, action);
        ExecutableActions actions = new ExecutableActions(Arrays.asList(actionWrapper));

        WatchStatus watchStatus = new WatchStatus(ImmutableMap.of("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.execution().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.execution().transformResult(), sameInstance(watchTransformResult));
        ActionWrapper.Result result = watchRecord.execution().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.transform(), sameInstance(actionTransformResult));
        assertThat(result.action(), sameInstance(actionResult));

        verify(condition, times(1)).execute(context);
        verify(watchTransform, times(1)).execute(context, payload);
        verify(action, times(1)).execute("_action", context, payload);
    }

    @Test
    public void testExecute_throttled() throws Exception {
        DateTime now = DateTime.now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.Result.INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);
        when(throttleResult.reason()).thenReturn("_throttle_reason");
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        ExecutableTransform transform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, transform, action);
        ExecutableActions actions = new ExecutableActions(Arrays.asList(actionWrapper));

        WatchStatus watchStatus = new WatchStatus(ImmutableMap.of("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord executionResult = executionService.executeInner(context);
        assertThat(executionResult.execution().inputResult(), sameInstance(inputResult));
        assertThat(executionResult.execution().conditionResult(), sameInstance(conditionResult));
        assertThat(executionResult.execution().transformResult(), nullValue());
        assertThat(executionResult.execution().actionsResults().count(), is(1));
        ActionWrapper.Result result = executionResult.execution().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.transform(), nullValue());
        assertThat(result.action(), instanceOf(Action.Result.Throttled.class));
        Action.Result.Throttled throttled = (Action.Result.Throttled) result.action();
        assertThat(throttled.reason(), is("_throttle_reason"));

        verify(condition, times(1)).execute(context);
        verify(throttler, times(1)).throttle("_action", context);
        verify(transform, never()).execute(context, payload);
    }

    @Test
    public void testExecute_conditionNotMet() throws Exception {
        DateTime now = DateTime.now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = NeverCondition.Result.INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);

        // action throttler
        ActionThrottler throttler = mock(ActionThrottler.class);
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);
        ExecutableAction action = mock(ExecutableAction.class);
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionTransform, action);
        ExecutableActions actions = new ExecutableActions(Arrays.asList(actionWrapper));

        WatchStatus watchStatus = new WatchStatus(ImmutableMap.of("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord executionResult = executionService.executeInner(context);
        assertThat(executionResult.execution().inputResult(), sameInstance(inputResult));
        assertThat(executionResult.execution().conditionResult(), sameInstance(conditionResult));
        assertThat(executionResult.execution().transformResult(), nullValue());
        assertThat(executionResult.execution().actionsResults().count(), is(0));

        verify(condition, times(1)).execute(context);
        verify(watchTransform, never()).execute(context, payload);
        verify(throttler, never()).throttle("_action", context);
        verify(actionTransform, never()).execute(context, payload);
        verify(action, never()).execute("_action", context, payload);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.never.NeverCondition;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transform.ExecutableTransform;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.watcher.watch.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
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
        WatchExecutor executor = mock(WatchExecutor.class);
        WatchStore watchStore = mock(WatchStore.class);
        WatchLockService watchLockService = mock(WatchLockService.class);
        Clock clock = new ClockMock();
        executionService = new ExecutionService(ImmutableSettings.EMPTY, historyStore, executor, watchStore, watchLockService, clock);
    }

    @Test
    public void testExecute() throws Exception {
        Condition.Result conditionResult = AlwaysCondition.Result.INSTANCE;
        Throttler.Result throttleResult = Throttler.Result.NO;
        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        ActionWrapper.Result watchActionResult = new ActionWrapper.Result("_id", null, actionResult);

        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(WatchExecutionContext.class))).thenReturn(throttleResult);
        ExecutableTransform transform = mock(ExecutableTransform.class);
        when(transform.execute(any(WatchExecutionContext.class), same(payload))).thenReturn(transformResult);
        ActionWrapper action = mock(ActionWrapper.class);
        when(action.execute(any(WatchExecutionContext.class))).thenReturn(watchActionResult);
        ExecutableActions actions = new ExecutableActions(Arrays.asList(action));

        Watch.Status watchStatus = new Watch.Status();
        Watch watch = mock(Watch.class);
        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.throttler()).thenReturn(throttler);
        when(watch.transform()).thenReturn(transform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        DateTime now = DateTime.now(UTC);

        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event);
        WatchExecution watchExecution = executionService.executeInner(context);
        assertThat(watchExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(watchExecution.transformResult(), sameInstance(transformResult));
        assertThat(watchExecution.throttleResult(), sameInstance(throttleResult));
        assertThat(watchExecution.actionsResults().get("_id"), sameInstance(watchActionResult));

        verify(condition, times(1)).execute(any(WatchExecutionContext.class));
        verify(throttler, times(1)).throttle(any(WatchExecutionContext.class));
        verify(transform, times(1)).execute(any(WatchExecutionContext.class), same(payload));
        verify(action, times(1)).execute(any(WatchExecutionContext.class));
    }

    @Test
    public void testExecute_throttled() throws Exception {
        Condition.Result conditionResult = AlwaysCondition.Result.INSTANCE;
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);

        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        ActionWrapper.Result actionResult = mock(ActionWrapper.Result.class);
        when(actionResult.id()).thenReturn("_id");

        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(WatchExecutionContext.class))).thenReturn(throttleResult);
        ExecutableTransform transform = mock(ExecutableTransform.class);
        when(transform.execute(any(WatchExecutionContext.class), same(payload))).thenReturn(transformResult);
        ActionWrapper action = mock(ActionWrapper.class);
        when(action.execute(any(WatchExecutionContext.class))).thenReturn(actionResult);
        ExecutableActions actions = new ExecutableActions(Arrays.asList(action));

        Watch.Status watchStatus = new Watch.Status();
        Watch watch = mock(Watch.class);
        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.throttler()).thenReturn(throttler);
        when(watch.transform()).thenReturn(transform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        DateTime now = DateTime.now(UTC);

        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event);
        WatchExecution watchExecution = executionService.executeInner(context);
        assertThat(watchExecution.inputResult(), sameInstance(inputResult));
        assertThat(watchExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(watchExecution.throttleResult(), sameInstance(throttleResult));
        assertThat(watchExecution.actionsResults().count(), is(0));
        assertThat(watchExecution.transformResult(), nullValue());

        verify(condition, times(1)).execute(any(WatchExecutionContext.class));
        verify(throttler, times(1)).throttle(any(WatchExecutionContext.class));
        verify(transform, never()).execute(any(WatchExecutionContext.class), same(payload));
        verify(action, never()).execute(any(WatchExecutionContext.class));
    }

    @Test
    public void testExecute_conditionNotMet() throws Exception {
        Condition.Result conditionResult = NeverCondition.Result.INSTANCE;
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);

        Transform.Result transformResult = mock(Transform.Result.class);
        ActionWrapper.Result actionResult = mock(ActionWrapper.Result.class);
        when(actionResult.id()).thenReturn("_id");

        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(WatchExecutionContext.class))).thenReturn(throttleResult);
        ExecutableTransform transform = mock(ExecutableTransform.class);
        when(transform.execute(any(WatchExecutionContext.class), same(payload))).thenReturn(transformResult);
        ActionWrapper action = mock(ActionWrapper.class);
        when(action.execute(any(WatchExecutionContext.class))).thenReturn(actionResult);
        ExecutableActions actions = new ExecutableActions(Arrays.asList(action));

        Watch.Status watchStatus = new Watch.Status();
        Watch watch = mock(Watch.class);
        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.throttler()).thenReturn(throttler);
        when(watch.transform()).thenReturn(transform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        DateTime now = DateTime.now(UTC);

        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event);
        WatchExecution watchExecution = executionService.executeInner(context);
        assertThat(watchExecution.inputResult(), sameInstance(inputResult));
        assertThat(watchExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(watchExecution.throttleResult(), nullValue());
        assertThat(watchExecution.transformResult(), nullValue());
        assertThat(watchExecution.actionsResults().count(), is(0));

        verify(condition, times(1)).execute(any(WatchExecutionContext.class));
        verify(throttler, never()).throttle(any(WatchExecutionContext.class));
        verify(transform, never()).execute(any(WatchExecutionContext.class), same(payload));
        verify(action, never()).execute(any(WatchExecutionContext.class));
    }

}

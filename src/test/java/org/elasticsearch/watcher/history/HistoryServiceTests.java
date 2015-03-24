/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.Actions;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.simple.AlwaysFalseCondition;
import org.elasticsearch.watcher.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.scheduler.Scheduler;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.watch.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.*;

/**
 */
public class HistoryServiceTests extends ElasticsearchTestCase {

    private Payload payload;
    private Input input;
    private Input.Result inputResult;

    private HistoryService historyService;

    @Before
    public void init() throws Exception {
        payload = mock(Payload.class);
        input = mock(Input.class);
        inputResult = mock(Input.Result.class);
        when(inputResult.payload()).thenReturn(payload);
        when(input.execute(any(WatchExecutionContext.class))).thenReturn(inputResult);

        HistoryStore historyStore = mock(HistoryStore.class);
        WatchExecutor executor = mock(WatchExecutor.class);
        WatchStore watchStore = mock(WatchStore.class);
        WatchLockService watchLockService = mock(WatchLockService.class);
        Scheduler scheduler = mock(Scheduler.class);
        ClusterService clusterService = mock(ClusterService.class);
        historyService = new HistoryService(ImmutableSettings.EMPTY, historyStore, executor, watchStore, watchLockService, scheduler, clusterService, SystemClock.INSTANCE);
    }

    @Test
    public void testExecute() throws Exception {
        Condition.Result conditionResult = AlwaysTrueCondition.RESULT;
        Throttler.Result throttleResult = Throttler.Result.NO;
        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");

        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(WatchExecutionContext.class))).thenReturn(throttleResult);
        Transform transform = mock(Transform.class);
        when(transform.apply(any(WatchExecutionContext.class), same(payload))).thenReturn(transformResult);
        Action action = mock(Action.class);
        when(action.execute(any(WatchExecutionContext.class))).thenReturn(actionResult);
        Actions actions = new Actions(Arrays.asList(action));

        Watch.Status watchStatus = new Watch.Status();
        Watch watch = mock(Watch.class);
        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.throttler()).thenReturn(throttler);
        when(watch.transform()).thenReturn(transform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        WatchExecutionContext context = new WatchExecutionContext("1", watch, DateTime.now(), DateTime.now(), DateTime.now());
        WatchExecution watchExecution = historyService.execute(context);
        assertThat(watchExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(watchExecution.transformResult(), sameInstance(transformResult));
        assertThat(watchExecution.throttleResult(), sameInstance(throttleResult));
        assertThat(watchExecution.actionsResults().get("_action_type"), sameInstance(actionResult));

        verify(condition, times(1)).execute(any(WatchExecutionContext.class));
        verify(throttler, times(1)).throttle(any(WatchExecutionContext.class));
        verify(transform, times(1)).apply(any(WatchExecutionContext.class), same(payload));
        verify(action, times(1)).execute(any(WatchExecutionContext.class));
    }

    @Test
    public void testExecute_throttled() throws Exception {
        Condition.Result conditionResult = AlwaysTrueCondition.RESULT;
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);

        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");

        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(WatchExecutionContext.class))).thenReturn(throttleResult);
        Transform transform = mock(Transform.class);
        when(transform.apply(any(WatchExecutionContext.class), same(payload))).thenReturn(transformResult);
        Action action = mock(Action.class);
        when(action.execute(any(WatchExecutionContext.class))).thenReturn(actionResult);
        Actions actions = new Actions(Arrays.asList(action));

        Watch.Status watchStatus = new Watch.Status();
        Watch watch = mock(Watch.class);
        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.throttler()).thenReturn(throttler);
        when(watch.transform()).thenReturn(transform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        WatchExecutionContext context = new WatchExecutionContext("1", watch, DateTime.now(), DateTime.now(), DateTime.now());
        WatchExecution watchExecution = historyService.execute(context);
        assertThat(watchExecution.inputResult(), sameInstance(inputResult));
        assertThat(watchExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(watchExecution.throttleResult(), sameInstance(throttleResult));
        assertThat(watchExecution.actionsResults().isEmpty(), is(true));
        assertThat(watchExecution.transformResult(), nullValue());

        verify(condition, times(1)).execute(any(WatchExecutionContext.class));
        verify(throttler, times(1)).throttle(any(WatchExecutionContext.class));
        verify(transform, never()).apply(any(WatchExecutionContext.class), same(payload));
        verify(action, never()).execute(any(WatchExecutionContext.class));
    }

    @Test
    public void testExecute_conditionNotMet() throws Exception {
        Condition.Result conditionResult = AlwaysFalseCondition.RESULT;
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);

        Transform.Result transformResult = mock(Transform.Result.class);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");

        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(WatchExecutionContext.class))).thenReturn(throttleResult);
        Transform transform = mock(Transform.class);
        when(transform.apply(any(WatchExecutionContext.class), same(payload))).thenReturn(transformResult);
        Action action = mock(Action.class);
        when(action.execute(any(WatchExecutionContext.class))).thenReturn(actionResult);
        Actions actions = new Actions(Arrays.asList(action));

        Watch.Status watchStatus = new Watch.Status();
        Watch watch = mock(Watch.class);
        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.throttler()).thenReturn(throttler);
        when(watch.transform()).thenReturn(transform);
        when(watch.actions()).thenReturn(actions);
        when(watch.status()).thenReturn(watchStatus);

        WatchExecutionContext context = new WatchExecutionContext("1", watch, DateTime.now(), DateTime.now(), DateTime.now());
        WatchExecution watchExecution = historyService.execute(context);
        assertThat(watchExecution.inputResult(), sameInstance(inputResult));
        assertThat(watchExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(watchExecution.throttleResult(), nullValue());
        assertThat(watchExecution.transformResult(), nullValue());
        assertThat(watchExecution.actionsResults().isEmpty(), is(true));

        verify(condition, times(1)).execute(any(WatchExecutionContext.class));
        verify(throttler, never()).throttle(any(WatchExecutionContext.class));
        verify(transform, never()).apply(any(WatchExecutionContext.class), same(payload));
        verify(action, never()).execute(any(WatchExecutionContext.class));
    }

}

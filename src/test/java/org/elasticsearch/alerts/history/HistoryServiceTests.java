/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.alerts.*;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.simple.AlwaysFalseCondition;
import org.elasticsearch.alerts.condition.simple.AlwaysTrueCondition;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.scheduler.Scheduler;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.*;

/**
 */
public class HistoryServiceTests extends ElasticsearchTestCase {

    private Payload payload;
    private Input input;

    private HistoryService historyService;

    @Before
    public void init() throws Exception {
        payload = mock(Payload.class);
        input = mock(Input.class);
        Input.Result inputResult = mock(Input.Result.class);
        when(inputResult.payload()).thenReturn(payload);
        when(input.execute(any(ExecutionContext.class))).thenReturn(inputResult);

        HistoryStore historyStore = mock(HistoryStore.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        AlertsStore alertsStore = mock(AlertsStore.class);
        AlertLockService alertLockService = mock(AlertLockService.class);
        Scheduler scheduler = mock(Scheduler.class);
        ClusterService clusterService = mock(ClusterService.class);
        historyService = new HistoryService(ImmutableSettings.EMPTY, historyStore, threadPool, alertsStore, alertLockService, scheduler, clusterService);
    }

    @Test
    public void testExecute() throws Exception {
        Condition.Result conditionResult = AlwaysTrueCondition.RESULT;
        Throttler.Result throttleResult = Throttler.Result.NO;
        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("actionResult");

        Condition condition = mock(Condition.class);
        when(condition.execute(any(ExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(ExecutionContext.class))).thenReturn(throttleResult);
        Transform transform = mock(Transform.class);
        when(transform.apply(any(ExecutionContext.class), same(payload))).thenReturn(transformResult);
        Action action = mock(Action.class);
        when(action.execute(any(ExecutionContext.class), same(payload))).thenReturn(actionResult);
        Actions actions = new Actions(Arrays.asList(action));

        Alert.Status alertStatus = new Alert.Status();
        Alert alert = mock(Alert.class);
        when(alert.input()).thenReturn(input);
        when(alert.condition()).thenReturn(condition);
        when(alert.throttler()).thenReturn(throttler);
        when(alert.transform()).thenReturn(transform);
        when(alert.actions()).thenReturn(actions);
        when(alert.status()).thenReturn(alertStatus);

        ExecutionContext context = new ExecutionContext("1", alert, DateTime.now(), DateTime.now());
        AlertExecution alertExecution = historyService.execute(context);
        assertThat(alertExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(alertExecution.payload(), sameInstance(payload));
        assertThat(alertExecution.throttleResult(), sameInstance(throttleResult));
        assertThat(alertExecution.actionsResults().get("actionResult"), sameInstance(actionResult));

        verify(condition, times(1)).execute(any(ExecutionContext.class));
        verify(throttler, times(1)).throttle(any(ExecutionContext.class));
        verify(transform, times(1)).apply(any(ExecutionContext.class), same(payload));
        verify(action, times(1)).execute(any(ExecutionContext.class), same(payload));
    }

    @Test
    public void testExecute_throttled() throws Exception {
        Condition.Result conditionResult = AlwaysTrueCondition.RESULT;
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);

        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("actionResult");

        Condition condition = mock(Condition.class);
        when(condition.execute(any(ExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(ExecutionContext.class))).thenReturn(throttleResult);
        Transform transform = mock(Transform.class);
        when(transform.apply(any(ExecutionContext.class), same(payload))).thenReturn(transformResult);
        Action action = mock(Action.class);
        when(action.execute(any(ExecutionContext.class), same(payload))).thenReturn(actionResult);
        Actions actions = new Actions(Arrays.asList(action));

        Alert.Status alertStatus = new Alert.Status();
        Alert alert = mock(Alert.class);
        when(alert.input()).thenReturn(input);
        when(alert.condition()).thenReturn(condition);
        when(alert.throttler()).thenReturn(throttler);
        when(alert.transform()).thenReturn(transform);
        when(alert.actions()).thenReturn(actions);
        when(alert.status()).thenReturn(alertStatus);

        ExecutionContext context = new ExecutionContext("1", alert, DateTime.now(), DateTime.now());
        AlertExecution alertExecution = historyService.execute(context);
        assertThat(alertExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(alertExecution.payload(), sameInstance(payload));
        assertThat(alertExecution.throttleResult(), sameInstance(throttleResult));
        assertThat(alertExecution.actionsResults().isEmpty(), is(true));

        verify(condition, times(1)).execute(any(ExecutionContext.class));
        verify(throttler, times(1)).throttle(any(ExecutionContext.class));
        verify(transform, never()).apply(any(ExecutionContext.class), same(payload));
        verify(action, never()).execute(any(ExecutionContext.class), same(payload));
    }

    @Test
    public void testExecute_conditionNotMet() throws Exception {
        Condition.Result conditionResult = AlwaysFalseCondition.RESULT;
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);

        Transform.Result transformResult = mock(Transform.Result.class);
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("actionResult");

        Condition condition = mock(Condition.class);
        when(condition.execute(any(ExecutionContext.class))).thenReturn(conditionResult);
        Throttler throttler = mock(Throttler.class);
        when(throttler.throttle(any(ExecutionContext.class))).thenReturn(throttleResult);
        Transform transform = mock(Transform.class);
        when(transform.apply(any(ExecutionContext.class), same(payload))).thenReturn(transformResult);
        Action action = mock(Action.class);
        when(action.execute(any(ExecutionContext.class), same(payload))).thenReturn(actionResult);
        Actions actions = new Actions(Arrays.asList(action));

        Alert.Status alertStatus = new Alert.Status();
        Alert alert = mock(Alert.class);
        when(alert.input()).thenReturn(input);
        when(alert.condition()).thenReturn(condition);
        when(alert.throttler()).thenReturn(throttler);
        when(alert.transform()).thenReturn(transform);
        when(alert.actions()).thenReturn(actions);
        when(alert.status()).thenReturn(alertStatus);

        ExecutionContext context = new ExecutionContext("1", alert, DateTime.now(), DateTime.now());
        AlertExecution alertExecution = historyService.execute(context);
        assertThat(alertExecution.conditionResult(), sameInstance(conditionResult));
        assertThat(alertExecution.payload(), sameInstance(payload));
        assertThat(alertExecution.throttleResult(), nullValue());
        assertThat(alertExecution.actionsResults().isEmpty(), is(true));

        verify(condition, times(1)).execute(any(ExecutionContext.class));
        verify(throttler, never()).throttle(any(ExecutionContext.class));
        verify(transform, never()).apply(any(ExecutionContext.class), same(payload));
        verify(action, never()).execute(any(ExecutionContext.class), same(payload));
    }

}

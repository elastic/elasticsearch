/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.support.clock.ClockMock;
import org.elasticsearch.xpack.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.xpack.watcher.actions.throttler.Throttler;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.Condition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.history.WatchRecord;
import org.elasticsearch.xpack.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.watcher.input.Input;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.joda.time.DateTime.now;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutionServiceTests extends ESTestCase {

    private Payload payload;
    private ExecutableInput input;
    private Input.Result inputResult;

    private TriggeredWatchStore triggeredWatchStore;
    private WatchExecutor executor;
    private HistoryStore historyStore;
    private ExecutionService executionService;
    private Clock clock;
    private ThreadPool threadPool;
    private WatcherClientProxy client;
    private Watch.Parser parser;

    @Before
    public void init() throws Exception {
        payload = mock(Payload.class);
        input = mock(ExecutableInput.class);
        inputResult = mock(Input.Result.class);
        when(inputResult.status()).thenReturn(Input.Result.Status.SUCCESS);
        when(inputResult.payload()).thenReturn(payload);
        when(input.execute(any(WatchExecutionContext.class), any(Payload.class))).thenReturn(inputResult);

        triggeredWatchStore = mock(TriggeredWatchStore.class);
        historyStore = mock(HistoryStore.class);

        executor = mock(WatchExecutor.class);
        when(executor.queue()).thenReturn(new ArrayBlockingQueue<>(1));

        clock = ClockMock.frozen();
        threadPool = mock(ThreadPool.class);

        client = mock(WatcherClientProxy.class);
        parser = mock(Watch.Parser.class);
        executionService = new ExecutionService(Settings.EMPTY, historyStore, triggeredWatchStore, executor, clock, threadPool,
                parser, client);

        ClusterState clusterState = mock(ClusterState.class);
        when(triggeredWatchStore.loadTriggeredWatches(clusterState)).thenReturn(new ArrayList<>());
        executionService.start(clusterState);
    }

    public void testExecute() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(client.getWatch("_id")).thenReturn(getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        Transform.Result watchTransformResult = mock(Transform.Result.class);
        when(watchTransformResult.status()).thenReturn(Transform.Result.Status.SUCCESS);
        when(watchTransformResult.payload()).thenReturn(payload);
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);
        when(watchTransform.execute(context, payload)).thenReturn(watchTransformResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action level conditional
        Condition actionCondition = null;
        Condition.Result actionConditionResult = null;

        if (randomBoolean()) {
            Tuple<Condition, Condition.Result> pair = whenCondition(context);

            actionCondition = pair.v1();
            actionConditionResult = pair.v2();
        }

        // action level transform
        ExecutableTransform actionTransform = null;
        Transform.Result actionTransformResult = null;

        if (randomBoolean()) {
            Tuple<ExecutableTransform, Transform.Result> pair = whenTransform(context);

            actionTransform = pair.v1();
            actionTransformResult = pair.v2();
        }

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("MY_AWESOME_TYPE");
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);

        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), sameInstance(watchTransformResult));
        ActionWrapper.Result result = watchRecord.result().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.condition(), sameInstance(actionConditionResult));
        assertThat(result.transform(), sameInstance(actionTransformResult));
        assertThat(result.action(), sameInstance(actionResult));

        verify(historyStore, times(1)).put(watchRecord);
        verify(condition, times(1)).execute(context);
        verify(watchTransform, times(1)).execute(context, payload);
        verify(action, times(1)).execute("_action", context, payload);

        // test stats
        XContentSource source = new XContentSource(jsonBuilder().map(executionService.usageStats()));
        assertThat(source.getValue("execution.actions._all.total_time_in_ms"), is(notNullValue()));
        assertThat(source.getValue("execution.actions._all.total"), is(1));
        assertThat(source.getValue("execution.actions.MY_AWESOME_TYPE.total_time_in_ms"), is(notNullValue()));
        assertThat(source.getValue("execution.actions.MY_AWESOME_TYPE.total"), is(1));
    }

    public void testExecuteFailedInput() throws Exception {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(client.getWatch("_id")).thenReturn(getResponse);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        input = mock(ExecutableInput.class);
        Input.Result inputResult = mock(Input.Result.class);
        when(inputResult.status()).thenReturn(Input.Result.Status.FAILURE);
        when(inputResult.reason()).thenReturn("_reason");
        when(input.execute(eq(context), any(Payload.class))).thenReturn(inputResult);

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
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

        // action level condition (unused)
        Condition actionCondition = randomBoolean() ? mock(Condition.class) : null;
        // action level transform (unused)
        ExecutableTransform actionTransform = randomBoolean() ? mock(ExecutableTransform.class) : null;

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord.result().inputResult(), is(inputResult));
        assertThat(watchRecord.result().conditionResult(), nullValue());
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults(), notNullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(0));

        verify(historyStore, times(1)).put(watchRecord);
        verify(input, times(1)).execute(context, null);
        verify(condition, never()).execute(context);
        verify(watchTransform, never()).execute(context, payload);
        verify(action, never()).execute("_action", context, payload);
    }

    public void testExecuteFailedCondition() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(client.getWatch("_id")).thenReturn(getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition condition = mock(Condition.class);
        Condition.Result conditionResult = mock(Condition.Result.class);
        when(conditionResult.status()).thenReturn(Condition.Result.Status.FAILURE);
        when(conditionResult.reason()).thenReturn("_reason");
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

        // action level condition (unused)
        Condition actionCondition = randomBoolean() ? mock(Condition.class) : null;
        // action level transform (unused)
        ExecutableTransform actionTransform = randomBoolean() ? mock(ExecutableTransform.class) : null;

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord.result().inputResult(), is(inputResult));
        assertThat(watchRecord.result().conditionResult(), is(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults(), notNullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(0));

        verify(historyStore, times(1)).put(watchRecord);
        verify(input, times(1)).execute(context, null);
        verify(condition, times(1)).execute(context);
        verify(watchTransform, never()).execute(context, payload);
        verify(action, never()).execute("_action", context, payload);
    }

    public void testExecuteFailedWatchTransform() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(client.getWatch("_id")).thenReturn(getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        Transform.Result watchTransformResult = mock(Transform.Result.class);
        when(watchTransformResult.status()).thenReturn(Transform.Result.Status.FAILURE);
        when(watchTransformResult.reason()).thenReturn("_reason");
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);
        when(watchTransform.execute(context, payload)).thenReturn(watchTransformResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action level condition (unused)
        Condition actionCondition = randomBoolean() ? mock(Condition.class) : null;
        // action level transform (unused)
        ExecutableTransform actionTransform = randomBoolean() ? mock(ExecutableTransform.class) : null;

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord.result().inputResult(), is(inputResult));
        assertThat(watchRecord.result().conditionResult(), is(conditionResult));
        assertThat(watchRecord.result().transformResult(), is(watchTransformResult));
        assertThat(watchRecord.result().actionsResults(), notNullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(0));

        verify(historyStore, times(1)).put(watchRecord);
        verify(input, times(1)).execute(context, null);
        verify(condition, times(1)).execute(context);
        verify(watchTransform, times(1)).execute(context, payload);
        verify(action, never()).execute("_action", context, payload);
    }

    public void testExecuteFailedActionTransform() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(client.getWatch("_id")).thenReturn(getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        Transform.Result watchTransformResult = mock(Transform.Result.class);
        when(watchTransformResult.status()).thenReturn(Transform.Result.Status.SUCCESS);
        when(watchTransformResult.payload()).thenReturn(payload);
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);
        when(watchTransform.execute(context, payload)).thenReturn(watchTransformResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action level condition
        Condition actionCondition = null;
        Condition.Result actionConditionResult = null;

        if (randomBoolean()) {
            Tuple<Condition, Condition.Result> pair = whenCondition(context);

            actionCondition = pair.v1();
            actionConditionResult = pair.v2();
        }

        // action level transform
        Transform.Result actionTransformResult = mock(Transform.Result.class);
        when(actionTransformResult.status()).thenReturn(Transform.Result.Status.FAILURE);
        when(actionTransformResult.reason()).thenReturn("_reason");
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);
        when(actionTransform.execute(context, payload)).thenReturn(actionTransformResult);

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.logger()).thenReturn(logger);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);

        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord.result().inputResult(), is(inputResult));
        assertThat(watchRecord.result().conditionResult(), is(conditionResult));
        assertThat(watchRecord.result().transformResult(), is(watchTransformResult));
        assertThat(watchRecord.result().actionsResults(), notNullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        assertThat(watchRecord.result().actionsResults().get("_action").condition(), is(actionConditionResult));
        assertThat(watchRecord.result().actionsResults().get("_action").transform(), is(actionTransformResult));
        assertThat(watchRecord.result().actionsResults().get("_action").action().status(), is(Action.Result.Status.FAILURE));

        verify(historyStore, times(1)).put(watchRecord);
        verify(input, times(1)).execute(context, null);
        verify(condition, times(1)).execute(context);
        verify(watchTransform, times(1)).execute(context, payload);
        // the action level transform is executed before the action itself
        verify(action, never()).execute("_action", context, payload);
    }

    public void testExecuteInner() throws Exception {
        DateTime now = now(DateTimeZone.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
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

        // action level conditional
        Condition actionCondition = null;
        Condition.Result actionConditionResult = null;

        if (randomBoolean()) {
            Tuple<Condition, Condition.Result> pair = whenCondition(context);

            actionCondition = pair.v1();
            actionConditionResult = pair.v2();
        }

        // action level transform
        ExecutableTransform actionTransform = null;
        Transform.Result actionTransformResult = null;

        if (randomBoolean()) {
            Tuple<ExecutableTransform, Transform.Result> pair = whenTransform(context);

            actionTransform = pair.v1();
            actionTransformResult = pair.v2();
        }

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(new DateTime(clock.millis()), singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), sameInstance(watchTransformResult));
        ActionWrapper.Result result = watchRecord.result().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.condition(), sameInstance(actionConditionResult));
        assertThat(result.transform(), sameInstance(actionTransformResult));
        assertThat(result.action(), sameInstance(actionResult));

        verify(condition, times(1)).execute(context);
        verify(watchTransform, times(1)).execute(context, payload);
        verify(action, times(1)).execute("_action", context, payload);
    }

    public void testExecuteInnerThrottled() throws Exception {
        DateTime now = now(DateTimeZone.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);
        when(throttleResult.reason()).thenReturn("_throttle_reason");
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // unused with throttle
        Condition actionCondition = mock(Condition.class);
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(new DateTime(clock.millis()), singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        ActionWrapper.Result result = watchRecord.result().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.condition(), nullValue());
        assertThat(result.transform(), nullValue());
        assertThat(result.action(), instanceOf(Action.Result.Throttled.class));
        Action.Result.Throttled throttled = (Action.Result.Throttled) result.action();
        assertThat(throttled.reason(), is("_throttle_reason"));

        verify(condition, times(1)).execute(context);
        verify(throttler, times(1)).throttle("_action", context);
        verify(actionCondition, never()).execute(context);
        verify(actionTransform, never()).execute(context, payload);
    }

    public void testExecuteInnerConditionNotMet() throws Exception {
        DateTime now = now(DateTimeZone.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action condition (always fails)
        Condition.Result actionConditionResult = mock(Condition.Result.class);
        // note: sometimes it can be met _with_ success
        if (randomBoolean()) {
            when(actionConditionResult.status()).thenReturn(Condition.Result.Status.SUCCESS);
        } else {
            when(actionConditionResult.status()).thenReturn(Condition.Result.Status.FAILURE);
        }
        when(actionConditionResult.met()).thenReturn(false);
        Condition actionCondition = mock(Condition.class);
        when(actionCondition.execute(context)).thenReturn(actionConditionResult);

        // unused with failed condition
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(new DateTime(clock.millis()), singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        ActionWrapper.Result result = watchRecord.result().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.condition(), sameInstance(actionConditionResult));
        assertThat(result.transform(), nullValue());
        assertThat(result.action(), instanceOf(Action.Result.ConditionFailed.class));
        Action.Result.ConditionFailed conditionFailed = (Action.Result.ConditionFailed) result.action();
        assertThat(conditionFailed.reason(), is("condition not met. skipping"));

        verify(condition, times(1)).execute(context);
        verify(throttler, times(1)).throttle("_action", context);
        verify(actionCondition, times(1)).execute(context);
        verify(actionTransform, never()).execute(context, payload);
    }

    public void testExecuteInnerConditionNotMetDueToException() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(getTestName());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action condition (always fails)
        Condition actionCondition = mock(Condition.class);
        when(actionCondition.execute(context)).thenThrow(new IllegalArgumentException("[expected] failed for test"));

        // unused with failed condition
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        when(action.logger()).thenReturn(logger);
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);
        WatchStatus watchStatus = new WatchStatus(new DateTime(clock.millis()), singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        ActionWrapper.Result result = watchRecord.result().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.condition(), nullValue());
        assertThat(result.transform(), nullValue());
        assertThat(result.action(), instanceOf(Action.Result.ConditionFailed.class));
        Action.Result.ConditionFailed conditionFailed = (Action.Result.ConditionFailed) result.action();
        assertThat(conditionFailed.reason(), is("condition failed. skipping: [expected] failed for test"));

        verify(condition, times(1)).execute(context);
        verify(throttler, times(1)).throttle("_action", context);
        verify(actionCondition, times(1)).execute(context);
        verify(actionTransform, never()).execute(context, payload);
    }

    public void testExecuteConditionNotMet() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = new TriggeredExecutionContext(watch, now, event, timeValueSeconds(5));

        Condition.Result conditionResult = NeverCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);

        // action throttler
        ActionThrottler throttler = mock(ActionThrottler.class);
        Condition actionCondition = mock(Condition.class);
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);
        ExecutableAction action = mock(ExecutableAction.class);
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action);

        WatchStatus watchStatus = new WatchStatus(new DateTime(clock.millis()), singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(0));

        verify(condition, times(1)).execute(context);
        verify(watchTransform, never()).execute(context, payload);
        verify(throttler, never()).throttle("_action", context);
        verify(actionTransform, never()).execute(context, payload);
        verify(action, never()).execute("_action", context, payload);
    }

    public void testThatTriggeredWatchDeletionWorksOnExecutionRejection() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("foo");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getId()).thenReturn("foo");
        when(client.getWatch(any())).thenReturn(getResponse);
        when(parser.parseWithSecrets(eq("foo"), eq(true), any(), any(), any())).thenReturn(watch);

        // execute needs to fail as well as storing the history
        doThrow(new EsRejectedExecutionException()).when(executor).execute(any());
        doThrow(new ElasticsearchException("whatever")).when(historyStore).forcePut(any());

        Wid wid = new Wid(watch.id(), now());

        final ExecutorService currentThreadExecutor = EsExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(currentThreadExecutor);

        TriggeredWatch triggeredWatch = new TriggeredWatch(wid, new ScheduleTriggerEvent(now() ,now()));
        executionService.executeTriggeredWatches(Collections.singleton(triggeredWatch));

        verify(triggeredWatchStore, times(1)).delete(wid);
        verify(historyStore, times(1)).forcePut(any(WatchRecord.class));
    }

    public void testThatSingleWatchCannotBeExecutedConcurrently() throws Exception {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        when(ctx.watch()).thenReturn(watch);

        executionService.getCurrentExecutions().put("_id", new ExecutionService.WatchExecution(ctx, Thread.currentThread()));

        executionService.execute(ctx);

        verify(ctx).abortBeforeExecution(eq(ExecutionState.NOT_EXECUTED_ALREADY_QUEUED), eq("Watch is already queued in thread pool"));
    }

    public void testExecuteWatchNotFound() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(ctx.knownWatch()).thenReturn(true);
        when(ctx.watch()).thenReturn(watch);

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        boolean exceptionThrown = false;
        if (randomBoolean()) {
            when(client.getWatch("_id")).thenReturn(getResponse);
        } else {
            // this emulates any failure while getting the watch, while index not found is an accepted issue
            if (randomBoolean()) {
                exceptionThrown = true;
                ElasticsearchException e = new ElasticsearchException("something went wrong, i.e. index not found");
                when(client.getWatch("_id")).thenThrow(e);
                WatchExecutionResult result = new WatchExecutionResult(ctx, randomInt(10));
                WatchRecord wr = new WatchRecord.ExceptionWatchRecord(ctx, result, e);
                when(ctx.abortFailedExecution(eq(e))).thenReturn(wr);
            } else {
                when(client.getWatch("_id")).thenThrow(new IndexNotFoundException(".watch"));
            }
        }

        WatchRecord.MessageWatchRecord record = mock(WatchRecord.MessageWatchRecord.class);
        when(record.state()).thenReturn(ExecutionState.NOT_EXECUTED_WATCH_MISSING);
        when(ctx.abortBeforeExecution(eq(ExecutionState.NOT_EXECUTED_WATCH_MISSING), any())).thenReturn(record);
        when(ctx.executionPhase()).thenReturn(ExecutionPhase.AWAITS_EXECUTION);

        WatchRecord watchRecord = executionService.execute(ctx);
        if (exceptionThrown) {
            assertThat(watchRecord.state(), is(ExecutionState.FAILED));
        } else {
            assertThat(watchRecord.state(), is(ExecutionState.NOT_EXECUTED_WATCH_MISSING));
        }
    }

    public void testValidateStartWithClosedIndex() throws Exception {
        when(triggeredWatchStore.validate(anyObject())).thenReturn(true);
        ClusterState.Builder csBuilder = new ClusterState.Builder(new ClusterName("_name"));
        MetaData.Builder metaDataBuilder = MetaData.builder();
        Settings indexSettings = settings(Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();
        metaDataBuilder.put(IndexMetaData.builder(Watch.INDEX).state(IndexMetaData.State.CLOSE).settings(indexSettings));
        csBuilder.metaData(metaDataBuilder);

        assertThat(executionService.validate(csBuilder.build()), is(false));
    }

    private Tuple<Condition, Condition.Result> whenCondition(final WatchExecutionContext context) {
        Condition.Result conditionResult = mock(Condition.Result.class);
        when(conditionResult.met()).thenReturn(true);
        Condition condition = mock(Condition.class);
        when(condition.execute(context)).thenReturn(conditionResult);

        return new Tuple<>(condition, conditionResult);
    }

    private Tuple<ExecutableTransform, Transform.Result> whenTransform(final WatchExecutionContext context) {
        Transform.Result transformResult = mock(Transform.Result.class);
        when(transformResult.payload()).thenReturn(payload);
        ExecutableTransform transform = mock(ExecutableTransform.class);
        when(transform.execute(context, payload)).thenReturn(transformResult);

        return new Tuple<>(transform, transformResult);
    }
}

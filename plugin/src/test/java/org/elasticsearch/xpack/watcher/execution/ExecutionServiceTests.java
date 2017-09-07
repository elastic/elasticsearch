/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.ESTestCase;
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
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.support.xcontent.ObjectPath;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTrigger;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
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
    private Client client;
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
        client = mock(Client.class);
        parser = mock(Watch.Parser.class);

        DiscoveryNode discoveryNode = new DiscoveryNode("node_1", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                new HashSet<>(asList(DiscoveryNode.Role.values())), Version.CURRENT);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(discoveryNode);

        executionService = new ExecutionService(Settings.EMPTY, historyStore, triggeredWatchStore, executor, clock, parser,
                clusterService, client);

        executionService.start();
    }

    public void testExecute() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any())).thenReturn(watch);

        Condition.Result conditionResult = AlwaysCondition.RESULT_INSTANCE;
        Condition condition = mock(Condition.class);
        // introduce a very short sleep time which we can use to check if the duration in milliseconds is correctly created
        long randomConditionDurationMs = randomIntBetween(1, 10);
        when(condition.execute(any(WatchExecutionContext.class))).then(invocationOnMock -> {
            Thread.sleep(randomConditionDurationMs);
            return conditionResult;
        });

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
        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), sameInstance(watchTransformResult));
        assertThat(watchRecord.getNodeId(), is("node_1"));
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

        // test execution duration
        assertThat(watchRecord.result().executionDurationMs(), is(greaterThanOrEqualTo(randomConditionDurationMs)));
        assertThat(watchRecord.result().executionTime(), is(notNullValue()));

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
        mockGetWatchResponse(client, "_id", getResponse);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any())).thenReturn(watch);

        input = mock(ExecutableInput.class);
        Input.Result inputResult = mock(Input.Result.class);
        when(inputResult.status()).thenReturn(Input.Result.Status.FAILURE);
        when(inputResult.getException()).thenReturn(new IOException());
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
        mockGetWatchResponse(client, "_id", getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any())).thenReturn(watch);

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
        mockGetWatchResponse(client, "_id", getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any())).thenReturn(watch);

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
        mockGetWatchResponse(client, "_id", getResponse);
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any())).thenReturn(watch);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));

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
        DateTime now = now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

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
        DateTime now = now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

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
        DateTime now = now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

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
        DateTime now = DateTime.now(UTC);
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(getTestName());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

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
        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));
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
        DateTime now = DateTime.now(UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

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
        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));
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
        mockGetWatchResponse(client, "foo", getResponse);
        when(parser.parseWithSecrets(eq("foo"), eq(true), any(), any(), any())).thenReturn(watch);

        // execute needs to fail as well as storing the history
        doThrow(new EsRejectedExecutionException()).when(executor).execute(any());
        doThrow(new ElasticsearchException("whatever")).when(historyStore).forcePut(any());

        Wid wid = new Wid(watch.id(), now());

        TriggeredWatch triggeredWatch = new TriggeredWatch(wid, new ScheduleTriggerEvent(now() ,now()));
        executionService.executeTriggeredWatches(Collections.singleton(triggeredWatch));

        verify(triggeredWatchStore, times(1)).delete(wid);
        verify(historyStore, times(1)).forcePut(any(WatchRecord.class));
    }

    public void testThatTriggeredWatchDeletionHappensOnlyIfWatchExists() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);

        DateTime now = new DateTime(clock.millis());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = ManualExecutionContext.builder(watch, false, new ManualTriggerEvent("foo", event),
                timeValueSeconds(5)).build();

        // action throttler, no throttling
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("MY_AWESOME_TYPE");
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, null, null, action);

        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(AlwaysCondition.INSTANCE);
        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        executionService.execute(context);
        verify(triggeredWatchStore, never()).delete(any());
    }

    public void testThatSingleWatchCannotBeExecutedConcurrently() throws Exception {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        when(ctx.watch()).thenReturn(watch);
        Wid wid = new Wid(watch.id(), DateTime.now(UTC));
        when(ctx.id()).thenReturn(wid);

        executionService.getCurrentExecutions().put("_id", new ExecutionService.WatchExecution(ctx, Thread.currentThread()));

        executionService.execute(ctx);

        verify(ctx).abortBeforeExecution(eq(ExecutionState.NOT_EXECUTED_ALREADY_QUEUED), eq("Watch is already queued in thread pool"));
    }

    public void testExecuteWatchNotFound() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent(watch.id(), new DateTime(0, UTC), new DateTime(0, UTC)),
                TimeValue.timeValueSeconds(5));

        GetResponse notFoundResponse = mock(GetResponse.class);
        when(notFoundResponse.isExists()).thenReturn(false);
        mockGetWatchResponse(client, "_id", notFoundResponse);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord, not(nullValue()));
        assertThat(watchRecord.state(), is(ExecutionState.NOT_EXECUTED_WATCH_MISSING));
    }

    public void testExecuteWatchIndexNotFoundException() {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent(watch.id(), new DateTime(0, UTC), new DateTime(0, UTC)),
                TimeValue.timeValueSeconds(5));

        mockGetWatchException(client, "_id", new IndexNotFoundException(".watch"));
        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord, not(nullValue()));
        assertThat(watchRecord.state(), is(ExecutionState.NOT_EXECUTED_WATCH_MISSING));
    }

    public void testExecuteWatchParseWatchException() {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(),
                new DateTime(0, UTC),
                new ScheduleTriggerEvent(watch.id(), new DateTime(0, UTC), new DateTime(0, UTC)),
                TimeValue.timeValueSeconds(5));

        IOException e = new IOException("something went wrong, i.e. index not found");
        mockGetWatchException(client, "_id", e);

        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord, not(nullValue()));
        assertThat(watchRecord.state(), is(ExecutionState.FAILED));
    }

    public void testWatchInactive() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(ctx.knownWatch()).thenReturn(true);
        WatchStatus status = mock(WatchStatus.class);
        when(status.state()).thenReturn(new WatchStatus.State(false, now()));
        when(watch.status()).thenReturn(status);
        when(ctx.watch()).thenReturn(watch);
        Wid wid = new Wid(watch.id(), DateTime.now(UTC));
        when(ctx.id()).thenReturn(wid);

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any())).thenReturn(watch);

        WatchRecord.MessageWatchRecord record = mock(WatchRecord.MessageWatchRecord.class);
        when(record.state()).thenReturn(ExecutionState.EXECUTION_NOT_NEEDED);
        when(ctx.abortBeforeExecution(eq(ExecutionState.EXECUTION_NOT_NEEDED), eq("Watch is not active"))).thenReturn(record);

        WatchRecord watchRecord = executionService.execute(ctx);
        assertThat(watchRecord.state(), is(ExecutionState.EXECUTION_NOT_NEEDED));
    }

    public void testCurrentExecutionSnapshots() throws Exception {
        DateTime time = DateTime.now(UTC);
        int snapshotCount = randomIntBetween(2, 8);
        for (int i = 0; i < snapshotCount; i++) {
            time = time.minusSeconds(10);
            WatchExecutionContext ctx = createMockWatchExecutionContext("_id" + i, time);
            executionService.getCurrentExecutions().put("_id" + i, new ExecutionService.WatchExecution(ctx, Thread.currentThread()));
        }

        List<WatchExecutionSnapshot> snapshots = executionService.currentExecutions();
        assertThat(snapshots, hasSize(snapshotCount));
        assertThat(snapshots.get(0).watchId(), is("_id" + (snapshotCount-1)));
        assertThat(snapshots.get(snapshots.size() - 1).watchId(), is("_id0"));
    }

    public void testQueuedWatches() throws Exception {
        Collection<Runnable> tasks = new ArrayList<>();
        DateTime time = DateTime.now(UTC);
        int queuedWatchCount = randomIntBetween(2, 8);
        for (int i = 0; i < queuedWatchCount; i++) {
            time = time.minusSeconds(10);
            WatchExecutionContext ctx = createMockWatchExecutionContext("_id" + i, time);
            tasks.add(new ExecutionService.WatchExecutionTask(ctx, () -> logger.info("this will never be called")));
        }

        when(executor.tasks()).thenReturn(tasks.stream());

        List<QueuedWatch> queuedWatches = executionService.queuedWatches();
        assertThat(queuedWatches, hasSize(queuedWatchCount));
        assertThat(queuedWatches.get(0).watchId(), is("_id" + (queuedWatchCount-1)));
        assertThat(queuedWatches.get(queuedWatches.size() - 1).watchId(), is("_id0"));
    }

    public void testUpdateWatchStatusDoesNotUpdateState() throws Exception {
        WatchStatus status = new WatchStatus(DateTime.now(UTC), Collections.emptyMap());
        Watch watch = new Watch("_id", new ManualTrigger(), new ExecutableNoneInput(logger), AlwaysCondition.INSTANCE, null, null,
                Collections.emptyList(), null, status);

        final AtomicBoolean assertionsTriggered = new AtomicBoolean(false);
        doAnswer(invocation -> {
            UpdateRequest request = (UpdateRequest) invocation.getArguments()[0];
            try (XContentParser parser =
                         XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY, request.doc().source())) {
                Map<String, Object> map = parser.map();
                Map<String, String> state = ObjectPath.eval("status.state", map);
                assertThat(state, is(nullValue()));
                assertionsTriggered.set(true);
            }

            PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
            future.onResponse(new UpdateResponse());
            return future;
        }).when(client).update(any());

        executionService.updateWatchStatus(watch);

        assertThat(assertionsTriggered.get(), is(true));
    }

    private WatchExecutionContext createMockWatchExecutionContext(String watchId, DateTime executionTime) {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(ctx.id()).thenReturn(new Wid(watchId, executionTime));
        when(ctx.executionTime()).thenReturn(executionTime);
        when(ctx.executionPhase()).thenReturn(ExecutionPhase.INPUT);

        TriggerEvent triggerEvent = mock(TriggerEvent.class);
        when(triggerEvent.triggeredTime()).thenReturn(executionTime.minusSeconds(1));
        when(ctx.triggerEvent()).thenReturn(triggerEvent);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(watchId);
        when(ctx.watch()).thenReturn(watch);

        WatchExecutionSnapshot snapshot = new WatchExecutionSnapshot(ctx, new StackTraceElement[]{});
        when(ctx.createSnapshot(anyObject())).thenReturn(snapshot);

        return ctx;
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

    private void mockGetWatchResponse(Client client, String id, GetResponse response) {
        doAnswer(invocation -> {
            GetRequest request = (GetRequest) invocation.getArguments()[0];
            ActionListener<GetResponse> listener = (ActionListener) invocation.getArguments()[1];
            if (request.id().equals(id)) {
                listener.onResponse(response);
            } else {
                GetResult notFoundResult = new GetResult(request.index(), request.type(), request.id(), -1, false, null, null);
                listener.onResponse(new GetResponse(notFoundResult));
            }
            return null;
        }).when(client).get(any(), any());
    }

    private void mockGetWatchException(Client client, String id, Exception e) {
        doAnswer(invocation -> {
            GetRequest request = (GetRequest) invocation.getArguments()[0];
            ActionListener<GetResponse> listener = (ActionListener) invocation.getArguments()[1];
            if (request.id().equals(id)) {
                listener.onFailure(e);
            } else {
                GetResult notFoundResult = new GetResult(request.index(), request.type(), request.id(), -1, false, null, null);
                listener.onResponse(new GetResponse(notFoundResult));
            }
            return null;
        }).when(client).get(any(), any());
    }
}

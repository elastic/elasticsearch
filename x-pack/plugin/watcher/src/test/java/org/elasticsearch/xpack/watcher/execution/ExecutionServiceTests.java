/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionPhase;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.execution.QueuedWatch;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionSnapshot;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.history.WatchRecord;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTrigger;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.WatchParser;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({ "rawtypes", "unchecked" })
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
    private WatchParser parser;

    @Before
    public void init() throws Exception {
        payload = mock(Payload.class);
        input = mock(ExecutableInput.class);
        inputResult = mock(Input.Result.class);
        when(inputResult.status()).thenReturn(Input.Result.Status.SUCCESS);
        when(inputResult.payload()).thenReturn(payload);
        when(input.execute(any(WatchExecutionContext.class), nullable(Payload.class))).thenReturn(inputResult);

        triggeredWatchStore = mock(TriggeredWatchStore.class);
        historyStore = mock(HistoryStore.class);

        executor = mock(WatchExecutor.class);
        when(executor.queue()).thenReturn(new ArrayBlockingQueue<>(1));

        clock = ClockMock.frozen();
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        parser = mock(WatchParser.class);

        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node_1",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(discoveryNode);

        executionService = new ExecutionService(
            Settings.EMPTY,
            historyStore,
            triggeredWatchStore,
            executor,
            clock,
            parser,
            clusterService,
            client,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    public void testExecute() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        // introduce a very short sleep time which we can use to check if the duration in milliseconds is correctly created
        long randomConditionDurationMs = randomIntBetween(5, 10);
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
        ExecutableCondition actionCondition = null;
        Condition.Result actionConditionResult = null;

        if (randomBoolean()) {
            Tuple<ExecutableCondition, Condition.Result> pair = whenCondition(context);

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

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);

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
        ActionWrapperResult result = watchRecord.result().actionsResults().get("_action");
        assertThat(result, notNullValue());
        assertThat(result.id(), is("_action"));
        assertThat(result.condition(), sameInstance(actionConditionResult));
        assertThat(result.transform(), sameInstance(actionTransformResult));
        assertThat(result.action(), sameInstance(actionResult));

        verify(historyStore, times(1)).put(watchRecord);
        verify(condition, times(1)).execute(context);
        verify(watchTransform, times(1)).execute(context, payload);
        verify(action, times(1)).execute("_action", context, payload);

        // test execution duration, make sure it is set at all
        // no exact duration check here, as different platforms handle sleep differently, so this might not be exact
        assertThat(watchRecord.result().executionDurationMs(), is(greaterThan(0L)));
        assertThat(watchRecord.result().executionTime(), is(notNullValue()));

        // test stats
        Counters counters = executionService.executionTimes();
        assertThat(counters.get("execution.actions._all.total_time_in_ms"), is(notNullValue()));
        assertThat(counters.get("execution.actions._all.total"), is(1L));
        assertThat(counters.get("execution.actions.MY_AWESOME_TYPE.total_time_in_ms"), is(notNullValue()));
        assertThat(counters.get("execution.actions.MY_AWESOME_TYPE.total"), is(1L));
    }

    public void testExecuteFailedInput() throws Exception {
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);

        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        input = mock(ExecutableInput.class);
        Input.Result inputResult = mock(Input.Result.class);
        when(inputResult.status()).thenReturn(Input.Result.Status.FAILURE);
        when(inputResult.getException()).thenReturn(new IOException());
        when(input.execute(eq(context), nullable(Payload.class))).thenReturn(inputResult);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
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

        // action level condition (unused)
        ExecutableCondition actionCondition = randomBoolean() ? mock(ExecutableCondition.class) : null;
        // action level transform (unused)
        ExecutableTransform actionTransform = randomBoolean() ? mock(ExecutableTransform.class) : null;

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
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

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        ExecutableCondition condition = mock(ExecutableCondition.class);
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
        ExecutableCondition actionCondition = randomBoolean() ? mock(ExecutableCondition.class) : null;
        // action level transform (unused)
        ExecutableTransform actionTransform = randomBoolean() ? mock(ExecutableTransform.class) : null;

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
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

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
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
        ExecutableCondition actionCondition = randomBoolean() ? mock(ExecutableCondition.class) : null;
        // action level transform (unused)
        ExecutableTransform actionTransform = randomBoolean() ? mock(ExecutableTransform.class) : null;

        // the action
        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.execute("_action", context, payload)).thenReturn(actionResult);

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
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
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
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
        ExecutableCondition actionCondition = null;
        Condition.Result actionConditionResult = null;

        if (randomBoolean()) {
            Tuple<ExecutableCondition, Condition.Result> pair = whenCondition(context);

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

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);

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
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
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

        // action level conditional
        ExecutableCondition actionCondition = null;
        Condition.Result actionConditionResult = null;

        if (randomBoolean()) {
            Tuple<ExecutableCondition, Condition.Result> pair = whenCondition(context);

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

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
        ZonedDateTime time = clock.instant().atZone(ZoneOffset.UTC);
        WatchStatus watchStatus = new WatchStatus(time, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.transform()).thenReturn(watchTransform);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), sameInstance(watchTransformResult));
        ActionWrapperResult result = watchRecord.result().actionsResults().get("_action");
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
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(true);
        when(throttleResult.reason()).thenReturn("_throttle_reason");
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // unused with throttle
        ExecutableCondition actionCondition = mock(ExecutableCondition.class);
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
        ZonedDateTime time = clock.instant().atZone(ZoneOffset.UTC);
        WatchStatus watchStatus = new WatchStatus(time, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        ActionWrapperResult result = watchRecord.result().actionsResults().get("_action");
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
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
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
        ExecutableCondition actionCondition = mock(ExecutableCondition.class);
        when(actionCondition.execute(context)).thenReturn(actionConditionResult);

        // unused with failed condition
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
        ZonedDateTime time = clock.instant().atZone(ZoneOffset.UTC);
        WatchStatus watchStatus = new WatchStatus(time, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(Arrays.asList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        ActionWrapperResult result = watchRecord.result().actionsResults().get("_action");
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
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn(getTestName());
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // action throttler
        Throttler.Result throttleResult = mock(Throttler.Result.class);
        when(throttleResult.throttle()).thenReturn(false);
        ActionThrottler throttler = mock(ActionThrottler.class);
        when(throttler.throttle("_action", context)).thenReturn(throttleResult);

        // action condition (always fails)
        ExecutableCondition actionCondition = mock(ExecutableCondition.class);
        when(actionCondition.execute(context)).thenThrow(new IllegalArgumentException("[expected] failed for test"));

        // unused with failed condition
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);

        ExecutableAction action = mock(ExecutableAction.class);
        when(action.type()).thenReturn("_type");
        when(action.logger()).thenReturn(logger);
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);
        ZonedDateTime time = clock.instant().atZone(ZoneOffset.UTC);
        WatchStatus watchStatus = new WatchStatus(time, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(condition);
        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        WatchRecord watchRecord = executionService.executeInner(context);
        assertThat(watchRecord.result().inputResult(), sameInstance(inputResult));
        assertThat(watchRecord.result().conditionResult(), sameInstance(conditionResult));
        assertThat(watchRecord.result().transformResult(), nullValue());
        assertThat(watchRecord.result().actionsResults().size(), is(1));
        ActionWrapperResult result = watchRecord.result().actionsResults().get("_action");
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
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Watch watch = mock(Watch.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);

        Condition.Result conditionResult = NeverCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);

        // watch level transform
        ExecutableTransform watchTransform = mock(ExecutableTransform.class);

        // action throttler
        ActionThrottler throttler = mock(ActionThrottler.class);
        ExecutableCondition actionCondition = mock(ExecutableCondition.class);
        ExecutableTransform actionTransform = mock(ExecutableTransform.class);
        ExecutableAction action = mock(ExecutableAction.class);
        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, actionCondition, actionTransform, action, null, null);

        ZonedDateTime time = clock.instant().atZone(ZoneOffset.UTC);
        WatchStatus watchStatus = new WatchStatus(time, singletonMap("_action", new ActionStatus(now)));

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
        WatchStatus status = new WatchStatus(ZonedDateTime.now(ZoneOffset.UTC), Collections.emptyMap());
        when(watch.status()).thenReturn(status);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getId()).thenReturn("foo");
        mockGetWatchResponse(client, "foo", getResponse);
        ActionFuture actionFuture = mock(ActionFuture.class);
        when(actionFuture.get()).thenReturn("");
        when(client.index(any())).thenReturn(actionFuture);
        when(client.delete(any())).thenReturn(actionFuture);

        when(parser.parseWithSecrets(eq("foo"), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        // execute needs to fail
        doThrow(new EsRejectedExecutionException()).when(executor).execute(any());

        Wid wid = new Wid(watch.id(), ZonedDateTime.now(ZoneOffset.UTC));

        TriggeredWatch triggeredWatch = new TriggeredWatch(
            wid,
            new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
        );
        executionService.executeTriggeredWatches(Collections.singleton(triggeredWatch));

        ArgumentCaptor<DeleteRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(client).delete(deleteCaptor.capture());
        assertThat(deleteCaptor.getValue().index(), equalTo(TriggeredWatchStoreField.INDEX_NAME));
        assertThat(deleteCaptor.getValue().id(), equalTo(wid.value()));

        ArgumentCaptor<IndexRequest> watchHistoryCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client).index(watchHistoryCaptor.capture());

        assertThat(watchHistoryCaptor.getValue().source().utf8ToString(), containsString(ExecutionState.THREADPOOL_REJECTION.toString()));
        assertThat(watchHistoryCaptor.getValue().index(), containsString(".watcher-history"));
    }

    public void testForcePutHistoryOnExecutionRejection() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("foo");
        WatchStatus status = new WatchStatus(ZonedDateTime.now(ZoneOffset.UTC), Collections.emptyMap());
        when(watch.status()).thenReturn(status);
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getId()).thenReturn("foo");
        mockGetWatchResponse(client, "foo", getResponse);
        ActionFuture actionFuture = mock(ActionFuture.class);
        when(actionFuture.get()).thenReturn("");
        when(client.index(any())).thenThrow(
            new VersionConflictEngineException(new ShardId(new Index("mockindex", "mockuuid"), 0), "id", "explaination")
        ).thenReturn(actionFuture);
        when(client.delete(any())).thenReturn(actionFuture);

        when(parser.parseWithSecrets(eq("foo"), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        // execute needs to fail
        doThrow(new EsRejectedExecutionException()).when(executor).execute(any());

        Wid wid = new Wid(watch.id(), ZonedDateTime.now(ZoneOffset.UTC));

        TriggeredWatch triggeredWatch = new TriggeredWatch(
            wid,
            new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC))
        );
        executionService.executeTriggeredWatches(Collections.singleton(triggeredWatch));

        ArgumentCaptor<DeleteRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(client).delete(deleteCaptor.capture());
        assertThat(deleteCaptor.getValue().index(), equalTo(TriggeredWatchStoreField.INDEX_NAME));
        assertThat(deleteCaptor.getValue().id(), equalTo(wid.value()));

        ArgumentCaptor<IndexRequest> watchHistoryCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client, times(2)).index(watchHistoryCaptor.capture());
        List<IndexRequest> indexRequests = watchHistoryCaptor.getAllValues();

        assertThat(indexRequests.get(0).id(), equalTo(indexRequests.get(1).id()));
        assertThat(indexRequests.get(0).source().utf8ToString(), containsString(ExecutionState.THREADPOOL_REJECTION.toString()));
        assertThat(indexRequests.get(1).source().utf8ToString(), containsString(ExecutionState.EXECUTED_MULTIPLE_TIMES.toString()));
    }

    public void testThatTriggeredWatchDeletionHappensOnlyIfWatchExists() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        WatchExecutionContext context = ManualExecutionContext.builder(
            watch,
            false,
            new ManualTriggerEvent("foo", event),
            timeValueSeconds(5)
        ).build();

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

        ActionWrapper actionWrapper = new ActionWrapper("_action", throttler, null, null, action, null, null);

        WatchStatus watchStatus = new WatchStatus(now, singletonMap("_action", new ActionStatus(now)));

        when(watch.input()).thenReturn(input);
        when(watch.condition()).thenReturn(InternalAlwaysCondition.INSTANCE);
        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));
        when(watch.status()).thenReturn(watchStatus);

        executionService.execute(context);
        verify(client, never()).delete(any());
    }

    public void testThatSingleWatchCannotBeExecutedConcurrently() throws Exception {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        when(ctx.watch()).thenReturn(watch);
        Wid wid = new Wid(watch.id(), ZonedDateTime.now(ZoneOffset.UTC));
        when(ctx.id()).thenReturn(wid);

        executionService.getCurrentExecutions().put("_id", new ExecutionService.WatchExecution(ctx, Thread.currentThread()));

        executionService.execute(ctx);

        verify(ctx).abortBeforeExecution(eq(ExecutionState.NOT_EXECUTED_ALREADY_QUEUED), eq("Watch is already queued in thread pool"));
    }

    public void testExecuteWatchNotFound() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        ZonedDateTime epochZeroTime = Instant.EPOCH.atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent triggerEvent = new ScheduleTriggerEvent(watch.id(), epochZeroTime, epochZeroTime);
        TriggeredExecutionContext context = new TriggeredExecutionContext(
            watch.id(),
            epochZeroTime,
            triggerEvent,
            TimeValue.timeValueSeconds(5)
        );

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
        ZonedDateTime epochZeroTime = Instant.EPOCH.atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent triggerEvent = new ScheduleTriggerEvent(watch.id(), epochZeroTime, epochZeroTime);
        TriggeredExecutionContext context = new TriggeredExecutionContext(
            watch.id(),
            epochZeroTime,
            triggerEvent,
            TimeValue.timeValueSeconds(5)
        );

        mockGetWatchException(client, "_id", new IndexNotFoundException(".watch"));
        WatchRecord watchRecord = executionService.execute(context);
        assertThat(watchRecord, not(nullValue()));
        assertThat(watchRecord.state(), is(ExecutionState.NOT_EXECUTED_WATCH_MISSING));
    }

    public void testExecuteWatchParseWatchException() {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");
        ZonedDateTime epochZeroTime = Instant.EPOCH.atZone(ZoneOffset.UTC);
        TriggeredExecutionContext context = new TriggeredExecutionContext(
            watch.id(),
            epochZeroTime,
            new ScheduleTriggerEvent(watch.id(), epochZeroTime, epochZeroTime),
            TimeValue.timeValueSeconds(5)
        );

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
        when(status.state()).thenReturn(new WatchStatus.State(false, ZonedDateTime.now(ZoneOffset.UTC)));
        when(watch.status()).thenReturn(status);
        when(ctx.watch()).thenReturn(watch);
        Wid wid = new Wid(watch.id(), ZonedDateTime.now(ZoneOffset.UTC));
        when(ctx.id()).thenReturn(wid);

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        mockGetWatchResponse(client, "_id", getResponse);
        when(parser.parseWithSecrets(eq(watch.id()), eq(true), any(), any(), any(), anyLong(), anyLong())).thenReturn(watch);

        WatchRecord.MessageWatchRecord record = mock(WatchRecord.MessageWatchRecord.class);
        when(record.state()).thenReturn(ExecutionState.EXECUTION_NOT_NEEDED);
        when(ctx.abortBeforeExecution(eq(ExecutionState.EXECUTION_NOT_NEEDED), eq("Watch is not active"))).thenReturn(record);

        WatchRecord watchRecord = executionService.execute(ctx);
        assertThat(watchRecord.state(), is(ExecutionState.EXECUTION_NOT_NEEDED));
    }

    public void testCurrentExecutionSnapshots() throws Exception {
        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);
        int snapshotCount = randomIntBetween(2, 8);
        for (int i = 0; i < snapshotCount; i++) {
            time = time.minusSeconds(10);
            WatchExecutionContext ctx = createMockWatchExecutionContext("_id" + i, time);
            executionService.getCurrentExecutions().put("_id" + i, new ExecutionService.WatchExecution(ctx, Thread.currentThread()));
        }

        List<WatchExecutionSnapshot> snapshots = executionService.currentExecutions();
        assertThat(snapshots, hasSize(snapshotCount));
        assertThat(snapshots.get(0).watchId(), is("_id" + (snapshotCount - 1)));
        assertThat(snapshots.get(snapshots.size() - 1).watchId(), is("_id0"));
    }

    public void testQueuedWatches() throws Exception {
        Collection<Runnable> tasks = new ArrayList<>();
        ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);
        int queuedWatchCount = randomIntBetween(2, 8);
        for (int i = 0; i < queuedWatchCount; i++) {
            time = time.minusSeconds(10);
            WatchExecutionContext ctx = createMockWatchExecutionContext("_id" + i, time);
            tasks.add(new ExecutionService.WatchExecutionTask(ctx, () -> logger.info("this will never be called")));
        }

        when(executor.tasks()).thenReturn(tasks.stream());

        List<QueuedWatch> queuedWatches = executionService.queuedWatches();
        assertThat(queuedWatches, hasSize(queuedWatchCount));
        assertThat(queuedWatches.get(0).watchId(), is("_id" + (queuedWatchCount - 1)));
        assertThat(queuedWatches.get(queuedWatches.size() - 1).watchId(), is("_id0"));
    }

    public void testUpdateWatchStatusDoesNotUpdateState() throws Exception {
        WatchStatus status = new WatchStatus(ZonedDateTime.now(ZoneOffset.UTC), Collections.emptyMap());
        Watch watch = new Watch(
            "_id",
            new ManualTrigger(),
            new ExecutableNoneInput(),
            InternalAlwaysCondition.INSTANCE,
            null,
            null,
            Collections.emptyList(),
            null,
            status,
            1L,
            1L
        );

        final AtomicBoolean assertionsTriggered = new AtomicBoolean(false);
        doAnswer(invocation -> {
            UpdateRequest request = (UpdateRequest) invocation.getArguments()[0];
            try (
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY, request.doc().source().streamInput())
            ) {
                Map<String, Object> map = parser.map();
                Map<String, String> state = ObjectPath.eval("status.state", map);
                assertThat(state, is(nullValue()));
                assertionsTriggered.set(true);
            }

            PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
            future.onResponse(new UpdateResponse(null, new ShardId("test", "test", 0), "test", 0, 0, 0, DocWriteResponse.Result.CREATED));
            return future;
        }).when(client).update(any());

        executionService.updateWatchStatus(watch);

        assertThat(assertionsTriggered.get(), is(true));
    }

    public void testManualWatchExecutionContextGetsAlwaysExecuted() throws Exception {
        Watch watch = mock(Watch.class);
        when(watch.id()).thenReturn("_id");

        ZonedDateTime now = clock.instant().atZone(ZoneOffset.UTC);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);
        ManualExecutionContext ctx = ManualExecutionContext.builder(watch, true, new ManualTriggerEvent("foo", event), timeValueSeconds(5))
            .build();

        when(watch.input()).thenReturn(input);
        Condition.Result conditionResult = InternalAlwaysCondition.RESULT_INSTANCE;
        ExecutableCondition condition = mock(ExecutableCondition.class);
        when(condition.execute(any(WatchExecutionContext.class))).thenReturn(conditionResult);
        when(watch.condition()).thenReturn(condition);

        Action.Result actionResult = mock(Action.Result.class);
        when(actionResult.type()).thenReturn("_action_type");
        when(actionResult.status()).thenReturn(Action.Result.Status.SUCCESS);
        ExecutableAction action = mock(ExecutableAction.class);
        when(action.logger()).thenReturn(logger);
        when(action.execute(eq("_action"), eq(ctx), eq(payload))).thenReturn(actionResult);

        ActionWrapper actionWrapper = mock(ActionWrapper.class);
        ActionWrapperResult actionWrapperResult = new ActionWrapperResult("_action", actionResult);
        when(actionWrapper.execute(any())).thenReturn(actionWrapperResult);

        when(watch.actions()).thenReturn(Collections.singletonList(actionWrapper));

        WatchStatus status = mock(WatchStatus.class);
        when(status.state()).thenReturn(new WatchStatus.State(false, ZonedDateTime.now(ZoneOffset.UTC)));
        when(watch.status()).thenReturn(status);

        WatchRecord watchRecord = executionService.execute(ctx);
        assertThat(watchRecord.state(), is(ExecutionState.EXECUTED));
    }

    public void testLoadingWatchExecutionUser() throws Exception {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        Watch watch = mock(Watch.class);
        WatchStatus status = mock(WatchStatus.class);
        ScheduleTriggerEvent event = new ScheduleTriggerEvent("_id", now, now);

        // Should be null
        TriggeredExecutionContext context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);
        assertNull(context.getUser());

        // Should still be null, header is not yet set
        when(watch.status()).thenReturn(status);
        context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);
        assertNull(context.getUser());

        Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("joe", "admin"))
            .realmRef(new Authentication.RealmRef("native_realm", "native", "node1"))
            .build(false);

        // Should no longer be null now that the proper header is set
        when(status.getHeaders()).thenReturn(Collections.singletonMap(AuthenticationField.AUTHENTICATION_KEY, authentication.encode()));
        context = new TriggeredExecutionContext(watch.id(), now, event, timeValueSeconds(5));
        context.ensureWatchExists(() -> watch);
        assertThat(context.getUser(), equalTo("joe"));
    }

    private WatchExecutionContext createMockWatchExecutionContext(String watchId, ZonedDateTime executionTime) {
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

        WatchExecutionSnapshot snapshot = new WatchExecutionSnapshot(ctx, new StackTraceElement[] {});
        when(ctx.createSnapshot(any())).thenReturn(snapshot);

        return ctx;
    }

    private Tuple<ExecutableCondition, Condition.Result> whenCondition(final WatchExecutionContext context) {
        Condition.Result conditionResult = mock(Condition.Result.class);
        when(conditionResult.met()).thenReturn(true);
        ExecutableCondition condition = mock(ExecutableCondition.class);
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
                GetResult notFoundResult = new GetResult(request.index(), request.id(), UNASSIGNED_SEQ_NO, 0, -1, false, null, null, null);
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
                GetResult notFoundResult = new GetResult(request.index(), request.id(), UNASSIGNED_SEQ_NO, 0, -1, false, null, null, null);
                listener.onResponse(new GetResponse(notFoundResult));
            }
            return null;
        }).when(client).get(any(), any());
    }
}

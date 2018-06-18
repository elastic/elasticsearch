/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.watcher.actions.ActionStatus.AckStatus.State;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionWrapperTests extends ESTestCase {

    private DateTime now = DateTime.now(DateTimeZone.UTC);
    private Watch watch = mock(Watch.class);
    private ExecutableAction executableAction = mock(ExecutableAction.class);
    private ActionWrapper actionWrapper = new ActionWrapper("_action", null, NeverCondition.INSTANCE, null, executableAction);

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

    private WatchExecutionContext mockExecutionContent(Watch watch) {
        WatchExecutionContext ctx = mock(WatchExecutionContext.class);
        when(watch.id()).thenReturn("watchId");
        when(ctx.watch()).thenReturn(watch);
        when(ctx.skipThrottling(eq("_action"))).thenReturn(true);
        return ctx;
    }

    private ActionStatus createActionStatus(State state) {
        ActionStatus.AckStatus ackStatus = new ActionStatus.AckStatus(now, state);
        ActionStatus.Execution execution = ActionStatus.Execution.successful(now);
        return new ActionStatus(ackStatus, execution, execution, null);
    }
}
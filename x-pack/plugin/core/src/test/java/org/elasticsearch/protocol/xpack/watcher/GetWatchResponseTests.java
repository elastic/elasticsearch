/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetWatchResponseTests extends
    AbstractHlrcStreamableXContentTestCase<GetWatchResponse, org.elasticsearch.client.watcher.GetWatchResponse> {

    private static final String[] SHUFFLE_FIELDS_EXCEPTION = new String[] { "watch" };

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return SHUFFLE_FIELDS_EXCEPTION;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected List<XContentType> supportedContentTypes() {
        // The get watch API supports JSON output only.
        return Arrays.asList(XContentType.JSON);
    }

    @Override
    protected GetWatchResponse createBlankInstance() {
        return new GetWatchResponse();
    }

    @Override
    protected GetWatchResponse createTestInstance() {
        String id = randomAlphaOfLength(10);
        if (rarely()) {
            return new GetWatchResponse(id);
        }
        long version = randomLongBetween(0, 10);
        WatchStatus status = randomWatchStatus();
        BytesReference source = emptyWatch();
        return new GetWatchResponse(id, version, status, new XContentSource(source, XContentType.JSON));
    }

    private static BytesReference emptyWatch() {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject().endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static WatchStatus randomWatchStatus() {
        long version = randomLongBetween(-1, Long.MAX_VALUE);
        WatchStatus.State state = new WatchStatus.State(randomBoolean(), DateTime.now(DateTimeZone.UTC));
        ExecutionState executionState = randomFrom(ExecutionState.values());
        DateTime lastChecked = rarely() ? null : DateTime.now(DateTimeZone.UTC);
        DateTime lastMetCondition = rarely() ? null : DateTime.now(DateTimeZone.UTC);
        int size = randomIntBetween(0, 5);
        Map<String, ActionStatus> actionMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            ActionStatus.AckStatus ack = new ActionStatus.AckStatus(
                DateTime.now(DateTimeZone.UTC),
                randomFrom(ActionStatus.AckStatus.State.values())
            );
            ActionStatus actionStatus = new ActionStatus(
                ack,
                randomBoolean() ? null : randomExecution(),
                randomBoolean() ? null : randomExecution(),
                randomBoolean() ? null : randomThrottle()
            );
            actionMap.put(randomAlphaOfLength(10), actionStatus);
        }
        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actionMap, null);
    }

    private static ActionStatus.Throttle randomThrottle() {
        return new ActionStatus.Throttle(DateTime.now(DateTimeZone.UTC), randomAlphaOfLengthBetween(10, 20));
    }

    private static ActionStatus.Execution randomExecution() {
        if (randomBoolean()) {
            return null;
        } else if (randomBoolean()) {
            return ActionStatus.Execution.failure(DateTime.now(DateTimeZone.UTC), randomAlphaOfLengthBetween(10, 20));
        } else {
            return ActionStatus.Execution.successful(DateTime.now(DateTimeZone.UTC));
        }
    }

    @Override
    public org.elasticsearch.client.watcher.GetWatchResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.watcher.GetWatchResponse.fromXContent(parser);
    }

    @Override
    public GetWatchResponse convertHlrcToInternal(org.elasticsearch.client.watcher.GetWatchResponse instance) {
        if (instance.isFound()) {
            return new GetWatchResponse(instance.getId(), instance.getVersion(), convertHlrcToInternal(instance.getStatus()),
                new XContentSource(instance.getSource(), instance.getContentType()));
        } else {
            return new GetWatchResponse(instance.getId());
        }
    }

    private static WatchStatus convertHlrcToInternal(org.elasticsearch.client.watcher.WatchStatus status) {
        final Map<String, ActionStatus> actions = new HashMap<>();
        for (Map.Entry<String, org.elasticsearch.client.watcher.ActionStatus> entry : status.getActions().entrySet()) {
            actions.put(entry.getKey(), convertHlrcToInternal(entry.getValue()));
        }
        return new WatchStatus(status.version(),
            convertHlrcToInternal(status.state()),
            status.getExecutionState() == null ? null : convertHlrcToInternal(status.getExecutionState()),
            status.lastChecked(), status.lastMetCondition(), actions, null
        );
    }

    private static ActionStatus convertHlrcToInternal(org.elasticsearch.client.watcher.ActionStatus actionStatus) {
        return new ActionStatus(convertHlrcToInternal(actionStatus.ackStatus()),
            actionStatus.lastExecution() == null ? null : convertHlrcToInternal(actionStatus.lastExecution()),
            actionStatus.lastSuccessfulExecution() == null ? null : convertHlrcToInternal(actionStatus.lastSuccessfulExecution()),
            actionStatus.lastThrottle() == null ? null : convertHlrcToInternal(actionStatus.lastThrottle())
        );
    }

    private static ActionStatus.AckStatus convertHlrcToInternal(org.elasticsearch.client.watcher.ActionStatus.AckStatus ackStatus) {
        return new ActionStatus.AckStatus(ackStatus.timestamp(), convertHlrcToInternal(ackStatus.state()));
    }

    private static ActionStatus.AckStatus.State convertHlrcToInternal(org.elasticsearch.client.watcher.ActionStatus.AckStatus.State state) {
        return ActionStatus.AckStatus.State.valueOf(state.name());
    }

    private static WatchStatus.State convertHlrcToInternal(org.elasticsearch.client.watcher.WatchStatus.State state) {
        return new WatchStatus.State(state.isActive(), state.getTimestamp());
    }

    private static ExecutionState convertHlrcToInternal(org.elasticsearch.client.watcher.ExecutionState executionState) {
        return ExecutionState.valueOf(executionState.name());
    }

    private static ActionStatus.Execution convertHlrcToInternal(org.elasticsearch.client.watcher.ActionStatus.Execution execution) {
        if (execution.successful()) {
            return ActionStatus.Execution.successful(execution.timestamp());
        } else {
            return ActionStatus.Execution.failure(execution.timestamp(), execution.reason());
        }
    }

    private static ActionStatus.Throttle convertHlrcToInternal(org.elasticsearch.client.watcher.ActionStatus.Throttle throttle) {
        return new ActionStatus.Throttle(throttle.timestamp(), throttle.reason());
    }
}

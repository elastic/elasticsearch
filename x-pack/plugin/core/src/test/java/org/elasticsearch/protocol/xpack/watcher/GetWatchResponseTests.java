/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class GetWatchResponseTests extends
    AbstractHlrcStreamableXContentTestCase<GetWatchResponse, org.elasticsearch.client.watcher.GetWatchResponse> {

    private static final String[] SHUFFLE_FIELDS_EXCEPTION = new String[] { "watch" };

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return SHUFFLE_FIELDS_EXCEPTION;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap("hide_headers", "false"));
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return f -> f.contains("watch") || f.contains("actions") || f.contains("headers");
    }

    @Override
    protected void assertEqualInstances(GetWatchResponse expectedInstance, GetWatchResponse newInstance) {
        if (expectedInstance.isFound() &&
                expectedInstance.getSource().getContentType() != newInstance.getSource().getContentType()) {
            /**
             * The {@link GetWatchResponse#getContentType()} depends on the content type that
             * was used to serialize the main object so we use the same content type than the
             * <code>expectedInstance</code> to translate the watch of the <code>newInstance</code>.
             */
            XContent from = XContentFactory.xContent(newInstance.getSource().getContentType());
            XContent to = XContentFactory.xContent(expectedInstance.getSource().getContentType());
            final BytesReference newSource;
            // It is safe to use EMPTY here because this never uses namedObject
            try (InputStream stream = newInstance.getSource().getBytes().streamInput();
                 XContentParser parser = XContentFactory.xContent(from.type()).createParser(NamedXContentRegistry.EMPTY,
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
                parser.nextToken();
                XContentBuilder builder = XContentFactory.contentBuilder(to.type());
                builder.copyCurrentStructure(parser);
                newSource = BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            newInstance = new GetWatchResponse(newInstance.getId(), newInstance.getVersion(),
                newInstance.getStatus(), new XContentSource(newSource, expectedInstance.getSource().getContentType()));
        }
        super.assertEqualInstances(expectedInstance, newInstance);
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
        BytesReference source = simpleWatch();
        return new GetWatchResponse(id, version, status, new XContentSource(source, XContentType.JSON));
    }

    private static BytesReference simpleWatch() {
        try {
            XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject()
                .startObject("trigger")
                    .startObject("schedule")
                        .field("interval", "10h")
                    .endObject()
                .endObject()
                .startObject("input")
                    .startObject("none").endObject()
                .endObject()
                .startObject("actions")
                    .startObject("logme")
                        .field("text", "{{ctx.payload}}")
                    .endObject()
                .endObject().endObject();
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
        Map<String, String> headers = new HashMap<>();
        int headerSize = randomIntBetween(0, 5);
        for (int i = 0; i < headerSize; i++) {
            headers.put(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(1, 10));
        }
        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actionMap, headers);
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
            status.lastChecked(), status.lastMetCondition(), actions, status.getHeaders()
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

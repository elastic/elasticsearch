/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionResult;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public abstract class WatchRecord implements ToXContentObject {

    public static final ParseField WATCH_ID = new ParseField("watch_id");
    public static final ParseField STATE = new ParseField("state");
    public static final ParseField TRIGGER_EVENT = new ParseField("trigger_event");
    public static final ParseField NODE = new ParseField("node");
    private static final ParseField MESSAGES = new ParseField("messages");
    private static final ParseField STATUS = new ParseField("status");
    private static final ParseField VARS = new ParseField("vars");
    private static final ParseField METADATA = new ParseField("metadata");
    private static final ParseField EXECUTION_RESULT = new ParseField("result");
    private static final ParseField EXCEPTION = new ParseField("exception");
    private static final ParseField USER = new ParseField("user");
    public static final String TRUNCATED_RECORD_KEY = "truncated";
    public static final String TRUNCATED_RECORD_VALUE = "Watch history record exceeded the value of the "
        + "`xpack.watcher.max.history.record.size' setting";

    protected final Wid id;
    protected final Watch watch;
    private final String nodeId;
    protected final TriggerEvent triggerEvent;
    protected final ExecutionState state;
    protected final String user;

    // only emitted to xcontent in "debug" mode
    protected final Map<String, Object> vars;

    @Nullable
    protected final ExecutableInput<? extends Input, ? extends Input.Result> input;
    @Nullable
    protected final ExecutableCondition condition;
    @Nullable
    protected final Map<String, Object> metadata;
    @Nullable
    protected final WatchExecutionResult executionResult;

    private WatchRecord(
        Wid id,
        TriggerEvent triggerEvent,
        ExecutionState state,
        Map<String, Object> vars,
        ExecutableInput<? extends Input, ? extends Input.Result> input,
        ExecutableCondition condition,
        Map<String, Object> metadata,
        Watch watch,
        WatchExecutionResult executionResult,
        String nodeId,
        String user
    ) {
        this.id = id;
        this.triggerEvent = triggerEvent;
        this.state = state;
        this.vars = vars;
        this.input = input;
        this.condition = condition;
        this.metadata = metadata;
        this.executionResult = executionResult;
        this.watch = watch;
        this.nodeId = nodeId;
        this.user = user;
    }

    private WatchRecord(Wid id, TriggerEvent triggerEvent, ExecutionState state, String nodeId) {
        this(id, triggerEvent, state, Collections.emptyMap(), null, null, null, null, null, nodeId, null);
    }

    private WatchRecord(WatchRecord record, ExecutionState state) {
        this(
            record.id,
            record.triggerEvent,
            state,
            record.vars,
            record.input,
            record.condition,
            record.metadata,
            record.watch,
            record.executionResult,
            record.nodeId,
            record.user
        );
    }

    private WatchRecord(WatchExecutionContext context, ExecutionState state) {
        this(
            context.id(),
            context.triggerEvent(),
            state,
            context.vars(),
            context.watch() != null ? context.watch().input() : null,
            context.watch() != null ? context.watch().condition() : null,
            context.watch() != null ? context.watch().metadata() : null,
            context.watch(),
            null,
            context.getNodeId(),
            context.getUser()
        );
    }

    private WatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult) {
        this(
            context.id(),
            context.triggerEvent(),
            getState(executionResult),
            context.vars(),
            context.watch().input(),
            context.watch().condition(),
            context.watch().metadata(),
            context.watch(),
            executionResult,
            context.getNodeId(),
            context.getUser()
        );
    }

    public static ExecutionState getState(WatchExecutionResult executionResult) {
        if (executionResult == null || executionResult.conditionResult() == null) {
            return ExecutionState.FAILED;
        }
        if (executionResult.conditionResult().met()) {
            final Collection<ActionWrapperResult> values = executionResult.actionsResults().values();
            // acknowledged as state wins because the user had explicitly set this, where as throttled may happen due to execution
            if (values.stream().anyMatch((r) -> r.action().status() == Action.Result.Status.ACKNOWLEDGED)) {
                return ExecutionState.ACKNOWLEDGED;
            } else if (values.stream().anyMatch((r) -> r.action().status() == Action.Result.Status.THROTTLED)) {
                return ExecutionState.THROTTLED;
            } else {
                return ExecutionState.EXECUTED;
            }
        } else {
            return ExecutionState.EXECUTION_NOT_NEEDED;
        }
    }

    public Wid id() {
        return id;
    }

    public TriggerEvent triggerEvent() {
        return triggerEvent;
    }

    public String watchId() {
        return id.watchId();
    }

    public ExecutableInput<? extends Input, ? extends Input.Result> input() {
        return input;
    }

    public ExecutionState state() {
        return state;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public WatchExecutionResult result() {
        return executionResult;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(WATCH_ID.getPreferredName(), id.watchId());
        builder.field(NODE.getPreferredName(), nodeId);
        builder.field(STATE.getPreferredName(), state.id());
        builder.field("@timestamp", triggerEvent.triggeredTime());

        if (user != null) {
            builder.field(USER.getPreferredName(), user);
        }
        if (watch != null && watch.status() != null) {
            builder.field(STATUS.getPreferredName(), watch.status(), params);
        }

        builder.field(TRIGGER_EVENT.getPreferredName());
        triggerEvent.recordXContent(builder, params);

        if (vars.isEmpty() == false && WatcherParams.debug(params)) {
            builder.field(VARS.getPreferredName(), vars);
        }

        if (input != null) {
            builder.startObject(WatchField.INPUT.getPreferredName()).field(input.type(), input, params).endObject();
        }
        if (condition != null) {
            builder.startObject(WatchField.CONDITION.getPreferredName()).field(condition.type(), condition, params).endObject();
        }
        if (metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        if (executionResult != null) {
            builder.field(EXECUTION_RESULT.getPreferredName(), executionResult, params);
        }
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    abstract void innerToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatchRecord entry = (WatchRecord) o;
        return Objects.equals(id, entry.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id.toString();
    }

    public abstract WatchRecord dropLargeFields() throws Exception;

    public static class MessageWatchRecord extends WatchRecord {
        @Nullable
        private final String[] messages;

        /**
         * Called when the execution was aborted before it started
         */
        public MessageWatchRecord(Wid id, TriggerEvent triggerEvent, ExecutionState state, String message, String nodeId) {
            super(id, triggerEvent, state, nodeId);
            this.messages = new String[] { message };
        }

        /**
         * Called when the execution was aborted due to an error during execution (the given result should reflect
         * were exactly the execution failed)
         */
        public MessageWatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult, String message) {
            super(context, executionResult);
            this.messages = new String[] { message };
        }

        /**
         * Called when the execution finished.
         */
        public MessageWatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult) {
            super(context, executionResult);
            this.messages = Strings.EMPTY_ARRAY;
        }

        public MessageWatchRecord(WatchRecord record, ExecutionState state, String message) {
            super(record, state);
            if (record instanceof MessageWatchRecord messageWatchRecord) {
                if (messageWatchRecord.messages.length == 0) {
                    this.messages = new String[] { message };
                } else {
                    String[] newMessages = new String[messageWatchRecord.messages.length + 1];
                    System.arraycopy(messageWatchRecord.messages, 0, newMessages, 0, messageWatchRecord.messages.length);
                    newMessages[messageWatchRecord.messages.length] = message;
                    this.messages = newMessages;
                }
            } else {
                messages = new String[] { message };
            }
        }

        private MessageWatchRecord(
            Wid id,
            TriggerEvent triggerEvent,
            ExecutionState state,
            Map<String, Object> vars,
            ExecutableInput<? extends Input, ? extends Input.Result> redactedInput,
            ExecutableCondition condition,
            Map<String, Object> metadata,
            Watch watch,
            WatchExecutionResult redactedResult,
            String nodeId,
            String user,
            String[] messages
        ) {
            super(id, triggerEvent, state, vars, redactedInput, condition, metadata, watch, redactedResult, nodeId, user);
            this.messages = messages;
        }

        public String[] messages() {
            return messages;
        }

        @Override
        void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            if (messages != null) {
                builder.array(MESSAGES.getPreferredName(), messages);
            }
        }

        @Override
        public WatchRecord dropLargeFields() throws Exception {
            return new MessageWatchRecord(
                this.id,
                this.triggerEvent,
                this.state,
                this.vars,
                this.input == null ? null : getTruncatedInput(),
                this.condition,
                this.metadata,
                this.watch,
                this.executionResult == null ? null : getTruncatedWatchExecutionResult(this),
                this.getNodeId(),
                this.user,
                this.messages
            );
        }
    }

    public static class ExceptionWatchRecord extends WatchRecord {

        private ExceptionWatchRecord(
            Wid id,
            TriggerEvent triggerEvent,
            ExecutionState state,
            Map<String, Object> vars,
            ExecutableInput<? extends Input, ? extends Input.Result> redactedInput,
            ExecutableCondition condition,
            Map<String, Object> metadata,
            Watch watch,
            WatchExecutionResult redactedResult,
            String nodeId,
            String user,
            Exception exception
        ) {
            super(id, triggerEvent, state, vars, redactedInput, condition, metadata, watch, redactedResult, nodeId, user);
            this.exception = exception;
        }

        private static final Map<String, String> STACK_TRACE_ENABLED_PARAMS = Map.of(
            ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE,
            "false"
        );

        @Nullable
        private final Exception exception;

        public ExceptionWatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult, Exception exception) {
            super(context, executionResult);
            this.exception = exception;
        }

        public ExceptionWatchRecord(WatchRecord record, Exception exception) {
            super(record, ExecutionState.FAILED);
            this.exception = exception;
        }

        public ExceptionWatchRecord(WatchExecutionContext context, Exception exception) {
            super(context, ExecutionState.FAILED);
            this.exception = exception;
        }

        public Exception getException() {
            return exception;
        }

        @Override
        void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            if (exception != null) {
                if (exception instanceof ElasticsearchException elasticsearchException) {
                    builder.startObject(EXCEPTION.getPreferredName());
                    Params delegatingParams = new DelegatingMapParams(STACK_TRACE_ENABLED_PARAMS, params);
                    elasticsearchException.toXContent(builder, delegatingParams);
                    builder.endObject();
                } else {
                    builder.startObject(EXCEPTION.getPreferredName())
                        .field("type", ElasticsearchException.getExceptionName(exception))
                        .field("reason", exception.getMessage())
                        .endObject();
                }
            }
        }

        @Override
        public WatchRecord dropLargeFields() throws Exception {
            return new ExceptionWatchRecord(
                this.id,
                triggerEvent,
                this.state,
                this.vars,
                this.input == null ? null : getTruncatedInput(),
                this.condition,
                this.metadata,
                this.watch,
                this.executionResult == null ? null : getTruncatedWatchExecutionResult(this),
                this.getNodeId(),
                this.user,
                this.exception
            );
        }
    }

    /*
     * This returns a ExecutableInput whose toXContent() returns no information other than a new TRUNCATED_MESSAGE field. It
     * drops other information to avoid having a document that is too large to index into Elasticsearch.
     */
    private static ExecutableInput<? extends Input, ? extends Input.Result> getTruncatedInput() {
        return new ExecutableInput<>(new Input() {
            @Override
            public String type() {
                return TRUNCATED_RECORD_KEY;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.value(TRUNCATED_RECORD_VALUE);
                return builder;
            }
        }) {
            @Override
            public Input.Result execute(WatchExecutionContext ctx, Payload payload) {
                throw new UnsupportedOperationException("Redacted input cannot be executed");
            }
        };
    }

    /*
     * This returns a WatchExecutionResult whose toXContent() returns minimal information, including a new TRUNCATED_MESSAGE field. It
     * drops most other information to avoid having a document that is too large to index into Elasticsearch.
     */
    private static WatchExecutionResult getTruncatedWatchExecutionResult(WatchRecord watchRecord) {
        WatchExecutionContext watchExecutionContext = new WatchExecutionContext(
            watchRecord.id.watchId(),
            watchRecord.executionResult.executionTime(),
            null,
            TimeValue.ZERO
        ) {
            @Override
            public boolean knownWatch() {
                return false;
            }

            @Override
            public boolean simulateAction(String actionId) {
                return false;
            }

            @Override
            public boolean skipThrottling(String actionId) {
                return false;
            }

            @Override
            public boolean shouldBeExecuted() {
                return false;
            }

            @Override
            public boolean recordExecution() {
                return false;
            }
        };

        return new WatchExecutionResult(watchExecutionContext, watchRecord.executionResult.executionDurationMs()) {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                WatcherDateTimeUtils.writeDate(
                    Field.EXECUTION_TIME.getPreferredName(),
                    builder,
                    watchRecord.executionResult.executionTime()
                );
                builder.field(Field.EXECUTION_DURATION.getPreferredName(), watchRecord.executionResult.executionDurationMs());
                builder.field(TRUNCATED_RECORD_KEY, TRUNCATED_RECORD_VALUE);
                builder.endObject();
                return builder;
            }
        };
    }
}

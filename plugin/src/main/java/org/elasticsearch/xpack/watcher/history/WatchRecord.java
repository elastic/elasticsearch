/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.watcher.actions.Action;
import org.elasticsearch.xpack.watcher.actions.ActionWrapper;
import org.elasticsearch.xpack.watcher.condition.Condition;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionResult;
import org.elasticsearch.xpack.watcher.execution.Wid;
import org.elasticsearch.xpack.watcher.input.ExecutableInput;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public abstract class WatchRecord implements ToXContentObject {

    protected final Wid id;
    protected final Watch watch;
    protected final TriggerEvent triggerEvent;
    protected final ExecutionState state;

    // only emitted to xcontent in "debug" mode
    protected final Map<String, Object> vars;

    @Nullable protected final ExecutableInput input;
    @Nullable protected final Condition condition;
    @Nullable protected final Map<String,Object> metadata;
    @Nullable protected final WatchExecutionResult executionResult;

    private WatchRecord(Wid id, TriggerEvent triggerEvent, ExecutionState state, Map<String, Object> vars, ExecutableInput input,
                       Condition condition, Map<String, Object> metadata, Watch watch, WatchExecutionResult executionResult) {
        this.id = id;
        this.triggerEvent = triggerEvent;
        this.state = state;
        this.vars = vars;
        this.input = input;
        this.condition = condition;
        this.metadata = metadata;
        this.executionResult = executionResult;
        this.watch = watch;
    }

    private WatchRecord(Wid id, TriggerEvent triggerEvent, ExecutionState state) {
        this(id, triggerEvent, state, Collections.emptyMap(), null, null, null, null, null);
    }

    private WatchRecord(WatchRecord record, ExecutionState state) {
        this(record.id, record.triggerEvent, state, record.vars, record.input, record.condition, record.metadata, record.watch,
                record.executionResult);
    }

    private WatchRecord(WatchExecutionContext context, ExecutionState state) {
        this(context.id(), context.triggerEvent(), state, context.vars(), context.watch().input(), context.watch().condition(),
                context.watch().metadata(), context.watch(), null);
    }

    private WatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult) {
        this(context.id(), context.triggerEvent(), getState(executionResult), context.vars(), context.watch().input(),
                context.watch().condition(), context.watch().metadata(), context.watch(), executionResult);
    }

    private static ExecutionState getState(WatchExecutionResult executionResult) {
        if (executionResult == null || executionResult.conditionResult() == null) {
            return ExecutionState.FAILED;
        }
        if (executionResult.conditionResult().met()) {
            final Collection<ActionWrapper.Result> values = executionResult.actionsResults().values();
            // acknowledged as state wins because the user had explicitely set this, where as throttled may happen due to execution
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

    public ExecutableInput input() { return input; }

    public ExecutionState state() {
        return state;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public WatchExecutionResult result() {
        return executionResult;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.WATCH_ID.getPreferredName(), id.watchId());
        builder.field(Field.STATE.getPreferredName(), state.id());

        if (watch != null && watch.status() != null) {
            builder.field("_status", watch.status(), params);
        }

        builder.field(Field.TRIGGER_EVENT.getPreferredName());
        triggerEvent.recordXContent(builder, params);

        if (!vars.isEmpty() && WatcherParams.debug(params)) {
            builder.field(Field.VARS.getPreferredName(), vars);
        }

        if (input != null) {
            builder.startObject(Watch.Field.INPUT.getPreferredName())
                    .field(input.type(), input, params)
                    .endObject();
        }
        if (condition != null) {
            builder.startObject(Watch.Field.CONDITION.getPreferredName())
                    .field(condition.type(), condition, params)
                    .endObject();
        }
        if (metadata != null) {
            builder.field(Field.METADATA.getPreferredName(), metadata);
        }
        if (executionResult != null) {
            builder.field(Field.EXECUTION_RESULT.getPreferredName(), executionResult, params);
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

    public interface Field {
        ParseField WATCH_ID = new ParseField("watch_id");
        ParseField TRIGGER_EVENT = new ParseField("trigger_event");
        ParseField MESSAGES = new ParseField("messages");
        ParseField STATE = new ParseField("state");
        ParseField STATUS = new ParseField("_status");
        ParseField VARS = new ParseField("vars");
        ParseField METADATA = new ParseField("metadata");
        ParseField EXECUTION_RESULT = new ParseField("result");
        ParseField EXCEPTION = new ParseField("exception");
    }

    public static class MessageWatchRecord extends WatchRecord {
        @Nullable private final String[] messages;

        /**
         * Called when the execution was aborted before it started
         */
        public MessageWatchRecord(Wid id, TriggerEvent triggerEvent, ExecutionState state, String message) {
            super(id, triggerEvent, state);
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
            if (record instanceof MessageWatchRecord) {
                MessageWatchRecord messageWatchRecord = (MessageWatchRecord) record;
                if (messageWatchRecord.messages.length == 0) {
                    this.messages = new String[] { message };
                } else {
                    String[] newMessages = new String[messageWatchRecord.messages.length + 1];
                    System.arraycopy(messageWatchRecord.messages, 0, newMessages, 0, messageWatchRecord.messages.length);
                    newMessages[messageWatchRecord.messages.length] = message;
                    this.messages = newMessages;
                }
            } else {
                messages = new String []{ message };
            }
        }

        public String[] messages(){
            return messages;
        }

        @Override
        void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            if (messages != null) {
                builder.array(Field.MESSAGES.getPreferredName(), messages);
            }
        }
    }

    public static class ExceptionWatchRecord extends WatchRecord {

        private static final Map<String, String> STACK_TRACE_ENABLED_PARAMS = MapBuilder.<String, String>newMapBuilder()
                .put(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false")
                .immutableMap();

        @Nullable private final Exception exception;

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
                if (exception instanceof ElasticsearchException) {
                    ElasticsearchException elasticsearchException = (ElasticsearchException) exception;
                    builder.startObject(Field.EXCEPTION.getPreferredName());
                    Params delegatingParams = new DelegatingMapParams(STACK_TRACE_ENABLED_PARAMS, params);
                    elasticsearchException.toXContent(builder, delegatingParams);
                    builder.endObject();
                } else {
                    builder.startObject(Field.EXCEPTION.getPreferredName())
                            .field("type", ElasticsearchException.getExceptionName(exception))
                            .field("reason", exception.getMessage())
                            .endObject();
                }
            }
        }
    }
}

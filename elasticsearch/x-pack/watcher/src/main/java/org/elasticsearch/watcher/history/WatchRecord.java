/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.execution.ExecutionState;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.WatchExecutionResult;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.watch.Watch;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class WatchRecord implements ToXContent {

    private final Wid id;
    private final TriggerEvent triggerEvent;
    private final ExecutionState state;

    // only emitted to xcontent in "debug" mode
    private final Map<String, Object> vars;

    private final @Nullable ExecutableInput input;
    private final @Nullable Condition condition;
    private final @Nullable Map<String,Object> metadata;

    private final @Nullable String[] messages;
    private final @Nullable WatchExecutionResult executionResult;

    /**
     * Called when the execution was aborted before it started
     */
    public WatchRecord(Wid id, TriggerEvent triggerEvent, ExecutionState state, String message) {
        this.id = id;
        this.triggerEvent = triggerEvent;
        this.state = state;
        this.messages = new String[] { message };
        this.vars = Collections.emptyMap();
        this.executionResult = null;
        this.condition = null;
        this.input = null;
        this.metadata = null;
    }

    /**
     * Called when the execution was aborted due to an error during execution (the given result should reflect
     * were exactly the execution failed)
     */
    public WatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult, String message) {
        this.id = context.id();
        this.triggerEvent = context.triggerEvent();
        this.state = ExecutionState.FAILED;
        this.messages = new String[] { message };
        this.vars = context.vars();
        this.executionResult = executionResult;
        this.condition = context.watch().condition().condition();
        this.input = context.watch().input();
        this.metadata = context.watch().metadata();
    }

    /**
     * Called when the execution finished.
     */
    public WatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult) {
        this.id = context.id();
        this.triggerEvent = context.triggerEvent();
        this.messages = Strings.EMPTY_ARRAY;
        this.vars = context.vars();
        this.executionResult = executionResult;
        this.condition = context.watch().condition().condition();
        this.input = context.watch().input();
        this.metadata = context.watch().metadata();

        if (!this.executionResult.conditionResult().met()) {
            state = ExecutionState.EXECUTION_NOT_NEEDED;
        } else {
            if (this.executionResult.actionsResults().throttled()) {
                state = ExecutionState.THROTTLED;
            } else {
                state = ExecutionState.EXECUTED;
            }
        }
    }

    public WatchRecord(WatchRecord record, ExecutionState state, String message) {
        this.id = record.id;
        this.triggerEvent = record.triggerEvent;
        this.vars = record.vars;
        this.executionResult = record.executionResult;
        this.condition = record.condition;
        this.input = record.input;
        this.metadata = record.metadata;
        this.state = state;

        if (record.messages.length == 0) {
            this.messages = new String[] { message };
        } else {
            String[] newMessages = new String[record.messages.length + 1];
            System.arraycopy(record.messages, 0, newMessages, 0, record.messages.length);
            newMessages[record.messages.length] = message;
            this.messages = newMessages;
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

    public Condition condition() {
        return condition;
    }

    public ExecutionState state() {
        return state;
    }

    public String[] messages(){
        return messages;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public WatchExecutionResult result() {
        return executionResult;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.WATCH_ID.getPreferredName(), id.watchId());
        builder.field(Field.STATE.getPreferredName(), state.id());

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
        if (messages != null) {
            builder.field(Field.MESSAGES.getPreferredName(), messages);
        }
        if (metadata != null) {
            builder.field(Field.METADATA.getPreferredName(), metadata);
        }
        if (executionResult != null) {
            builder.field(Field.EXECUTION_RESULT.getPreferredName(), executionResult, params);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatchRecord entry = (WatchRecord) o;
        if (!id.equals(entry.id)) return false;

        return true;
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
        ParseField VARS = new ParseField("vars");
        ParseField METADATA = new ParseField("metadata");
        ParseField EXECUTION_RESULT = new ParseField("result");
    }
}

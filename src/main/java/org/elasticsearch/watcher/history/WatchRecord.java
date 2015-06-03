/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.execution.*;
import org.elasticsearch.watcher.input.ExecutableInput;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.watch.Watch;

import java.io.IOException;
import java.util.Map;

public class WatchRecord implements ToXContent {

    private final Wid id;
    private final TriggerEvent triggerEvent;
    private final ExecutionState state;

    private final @Nullable ExecutableInput input;
    private final @Nullable Condition condition;
    private final @Nullable Map<String,Object> metadata;

    private final @Nullable String message;
    private final @Nullable WatchExecutionResult executionResult;

    public WatchRecord(Wid id, TriggerEvent triggerEvent, String message, ExecutionState state) {
        this.id = id;
        this.triggerEvent = triggerEvent;
        this.executionResult = null;
        this.state = state;
        this.message = message;
        this.condition = null;
        this.input = null;
        this.metadata = null;
    }

    public WatchRecord(WatchExecutionContext context, WatchExecutionResult executionResult) {
        this.id = context.id();
        this.triggerEvent = context.triggerEvent();
        this.condition = context.watch().condition().condition();
        this.input = context.watch().input();
        this.executionResult = executionResult;
        this.metadata = context.watch().metadata();
        this.message = null;

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

    public String message(){
        return this.message;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public WatchExecutionResult execution() {
        return executionResult;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.WATCH_ID.getPreferredName(), id.watchId());
        builder.field(Field.STATE.getPreferredName(), state.id());

        builder.field(Field.TRIGGER_EVENT.getPreferredName());
        triggerEvent.recordXContent(builder, params);

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

        if (message != null) {
            builder.field(Field.MESSAGE.getPreferredName(), message);
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
        ParseField MESSAGE = new ParseField("message");
        ParseField STATE = new ParseField("state");
        ParseField METADATA = new ParseField("metadata");
        ParseField EXECUTION_RESULT = new ParseField("result");
    }
}

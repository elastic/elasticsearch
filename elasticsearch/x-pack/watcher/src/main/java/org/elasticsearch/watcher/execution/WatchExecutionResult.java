/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.transform.Transform;
import org.joda.time.DateTime;

import java.io.IOException;

/**
*
*/
public class WatchExecutionResult implements ToXContent {

    private final DateTime executionTime;
    private final long executionDurationMs;
    private final @Nullable Input.Result inputResult;
    private final @Nullable Condition.Result conditionResult;
    private final @Nullable Transform.Result transformResult;
    private final ExecutableActions.Results actionsResults;

    public WatchExecutionResult(WatchExecutionContext context, long executionDurationMs) {
        this(context.executionTime(), executionDurationMs, context.inputResult(), context.conditionResult(), context.transformResult(),
                context.actionsResults());
    }

    WatchExecutionResult(DateTime executionTime, long executionDurationMs, Input.Result inputResult, Condition.Result conditionResult,
                         @Nullable Transform.Result transformResult, ExecutableActions.Results actionsResults) {
        this.executionTime = executionTime;
        this.inputResult = inputResult;
        this.conditionResult = conditionResult;
        this.transformResult = transformResult;
        this.actionsResults = actionsResults;
        this.executionDurationMs = executionDurationMs;
    }

    public DateTime executionTime() {
        return executionTime;
    }

    public long executionDurationMs() {
        return executionDurationMs;
    }

    public Input.Result inputResult() {
        return inputResult;
    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public Transform.Result transformResult() {
        return transformResult;
    }

    public ExecutableActions.Results actionsResults() {
        return actionsResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        WatcherDateTimeUtils.writeDate(Field.EXECUTION_TIME.getPreferredName(), builder, executionTime);
        builder.field(Field.EXECUTION_DURATION.getPreferredName(), executionDurationMs);

        if (inputResult != null) {
            builder.field(Field.INPUT.getPreferredName(), inputResult, params);
        }
        if (conditionResult != null) {
            builder.field(Field.CONDITION.getPreferredName(), conditionResult, params);
        }
        if (transformResult != null) {
            builder.field(Transform.Field.TRANSFORM.getPreferredName(), transformResult, params);
        }
        builder.field(Field.ACTIONS.getPreferredName(), actionsResults, params);
        builder.endObject();
        return builder;
    }

    public interface Field {
        ParseField EXECUTION_TIME = new ParseField("execution_time");
        ParseField EXECUTION_DURATION = new ParseField("execution_duration");
        ParseField INPUT = new ParseField("input");
        ParseField CONDITION = new ParseField("condition");
        ParseField ACTIONS = new ParseField("actions");

        ParseField TYPE = new ParseField("type");
    }
}

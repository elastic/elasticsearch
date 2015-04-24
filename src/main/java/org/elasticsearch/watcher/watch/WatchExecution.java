/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.actions.ActionRegistry;
import org.elasticsearch.watcher.actions.ActionWrapper;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ConditionRegistry;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.throttle.Throttler;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;

import java.io.IOException;

/**
*
*/
public class WatchExecution implements ToXContent {

    private final Input.Result inputResult;
    private final Condition.Result conditionResult;
    private final Throttler.Result throttleResult;
    private final @Nullable Transform.Result transformResult;
    private final ExecutableActions.Results actionsResults;

    public WatchExecution(WatchExecutionContext context) {
        this(context.inputResult(), context.conditionResult(), context.throttleResult(), context.transformResult(), context.actionsResults());
    }

    WatchExecution(Input.Result inputResult, Condition.Result conditionResult, Throttler.Result throttleResult, @Nullable Transform.Result transformResult, ExecutableActions.Results actionsResults) {
        this.inputResult = inputResult;
        this.conditionResult = conditionResult;
        this.throttleResult = throttleResult;
        this.transformResult = transformResult;
        this.actionsResults = actionsResults;
    }

    public Input.Result inputResult() {
        return inputResult;
    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public Throttler.Result throttleResult() {
        return throttleResult;
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
        if (inputResult != null) {
            builder.startObject(Parser.INPUT_RESULT_FIELD.getPreferredName())
                    .field(inputResult.type(), inputResult, params)
                    .endObject();
        }
        if (conditionResult != null) {
            builder.startObject(Parser.CONDITION_RESULT_FIELD.getPreferredName())
                    .field(conditionResult.type(), conditionResult, params)
                    .endObject();
        }
        if (throttleResult != null && throttleResult.throttle()) {
            builder.field(Parser.THROTTLED.getPreferredName(), throttleResult.throttle());
            if (throttleResult.reason() != null) {
                builder.field(Parser.THROTTLE_REASON.getPreferredName(), throttleResult.reason());
            }
        }
        if (transformResult != null) {
            builder.startObject(Transform.Field.TRANSFORM_RESULT.getPreferredName())
                    .field(transformResult.type(), transformResult, params)
                    .endObject();
        }
        builder.startObject(Parser.ACTIONS_RESULTS.getPreferredName());
        for (ActionWrapper.Result actionResult : actionsResults) {
            builder.field(actionResult.id(), actionResult, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static class Parser {

        public static final ParseField INPUT_RESULT_FIELD = new ParseField("input_result");
        public static final ParseField CONDITION_RESULT_FIELD = new ParseField("condition_result");
        public static final ParseField ACTIONS_RESULTS = new ParseField("actions_results");
        public static final ParseField THROTTLED = new ParseField("throttled");
        public static final ParseField THROTTLE_REASON = new ParseField("throttle_reason");

        public static WatchExecution parse(Wid wid, XContentParser parser, ConditionRegistry conditionRegistry, ActionRegistry actionRegistry,
                                           InputRegistry inputRegistry, TransformRegistry transformRegistry) throws IOException {
            boolean throttled = false;
            String throttleReason = null;
            ExecutableActions.Results actionResults = null;
            Input.Result inputResult = null;
            Condition.Result conditionResult = null;
            Transform.Result transformResult = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (THROTTLE_REASON.match(currentFieldName)) {
                        throttleReason = parser.text();
                    } else if (THROTTLED.match(currentFieldName)) {
                        throttled = parser.booleanValue();
                    } else {
                        throw new WatcherException("unable to parse watch execution. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (INPUT_RESULT_FIELD.match(currentFieldName)) {
                        inputResult = inputRegistry.parseResult(wid.watchId(), parser);
                    } else if (CONDITION_RESULT_FIELD.match(currentFieldName)) {
                        conditionResult = conditionRegistry.parseResult(wid.watchId(), parser);
                    } else if (Transform.Field.TRANSFORM_RESULT.match(currentFieldName)) {
                        transformResult = transformRegistry.parseResult(wid.watchId(), parser);
                    } else if (ACTIONS_RESULTS.match(currentFieldName)) {
                        actionResults = actionRegistry.parseResults(wid, parser);
                    } else {
                        throw new WatcherException("unable to parse watch execution. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new WatcherException("unable to parse watch execution. unexpected token [" + token + "]");
                }
            }

            Throttler.Result throttleResult = throttled ? Throttler.Result.throttle(throttleReason) : Throttler.Result.NO;
            return new WatchExecution(inputResult, conditionResult, throttleResult, transformResult, actionResults);

        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.ActionRegistry;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionRegistry;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.InputRegistry;
import org.elasticsearch.alerts.throttle.Throttler;
import org.elasticsearch.alerts.transform.Transform;
import org.elasticsearch.alerts.transform.TransformRegistry;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
*
*/
public class AlertExecution implements ToXContent {

    private final Input.Result inputResult;
    private final Condition.Result conditionResult;
    private final Throttler.Result throttleResult;
    private final @Nullable Transform.Result transformResult;
    private final Map<String, Action.Result> actionsResults;

    public AlertExecution(ExecutionContext context) {
        this(context.inputResult(), context.conditionResult(), context.throttleResult(), context.transformResult(), context.actionsResults());
    }

    AlertExecution(Input.Result inputResult, Condition.Result conditionResult, Throttler.Result throttleResult, @Nullable Transform.Result transformResult, Map<String, Action.Result> actionsResults) {
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

    public Map<String, Action.Result> actionsResults() {
        return actionsResults;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (inputResult != null) {
            builder.startObject(Parser.INPUT_RESULT_FIELD.getPreferredName()).field(inputResult.type(), inputResult).endObject();
        }
        if (conditionResult != null) {
            builder.startObject(Parser.CONDITION_RESULT_FIELD.getPreferredName()).field(conditionResult.type(), conditionResult).endObject();
        }
        if (throttleResult != null && throttleResult.throttle()) {
            builder.field(Parser.THROTTLED.getPreferredName(), throttleResult.throttle());
            if (throttleResult.reason() != null) {
                builder.field(Parser.THROTTLE_REASON.getPreferredName(), throttleResult.reason());
            }
        }
        if (transformResult != null) {
            builder.startObject(Transform.Parser.TRANSFORM_RESULT_FIELD.getPreferredName()).field(transformResult.type(), transformResult).endObject();
        }
        builder.startArray(Parser.ACTIONS_RESULTS.getPreferredName());
        for (Map.Entry<String, Action.Result> actionResult : actionsResults.entrySet()) {
            builder.startObject();
            builder.field(actionResult.getKey(), actionResult.getValue());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static class Parser {

        public static final ParseField INPUT_RESULT_FIELD = new ParseField("input_result");
        public static final ParseField CONDITION_RESULT_FIELD = new ParseField("condition_result");
        public static final ParseField ACTIONS_RESULTS = new ParseField("actions_results");
        public static final ParseField THROTTLED = new ParseField("throttled");
        public static final ParseField THROTTLE_REASON = new ParseField("throttle_reason");

        public static AlertExecution parse(XContentParser parser, ConditionRegistry conditionRegistry, ActionRegistry actionRegistry,
                                           InputRegistry inputRegistry, TransformRegistry transformRegistry) throws IOException {
            boolean throttled = false;
            String throttleReason = null;
            Map<String, Action.Result> actionResults = new HashMap<>();
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
                        throw new AlertsException("unable to parse alert run. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (INPUT_RESULT_FIELD.match(currentFieldName)) {
                        inputResult = inputRegistry.parseResult(parser);
                    } else if (CONDITION_RESULT_FIELD.match(currentFieldName)) {
                        conditionResult = conditionRegistry.parseResult(parser);
                    } else if (Transform.Parser.TRANSFORM_RESULT_FIELD.match(currentFieldName)) {
                        transformResult = transformRegistry.parseResult(parser);
                    } else {
                        throw new AlertsException("unable to parse alert run. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (ACTIONS_RESULTS.match(currentFieldName)) {
                        actionResults = parseActionResults(parser, actionRegistry);
                    } else {
                        throw new AlertsException("unable to parse alert run. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new AlertsException("unable to parse alert run. unexpected token [" + token + "]");
                }
            }

            Throttler.Result throttleResult = throttled ? Throttler.Result.throttle(throttleReason) : Throttler.Result.NO;
            return new AlertExecution(inputResult, conditionResult, throttleResult, transformResult, actionResults);

        }

        private static Map<String, Action.Result> parseActionResults(XContentParser parser, ActionRegistry actionRegistry) throws IOException {
            Map<String, Action.Result> actionResults = new HashMap<>();
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                Action.Result actionResult = actionRegistry.parseResult(parser);
                actionResults.put(actionResult.type(), actionResult);
            }
            return actionResults;
        }
    }
}

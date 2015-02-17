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
import org.elasticsearch.alerts.throttle.Throttler;
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

    private final Condition.Result conditionResult;
    private final Throttler.Result throttleResult;
    private final Map<String, Action.Result> actionsResults;
    private final Payload payload;

    public AlertExecution(ExecutionContext context) {
        this(context.conditionResult(), context.throttleResult(), context.actionsResults(), context.payload());
    }

    AlertExecution(Condition.Result conditionResult, Throttler.Result throttleResult, Map<String, Action.Result> actionsResults, Payload payload) {
        this.conditionResult = conditionResult;
        this.throttleResult = throttleResult;
        this.actionsResults = actionsResults;
        this.payload = payload;
    }

    public Condition.Result conditionResult() {
        return conditionResult;
    }

    public Throttler.Result throttleResult() {
        return throttleResult;
    }

    public Map<String, Action.Result> actionsResults() {
        return actionsResults;
    }

    public Payload payload() {
        return payload;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (conditionResult != null) {
            builder.startObject(Parser.CONDITION_RESULT.getPreferredName()).field(conditionResult.type(), conditionResult).endObject();
        }
        if (throttleResult != null && throttleResult.throttle()) {
            builder.field(Parser.THROTTLED.getPreferredName(), throttleResult.throttle());
            if (throttleResult.reason() != null) {
                builder.field(Parser.THROTTLE_REASON.getPreferredName(), throttleResult.reason());
            }
        }
        builder.field(Parser.PAYLOAD.getPreferredName(), payload());
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

        public static final ParseField CONDITION_RESULT = new ParseField("condition_result");
        public static final ParseField PAYLOAD = new ParseField("payload");
        public static final ParseField ACTIONS_RESULTS = new ParseField("actions_results");
        public static final ParseField THROTTLED = new ParseField("throttled");
        public static final ParseField THROTTLE_REASON = new ParseField("throttle_reason");

        public static AlertExecution parse(XContentParser parser, ConditionRegistry conditionRegistry, ActionRegistry actionRegistry) throws IOException {
            boolean throttled = false;
            String throttleReason = null;
            Map<String, Action.Result> actionResults = new HashMap<>();
            Condition.Result conditionResult = null;
            Payload payload = null;

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
                    if (CONDITION_RESULT.match(currentFieldName)) {
                        conditionResult = conditionRegistry.parseResult(parser);
                    } else if (PAYLOAD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
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
            return new AlertExecution(conditionResult, throttleResult, actionResults, payload );

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

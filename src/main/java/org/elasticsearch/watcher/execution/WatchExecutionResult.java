/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.actions.ActionRegistry;
import org.elasticsearch.watcher.actions.ExecutableActions;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ConditionRegistry;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.InputRegistry;
import org.elasticsearch.watcher.support.WatcherDateUtils;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.transform.TransformRegistry;

import java.io.IOException;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;

/**
*
*/
public class WatchExecutionResult implements ToXContent {

    private final DateTime executionTime;
    private final Input.Result inputResult;
    private final Condition.Result conditionResult;
    private final @Nullable Transform.Result transformResult;
    private final ExecutableActions.Results actionsResults;

    public WatchExecutionResult(WatchExecutionContext context) {
        this(context.executionTime(), context.inputResult(), context.conditionResult(), context.transformResult(), context.actionsResults());
    }

    WatchExecutionResult(DateTime executionTime, Input.Result inputResult, Condition.Result conditionResult, @Nullable Transform.Result transformResult, ExecutableActions.Results actionsResults) {
        this.executionTime = executionTime;
        this.inputResult = inputResult;
        this.conditionResult = conditionResult;
        this.transformResult = transformResult;
        this.actionsResults = actionsResults;
    }

    public DateTime executionTime() {
        return executionTime;
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
        WatcherDateUtils.writeDate(Field.EXECUTION_TIME.getPreferredName(), builder, executionTime);
        if (inputResult != null) {
            builder.startObject(Field.INPUT.getPreferredName())
                    .field(inputResult.type(), inputResult, params)
                    .endObject();
        }
        if (conditionResult != null) {
            builder.field(Field.CONDITION.getPreferredName());
            ConditionRegistry.writeResult(conditionResult, builder, params);
        }
        if (transformResult != null) {
            builder.startObject(Transform.Field.TRANSFORM.getPreferredName())
                    .field(transformResult.type(), transformResult, params)
                    .endObject();
        }
        builder.field(Field.ACTIONS.getPreferredName(), actionsResults, params);
        builder.endObject();
        return builder;
    }

    public static class Parser {

        public static WatchExecutionResult parse(Wid wid, XContentParser parser, ConditionRegistry conditionRegistry, ActionRegistry actionRegistry,
                                           InputRegistry inputRegistry, TransformRegistry transformRegistry) throws IOException {
            DateTime executionTime = null;
            ExecutableActions.Results actionResults = null;
            Input.Result inputResult = null;
            Condition.Result conditionResult = null;
            Transform.Result transformResult = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.EXECUTION_TIME.match(currentFieldName)) {
                    try {
                        executionTime = WatcherDateUtils.parseDate(currentFieldName, parser, UTC);
                    } catch (WatcherDateUtils.ParseException pe) {
                        throw new WatcherException("could not parse watch execution [{}]. failed to parse date field [{}]", pe, wid, currentFieldName);
                    }
                } else if (Field.ACTIONS.match(currentFieldName)) {
                    actionResults = actionRegistry.parseResults(wid, parser);
                } else if (Field.INPUT.match(currentFieldName)) {
                    inputResult = inputRegistry.parseResult(wid.watchId(), parser);
                } else if (Field.CONDITION.match(currentFieldName)) {
                    conditionResult = conditionRegistry.parseResult(wid.watchId(), parser);
                } else if (Transform.Field.TRANSFORM.match(currentFieldName)) {
                    transformResult = transformRegistry.parseResult(wid.watchId(), parser);
                } else {
                    throw new WatcherException("could not parse watch execution [{}]. unexpected field [{}]", wid, currentFieldName);
                }
            }
            if (executionTime == null) {
                throw new WatcherException("could not parse watch execution [{}]. missing required date field [{}]", wid, Field.EXECUTION_TIME.getPreferredName());
            }
            return new WatchExecutionResult(executionTime, inputResult, conditionResult, transformResult, actionResults);
        }
    }

    interface Field {
        ParseField EXECUTION_TIME = new ParseField("execution_time");
        ParseField INPUT = new ParseField("input");
        ParseField CONDITION = new ParseField("condition");
        ParseField ACTIONS = new ParseField("actions");
    }
}

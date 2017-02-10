/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.xpack.watcher.actions.throttler.Throttler;
import org.elasticsearch.xpack.watcher.condition.Condition;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.transform.Transform;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.watch.Watch;

import java.io.IOException;
import java.time.Clock;
import java.util.Objects;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class ActionWrapper implements ToXContentObject {

    private String id;
    @Nullable
    private final Condition condition;
    @Nullable
    private final ExecutableTransform transform;
    private final ActionThrottler throttler;
    private final ExecutableAction action;

    public ActionWrapper(String id, ExecutableAction action) {
        this(id, null, null, null, action);
    }

    public ActionWrapper(String id, ActionThrottler throttler,
                         @Nullable Condition condition,
                         @Nullable ExecutableTransform transform,
                         ExecutableAction action) {
        this.id = id;
        this.condition = condition;
        this.throttler = throttler;
        this.transform = transform;
        this.action = action;
    }

    public String id() {
        return id;
    }

    public Condition condition() {
        return condition;
    }

    public ExecutableTransform transform() {
        return transform;
    }

    public Throttler throttler() {
        return throttler;
    }

    public ExecutableAction action() {
        return action;
    }

    /**
     * Execute the current {@link #action()}.
     * <p>
     * This executes in the order of:
     * <ol>
     * <li>Throttling</li>
     * <li>Conditional Check</li>
     * <li>Transformation</li>
     * <li>Action</li>
     * </ol>
     *
     * @param ctx The current watch's context
     * @return Never {@code null}
     */
    public ActionWrapper.Result execute(WatchExecutionContext ctx) {
        ActionWrapper.Result result = ctx.actionsResults().get(id);
        if (result != null) {
            return result;
        }
        if (!ctx.skipThrottling(id)) {
            Throttler.Result throttleResult = throttler.throttle(id, ctx);
            if (throttleResult.throttle()) {
                if (throttleResult.type() == Throttler.Type.ACK) {
                    return new ActionWrapper.Result(id, new Action.Result.Acknowledged(action.type(), throttleResult.reason()));
                } else {
                    return new ActionWrapper.Result(id, new Action.Result.Throttled(action.type(), throttleResult.reason()));
                }
            }
        }
        Condition.Result conditionResult = null;
        if (condition != null) {
            try {
                conditionResult = condition.execute(ctx);
                if (conditionResult.met() == false) {
                    return new ActionWrapper.Result(id, conditionResult, null,
                                                    new Action.Result.ConditionFailed(action.type(), "condition not met. skipping"));
                }
            } catch (RuntimeException e) {
                action.logger().error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "failed to execute action [{}/{}]. failed to execute condition", ctx.watch().id(), id), e);
                return new ActionWrapper.Result(id, new Action.Result.ConditionFailed(action.type(),
                                                "condition failed. skipping: {}", e.getMessage()));
            }
        }
        Payload payload = ctx.payload();
        Transform.Result transformResult = null;
        if (transform != null) {
            try {
                transformResult = transform.execute(ctx, payload);
                if (transformResult.status() == Transform.Result.Status.FAILURE) {
                    action.logger().error("failed to execute action [{}/{}]. failed to transform payload. {}", ctx.watch().id(), id,
                            transformResult.reason());
                    String msg = "Failed to transform payload";
                    return new ActionWrapper.Result(id, conditionResult, transformResult, new Action.Result.Failure(action.type(), msg));
                }
                payload = transformResult.payload();
            } catch (Exception e) {
                action.logger().error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "failed to execute action [{}/{}]. failed to transform payload.", ctx.watch().id(), id), e);
                return new ActionWrapper.Result(id, conditionResult, null,
                                                new Action.Result.Failure(action.type(), "Failed to transform payload. error: {}",
                                                    ExceptionsHelper.detailedMessage(e)));
            }
        }
        try {
            Action.Result actionResult = action.execute(id, ctx, payload);
            return new ActionWrapper.Result(id, conditionResult, transformResult, actionResult);
        } catch (Exception e) {
            action.logger().error(
                    (Supplier<?>) () -> new ParameterizedMessage("failed to execute action [{}/{}]", ctx.watch().id(), id), e);
            return new ActionWrapper.Result(id, new Action.Result.Failure(action.type(), ExceptionsHelper.detailedMessage(e)));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionWrapper that = (ActionWrapper) o;

        if (!id.equals(that.id)) return false;
        if (condition != null ? !condition.equals(that.condition) : that.condition != null) return false;
        if (transform != null ? !transform.equals(that.transform) : that.transform != null) return false;
        return action.equals(that.action);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (condition != null ? condition.hashCode() : 0);
        result = 31 * result + (transform != null ? transform.hashCode() : 0);
        result = 31 * result + action.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        TimeValue throttlePeriod = throttler.throttlePeriod();
        if (throttlePeriod != null) {
            builder.timeValueField(Throttler.Field.THROTTLE_PERIOD.getPreferredName(),
                    Throttler.Field.THROTTLE_PERIOD_HUMAN.getPreferredName(), throttlePeriod);
        }
        if (condition != null) {
            builder.startObject(Watch.Field.CONDITION.getPreferredName())
                    .field(condition.type(), condition, params)
                    .endObject();
        }
        if (transform != null) {
            builder.startObject(Transform.Field.TRANSFORM.getPreferredName())
                    .field(transform.type(), transform, params)
                    .endObject();
        }
        builder.field(action.type(), action, params);
        return builder.endObject();
    }

    static ActionWrapper parse(String watchId, String actionId, XContentParser parser, ActionRegistry actionRegistry, Clock clock,
                               XPackLicenseState licenseState) throws IOException {

        assert parser.currentToken() == XContentParser.Token.START_OBJECT;

        Condition condition = null;
        ExecutableTransform transform = null;
        TimeValue throttlePeriod = null;
        ExecutableAction action = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (Watch.Field.CONDITION.match(currentFieldName)) {
                    condition = actionRegistry.getConditionRegistry().parseExecutable(watchId, parser);
                } else if (Transform.Field.TRANSFORM.match(currentFieldName)) {
                    transform = actionRegistry.getTransformRegistry().parse(watchId, parser);
                } else if (Throttler.Field.THROTTLE_PERIOD.match(currentFieldName)) {
                    throttlePeriod = timeValueMillis(parser.longValue());
                } else if (Throttler.Field.THROTTLE_PERIOD_HUMAN.match(currentFieldName)) {
                    try {
                        throttlePeriod = WatcherDateTimeUtils.parseTimeValue(parser, Throttler.Field.THROTTLE_PERIOD_HUMAN.toString());
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse action [{}/{}]. failed to parse field [{}] as time value",
                                pe, watchId, actionId, currentFieldName);
                    }
                } else {
                    // it's the type of the action
                    ActionFactory actionFactory = actionRegistry.factory(currentFieldName);
                    if (actionFactory == null) {
                        throw new ElasticsearchParseException("could not parse action [{}/{}]. unknown action type [{}]", watchId,
                                actionId, currentFieldName);
                    }
                    action = actionFactory.parseExecutable(watchId, actionId, parser);
                }
            }
        }
        if (action == null) {
            throw new ElasticsearchParseException("could not parse watch action [{}/{}]. missing action type", watchId, actionId);
        }

        ActionThrottler throttler = new ActionThrottler(clock, throttlePeriod, licenseState);
        return new ActionWrapper(actionId, throttler, condition, transform, action);
    }

    public static class Result implements ToXContent {

        private final String id;
        @Nullable
        private final Condition.Result condition;
        @Nullable
        private final Transform.Result transform;
        private final Action.Result action;

        public Result(String id, Action.Result action) {
            this(id, null, null, action);
        }

        public Result(String id, @Nullable Condition.Result condition, @Nullable Transform.Result transform, Action.Result action) {
            this.id = id;
            this.condition = condition;
            this.transform = transform;
            this.action = action;
        }

        public String id() {
            return id;
        }

        public Condition.Result condition() {
            return condition;
        }

        public Transform.Result transform() {
            return transform;
        }

        public Action.Result action() {
            return action;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Result result = (Result) o;

            return Objects.equals(id, result.id) &&
                    Objects.equals(condition, result.condition) &&
                    Objects.equals(transform, result.transform) &&
                    Objects.equals(action, result.action);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, condition, transform, action);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Field.ID.getPreferredName(), id);
            builder.field(Field.TYPE.getPreferredName(), action.type());
            builder.field(Field.STATUS.getPreferredName(), action.status(), params);
            if (condition != null) {
                builder.field(Watch.Field.CONDITION.getPreferredName(), condition, params);
            }
            if (transform != null) {
                builder.field(Transform.Field.TRANSFORM.getPreferredName(), transform, params);
            }
            action.toXContent(builder, params);
            return builder.endObject();
        }
    }

    interface Field {
        ParseField ID = new ParseField("id");
        ParseField TYPE = new ParseField("type");
        ParseField STATUS = new ParseField("status");
    }
}

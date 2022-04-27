/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.throttler.ActionThrottler;
import org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler;
import org.elasticsearch.xpack.core.watcher.actions.throttler.ThrottlerField;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

public class ActionWrapper implements ToXContentObject {

    private String id;
    @Nullable
    private final ExecutableCondition condition;
    @Nullable
    private final ExecutableTransform<? extends Transform, ? extends Transform.Result> transform;
    private final ActionThrottler throttler;
    private final ExecutableAction<? extends Action> action;
    @Nullable
    private String path;
    private final Integer maxIterations;

    public ActionWrapper(
        String id,
        ActionThrottler throttler,
        @Nullable ExecutableCondition condition,
        @Nullable ExecutableTransform<? extends Transform, ? extends Transform.Result> transform,
        ExecutableAction<? extends Action> action,
        @Nullable String path,
        @Nullable Integer maxIterations
    ) {
        this.id = id;
        this.condition = condition;
        this.throttler = throttler;
        this.transform = transform;
        this.action = action;
        this.path = path;
        this.maxIterations = (maxIterations != null) ? maxIterations : 100;
    }

    public String id() {
        return id;
    }

    public ExecutableCondition condition() {
        return condition;
    }

    public ExecutableTransform<? extends Transform, ? extends Transform.Result> transform() {
        return transform;
    }

    public Throttler throttler() {
        return throttler;
    }

    public ExecutableAction<? extends Action> action() {
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
    @SuppressWarnings("unchecked")
    public ActionWrapperResult execute(WatchExecutionContext ctx) {
        ActionWrapperResult result = ctx.actionsResults().get(id);
        if (result != null) {
            return result;
        }
        if (ctx.skipThrottling(id) == false) {
            Throttler.Result throttleResult = throttler.throttle(id, ctx);
            if (throttleResult.throttle()) {
                if (throttleResult.type() == Throttler.Type.ACK) {
                    return new ActionWrapperResult(id, new Action.Result.Acknowledged(action.type(), throttleResult.reason()));
                } else {
                    return new ActionWrapperResult(id, new Action.Result.Throttled(action.type(), throttleResult.reason()));
                }
            }
        }
        Condition.Result conditionResult = null;
        if (condition != null) {
            try {
                conditionResult = condition.execute(ctx);
                if (conditionResult.met() == false) {
                    ctx.watch().status().actionStatus(id).resetAckStatus(ZonedDateTime.now(ZoneOffset.UTC));
                    return new ActionWrapperResult(
                        id,
                        conditionResult,
                        null,
                        new Action.Result.ConditionFailed(action.type(), "condition not met. skipping")
                    );
                }
            } catch (RuntimeException e) {
                action.logger()
                    .error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to execute action [{}/{}]. failed to execute condition",
                            ctx.watch().id(),
                            id
                        ),
                        e
                    );
                return new ActionWrapperResult(
                    id,
                    new Action.Result.ConditionFailed(action.type(), "condition failed. skipping: {}", e.getMessage())
                );
            }
        }
        Payload payload = ctx.payload();
        Transform.Result transformResult = null;
        if (transform != null) {
            try {
                transformResult = transform.execute(ctx, payload);
                if (transformResult.status() == Transform.Result.Status.FAILURE) {
                    action.logger()
                        .error(
                            "failed to execute action [{}/{}]. failed to transform payload. {}",
                            ctx.watch().id(),
                            id,
                            transformResult.reason()
                        );
                    String msg = "Failed to transform payload";
                    return new ActionWrapperResult(id, conditionResult, transformResult, new Action.Result.Failure(action.type(), msg));
                }
                payload = transformResult.payload();
            } catch (Exception e) {
                action.logger()
                    .error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "failed to execute action [{}/{}]. failed to transform payload.",
                            ctx.watch().id(),
                            id
                        ),
                        e
                    );
                return new ActionWrapperResult(id, conditionResult, null, new Action.Result.FailureWithException(action.type(), e));
            }
        }
        if (Strings.isEmpty(path)) {
            try {
                Action.Result actionResult = action.execute(id, ctx, payload);
                return new ActionWrapperResult(id, conditionResult, transformResult, actionResult);
            } catch (Exception e) {
                action.logger()
                    .error((Supplier<?>) () -> new ParameterizedMessage("failed to execute action [{}/{}]", ctx.watch().id(), id), e);
                return new ActionWrapperResult(id, new Action.Result.FailureWithException(action.type(), e));
            }
        } else {
            try {
                List<Action.Result> results = new ArrayList<>();
                Object object = ObjectPath.eval(path, toMap(ctx));
                int runs = 0;
                if (object instanceof Collection<?> collection) {
                    if (collection.isEmpty()) {
                        throw new ElasticsearchException("foreach object [{}] was an empty list, could not run any action", path);
                    } else {
                        for (Object o : collection) {
                            if (runs >= maxIterations) {
                                break;
                            }
                            if (o instanceof Map) {
                                results.add(action.execute(id, ctx, new Payload.Simple((Map<String, Object>) o)));
                            } else {
                                results.add(action.execute(id, ctx, new Payload.Simple("_value", o)));
                            }
                            runs++;
                        }
                    }
                } else if (object == null) {
                    throw new ElasticsearchException("specified foreach object was null: [{}]", path);
                } else {
                    throw new ElasticsearchException("specified foreach object was not a an array/collection: [{}]", path);
                }

                // check if we have mixed results, then set to partial failure
                final Set<Action.Result.Status> statuses = results.stream().map(Action.Result::status).collect(Collectors.toSet());
                Action.Result.Status status;
                if (statuses.size() == 1) {
                    status = statuses.iterator().next();
                } else {
                    status = Action.Result.Status.PARTIAL_FAILURE;
                }

                final int numberOfActionsExecuted = runs;
                return new ActionWrapperResult(id, conditionResult, transformResult, new Action.Result(action.type(), status) {
                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                        builder.field("number_of_actions_executed", numberOfActionsExecuted);
                        builder.startArray(WatchField.FOREACH.getPreferredName());
                        for (Action.Result result : results) {
                            builder.startObject();
                            result.toXContent(builder, params);
                            builder.endObject();
                        }
                        builder.endArray();
                        builder.field(WatchField.MAX_ITERATIONS.getPreferredName(), maxIterations);
                        return builder;
                    }
                });
            } catch (Exception e) {
                action.logger()
                    .error((Supplier<?>) () -> new ParameterizedMessage("failed to execute action [{}/{}]", ctx.watch().id(), id), e);
                return new ActionWrapperResult(id, new Action.Result.FailureWithException(action.type(), e));
            }
        }
    }

    private Map<String, Object> toMap(WatchExecutionContext ctx) {
        Map<String, Object> model = new HashMap<>();
        model.put("id", ctx.id().value());
        model.put("watch_id", ctx.id().watchId());
        model.put("execution_time", ZonedDateTime.ofInstant(ctx.executionTime().toInstant(), ZoneOffset.UTC));
        model.put("trigger", ctx.triggerEvent().data());
        model.put("metadata", ctx.watch().metadata());
        model.put("vars", ctx.vars());
        if (ctx.payload().data() != null) {
            model.put("payload", ctx.payload().data());
        }
        return Map.of("ctx", model);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionWrapper that = (ActionWrapper) o;

        return Objects.equals(id, that.id)
            && Objects.equals(condition, that.condition)
            && Objects.equals(transform, that.transform)
            && Objects.equals(action, that.action);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, condition, transform, action);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        TimeValue throttlePeriod = throttler.throttlePeriod();
        if (throttlePeriod != null) {
            builder.humanReadableField(
                ThrottlerField.THROTTLE_PERIOD.getPreferredName(),
                ThrottlerField.THROTTLE_PERIOD_HUMAN.getPreferredName(),
                throttlePeriod
            );
        }
        if (condition != null) {
            builder.startObject(WatchField.CONDITION.getPreferredName()).field(condition.type(), condition, params).endObject();
        }
        if (transform != null) {
            builder.startObject(Transform.TRANSFORM.getPreferredName()).field(transform.type(), transform, params).endObject();
        }
        if (Strings.isEmpty(path) == false) {
            builder.field(WatchField.FOREACH.getPreferredName(), path);
            builder.field(WatchField.MAX_ITERATIONS.getPreferredName(), maxIterations);
        }

        builder.field(action.type(), action, params);
        return builder.endObject();
    }

    static ActionWrapper parse(
        String watchId,
        String actionId,
        XContentParser parser,
        ActionRegistry actionRegistry,
        Clock clock,
        XPackLicenseState licenseState
    ) throws IOException {

        assert parser.currentToken() == XContentParser.Token.START_OBJECT;

        ExecutableCondition condition = null;
        ExecutableTransform<? extends Transform, ? extends Transform.Result> transform = null;
        TimeValue throttlePeriod = null;
        String path = null;
        ExecutableAction<? extends Action> action = null;
        Integer maxIterations = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (WatchField.CONDITION.match(currentFieldName, parser.getDeprecationHandler())) {
                    condition = actionRegistry.getConditionRegistry().parseExecutable(watchId, parser);
                } else if (WatchField.FOREACH.match(currentFieldName, parser.getDeprecationHandler())) {
                    path = parser.text();
                } else if (Transform.TRANSFORM.match(currentFieldName, parser.getDeprecationHandler())) {
                    transform = actionRegistry.getTransformRegistry().parse(watchId, parser);
                } else if (ThrottlerField.THROTTLE_PERIOD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throttlePeriod = timeValueMillis(parser.longValue());
                } else if (ThrottlerField.THROTTLE_PERIOD_HUMAN.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        throttlePeriod = WatcherDateTimeUtils.parseTimeValue(parser, ThrottlerField.THROTTLE_PERIOD_HUMAN.toString());
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException(
                            "could not parse action [{}/{}]. failed to parse field [{}] as time value",
                            pe,
                            watchId,
                            actionId,
                            currentFieldName
                        );
                    }
                } else if (WatchField.MAX_ITERATIONS.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxIterations = parser.intValue();
                } else {
                    // it's the type of the action
                    ActionFactory actionFactory = actionRegistry.factory(currentFieldName);
                    if (actionFactory == null) {
                        throw new ElasticsearchParseException(
                            "could not parse action [{}/{}]. unknown action type [{}]",
                            watchId,
                            actionId,
                            currentFieldName
                        );
                    }
                    action = actionFactory.parseExecutable(watchId, actionId, parser);
                }
            }
        }
        if (action == null) {
            throw new ElasticsearchParseException("could not parse watch action [{}/{}]. missing action type", watchId, actionId);
        }

        ActionThrottler throttler = new ActionThrottler(clock, throttlePeriod, licenseState);
        return new ActionWrapper(actionId, throttler, condition, transform, action, path, maxIterations);
    }

}

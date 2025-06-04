/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.client;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.throttler.ThrottlerField;
import org.elasticsearch.xpack.core.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.input.Input;
import org.elasticsearch.xpack.core.watcher.input.none.NoneInput;
import org.elasticsearch.xpack.core.watcher.support.Exceptions;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class WatchSourceBuilder implements ToXContentObject {

    private Trigger trigger;
    private Input input = NoneInput.INSTANCE;
    private Condition condition = AlwaysCondition.INSTANCE;
    private Transform transform = null;
    private Map<String, TransformedAction> actions = new HashMap<>();
    private TimeValue defaultThrottlePeriod = null;
    private Map<String, Object> metadata;

    public WatchSourceBuilder trigger(Trigger.Builder<? extends Trigger> trigger) {
        return trigger(trigger.build());
    }

    public WatchSourceBuilder trigger(Trigger trigger) {
        this.trigger = trigger;
        return this;
    }

    public WatchSourceBuilder input(Input.Builder<? extends Input> input) {
        return input(input.build());
    }

    public WatchSourceBuilder input(Input input) {
        this.input = input;
        return this;
    }

    public WatchSourceBuilder condition(Condition condition) {
        this.condition = condition;
        return this;
    }

    public WatchSourceBuilder transform(Transform transform) {
        this.transform = transform;
        return this;
    }

    public WatchSourceBuilder transform(Transform.Builder<? extends Transform> transform) {
        return transform(transform.build());
    }

    public WatchSourceBuilder defaultThrottlePeriod(TimeValue throttlePeriod) {
        this.defaultThrottlePeriod = throttlePeriod;
        return this;
    }

    public WatchSourceBuilder addAction(String id, Action.Builder<? extends Action> action) {
        return addAction(id, null, null, action.build());
    }

    public WatchSourceBuilder addAction(String id, TimeValue throttlePeriod, Action.Builder<? extends Action> action) {
        return addAction(id, throttlePeriod, null, action.build());
    }

    public WatchSourceBuilder addAction(
        String id,
        Transform.Builder<? extends Transform> transformBuilder,
        Action.Builder<? extends Action> action
    ) {
        return addAction(id, null, transformBuilder.build(), action.build());
    }

    @SuppressWarnings("HiddenField")
    public WatchSourceBuilder addAction(String id, Condition condition, Action.Builder<? extends Action> action) {
        return addAction(id, null, condition, null, action.build());
    }

    public WatchSourceBuilder addAction(
        String id,
        TimeValue throttlePeriod,
        Transform.Builder<? extends Transform> transformBuilder,
        Action.Builder<? extends Action> action
    ) {
        return addAction(id, throttlePeriod, transformBuilder.build(), action.build());
    }

    public WatchSourceBuilder addAction(String id, TimeValue throttlePeriod, Transform aTransform, Action action) {
        actions.put(id, new TransformedAction(id, action, throttlePeriod, null, aTransform, null));
        return this;
    }

    @SuppressWarnings("HiddenField")
    public WatchSourceBuilder addAction(
        String id,
        TimeValue throttlePeriod,
        Condition condition,
        Transform.Builder<? extends Transform> transform,
        Action.Builder<? extends Action> action
    ) {
        return addAction(id, throttlePeriod, condition, transform.build(), action.build());
    }

    @SuppressWarnings("HiddenField")
    public WatchSourceBuilder addAction(String id, TimeValue throttlePeriod, Condition condition, Transform transform, Action action) {
        actions.put(id, new TransformedAction(id, action, throttlePeriod, condition, transform, null));
        return this;
    }

    @SuppressWarnings("HiddenField")
    public WatchSourceBuilder addAction(
        String id,
        TimeValue throttlePeriod,
        Condition condition,
        Transform transform,
        String path,
        Action action
    ) {
        actions.put(id, new TransformedAction(id, action, throttlePeriod, condition, transform, path));
        return this;
    }

    public WatchSourceBuilder metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public XContentSource build() throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            return new XContentSource(toXContent(builder, ToXContent.EMPTY_PARAMS));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (trigger == null) {
            throw Exceptions.illegalState("failed to build watch source. no trigger defined");
        }
        builder.startObject(WatchField.TRIGGER.getPreferredName()).field(trigger.type(), trigger, params).endObject();

        builder.startObject(WatchField.INPUT.getPreferredName()).field(input.type(), input, params).endObject();

        builder.startObject(WatchField.CONDITION.getPreferredName()).field(condition.type(), condition, params).endObject();

        if (transform != null) {
            builder.startObject(WatchField.TRANSFORM.getPreferredName()).field(transform.type(), transform, params).endObject();
        }

        if (defaultThrottlePeriod != null) {
            builder.humanReadableField(
                WatchField.THROTTLE_PERIOD.getPreferredName(),
                WatchField.THROTTLE_PERIOD_HUMAN.getPreferredName(),
                defaultThrottlePeriod
            );
        }

        builder.startObject(WatchField.ACTIONS.getPreferredName());
        for (Map.Entry<String, TransformedAction> entry : actions.entrySet()) {
            builder.field(entry.getKey(), entry.getValue(), params);
        }
        builder.endObject();

        if (metadata != null) {
            builder.field(WatchField.METADATA.getPreferredName(), metadata);
        }

        return builder.endObject();
    }

    /**
     * Returns a {@link org.elasticsearch.common.bytes.BytesReference}
     * containing the {@link ToXContent} output in binary format. Builds the
     * request as the provided <code>contentType</code>
     */
    public final BytesReference buildAsBytes(XContentType contentType) {
        try {
            WatcherParams params = WatcherParams.builder().hideSecrets(false).build();
            return XContentHelper.toXContent(this, contentType, params, false);
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to build ToXContent", e);
        }
    }

    static class TransformedAction implements ToXContentObject {

        private final Action action;
        @Nullable
        private String path;
        @Nullable
        private final TimeValue throttlePeriod;
        @Nullable
        private final Condition condition;
        @Nullable
        private final Transform transform;

        TransformedAction(
            String id,
            Action action,
            @Nullable TimeValue throttlePeriod,
            @Nullable Condition condition,
            @Nullable Transform transform,
            @Nullable String path
        ) {
            this.throttlePeriod = throttlePeriod;
            this.condition = condition;
            this.transform = transform;
            this.action = action;
            this.path = path;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
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
            if (path != null) {
                builder.field("foreach", path);
            }
            builder.field(action.type(), action, params);
            return builder.endObject();
        }
    }
}

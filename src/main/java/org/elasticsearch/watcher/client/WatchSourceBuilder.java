/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.client;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.none.NoneInput;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.watch.Watch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class WatchSourceBuilder implements ToXContent {

    private Trigger trigger;
    private Input input = NoneInput.INSTANCE;
    private Condition condition = AlwaysCondition.INSTANCE;
    private Transform transform = null;
    private Map<String, TransformedAction> actions = new HashMap<>();
    private TimeValue throttlePeriod = null;
    private Map<String, Object> metadata;

    public WatchSourceBuilder trigger(Trigger.Builder trigger) {
        return trigger(trigger.build());
    }

    public WatchSourceBuilder trigger(Trigger trigger) {
        this.trigger = trigger;
        return this;
    }

    public WatchSourceBuilder input(Input.Builder input) {
        return input(input.build());
    }

    public WatchSourceBuilder input(Input input) {
        this.input = input;
        return this;
    }

    public WatchSourceBuilder condition(Condition.Builder condition) {
        return condition(condition.build());
    }

    public WatchSourceBuilder condition(Condition condition) {
        this.condition = condition;
        return this;
    }

    public WatchSourceBuilder transform(Transform transform) {
        this.transform = transform;
        return this;
    }

    public WatchSourceBuilder transform(Transform.Builder transform) {
        return transform(transform.build());
    }

    public WatchSourceBuilder throttlePeriod(TimeValue throttlePeriod) {
        this.throttlePeriod = throttlePeriod;
        return this;
    }

    public WatchSourceBuilder addAction(String id, Transform.Builder transform, Action action) {
        return addAction(id, transform.build(), action);
    }


    public WatchSourceBuilder addAction(String id, Action action) {
        actions.put(id, new TransformedAction(id, action));
        return this;
    }

    public WatchSourceBuilder addAction(String id, Action.Builder action) {
        return addAction(id, action.build());
    }

    public WatchSourceBuilder addAction(String id, Transform.Builder transform, Action.Builder action) {
        actions.put(id, new TransformedAction(id, action.build(), transform.build()));
        return this;
    }

    public WatchSourceBuilder addAction(String id, Transform transform, Action action) {
        actions.put(id, new TransformedAction(id, action, transform));
        return this;
    }

    public WatchSourceBuilder metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (trigger == null) {
            throw new BuilderException("failed to build watch source. no trigger defined");
        }
        builder.startObject(Watch.Parser.TRIGGER_FIELD.getPreferredName())
                .field(trigger.type(), trigger, params)
                .endObject();

        builder.startObject(Watch.Parser.INPUT_FIELD.getPreferredName())
                .field(input.type(), input, params)
                .endObject();

        builder.startObject(Watch.Parser.CONDITION_FIELD.getPreferredName())
                .field(condition.type(), condition, params)
                .endObject();

        if (transform != null) {
            builder.startObject(Watch.Parser.TRANSFORM_FIELD.getPreferredName())
                    .field(transform.type(), transform, params)
                    .endObject();
        }

        if (throttlePeriod != null) {
            builder.field(Watch.Parser.THROTTLE_PERIOD_FIELD.getPreferredName(), throttlePeriod.getMillis());
        }

        builder.startObject(Watch.Parser.ACTIONS_FIELD.getPreferredName());
        for (Map.Entry<String, TransformedAction> entry : actions.entrySet()) {
            builder.field(entry.getKey(), entry.getValue(), params);
        }
        builder.endObject();

        if (metadata != null) {
            builder.field(Watch.Parser.META_FIELD.getPreferredName(), metadata);
        }

        return builder.endObject();
    }

    public BytesReference buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.bytes();
        } catch (java.lang.Exception e) {
            throw new BuilderException("Failed to build watch source", e);
        }
    }

    static class TransformedAction implements ToXContent {

        private final String id;
        private final Action action;
        private final @Nullable Transform transform;

        public TransformedAction(String id, Action action) {
            this(id, action, null);
        }

        public TransformedAction(String id, Action action, @Nullable Transform transform) {
            this.id = id;
            this.transform = transform;
            this.action = action;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (transform != null) {
                builder.startObject(Transform.Field.TRANSFORM.getPreferredName())
                        .field(transform.type(), transform, params)
                        .endObject();
            }
            builder.field(action.type(), action, params);
            return builder.endObject();
        }
    }

    public static class BuilderException extends WatcherException {

        public BuilderException(String msg) {
            super(msg);
        }

        public BuilderException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

}

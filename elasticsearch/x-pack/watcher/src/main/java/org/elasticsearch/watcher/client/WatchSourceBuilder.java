/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.throttler.Throttler;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.none.NoneInput;
import org.elasticsearch.watcher.support.Exceptions;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.watch.Watch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 */
public class WatchSourceBuilder implements ToXContent {

    private Trigger trigger;
    private Input input = NoneInput.INSTANCE;
    private Condition condition = AlwaysCondition.INSTANCE;
    private Transform transform = null;
    private Map<String, TransformedAction> actions = new HashMap<>();
    private TimeValue defaultThrottlePeriod = null;
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

    public WatchSourceBuilder defaultThrottlePeriod(TimeValue throttlePeriod) {
        this.defaultThrottlePeriod = throttlePeriod;
        return this;
    }

    public WatchSourceBuilder addAction(String id, Action.Builder action) {
        return addAction(id, null, null, action.build());
    }

    public WatchSourceBuilder addAction(String id, TimeValue throttlePeriod, Action.Builder action) {
        return addAction(id, throttlePeriod, null, action.build());
    }

    public WatchSourceBuilder addAction(String id, Transform.Builder transform, Action.Builder action) {
        return addAction(id, null, transform.build(), action.build());
    }

    public WatchSourceBuilder addAction(String id, TimeValue throttlePeriod, Transform.Builder transform, Action.Builder action) {
        return addAction(id, throttlePeriod, transform.build(), action.build());
    }

    public WatchSourceBuilder addAction(String id, TimeValue throttlePeriod, Transform transform, Action action) {
        actions.put(id, new TransformedAction(id, action, throttlePeriod, transform));
        return this;
    }

    public WatchSourceBuilder metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public XContentSource build() throws IOException {
        return new XContentSource(toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (trigger == null) {
            throw Exceptions.illegalState("failed to build watch source. no trigger defined");
        }
        builder.startObject(Watch.Field.TRIGGER.getPreferredName())
                .field(trigger.type(), trigger, params)
                .endObject();

        builder.startObject(Watch.Field.INPUT.getPreferredName())
                .field(input.type(), input, params)
                .endObject();

        builder.startObject(Watch.Field.CONDITION.getPreferredName())
                .field(condition.type(), condition, params)
                .endObject();

        if (transform != null) {
            builder.startObject(Watch.Field.TRANSFORM.getPreferredName())
                    .field(transform.type(), transform, params)
                    .endObject();
        }

        if (defaultThrottlePeriod != null) {
            builder.field(Watch.Field.THROTTLE_PERIOD.getPreferredName(), defaultThrottlePeriod.toString());
        }

        builder.startObject(Watch.Field.ACTIONS.getPreferredName());
        for (Map.Entry<String, TransformedAction> entry : actions.entrySet()) {
            builder.field(entry.getKey(), entry.getValue(), params);
        }
        builder.endObject();

        if (metadata != null) {
            builder.field(Watch.Field.METADATA.getPreferredName(), metadata);
        }

        return builder.endObject();
    }

    public BytesReference buildAsBytes(XContentType contentType) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.bytes();
        } catch (IOException ioe) {
            // todo think of a better std exception for this
            throw new ElasticsearchException("failed to render watch source as bytes", ioe);
        }
    }

    static class TransformedAction implements ToXContent {

        private final String id;
        private final Action action;
        private final @Nullable TimeValue throttlePeriod;
        private final @Nullable Transform transform;

        public TransformedAction(String id, Action action, @Nullable TimeValue throttlePeriod, @Nullable Transform transform) {
            this.id = id;
            this.throttlePeriod = throttlePeriod;
            this.transform = transform;
            this.action = action;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (throttlePeriod != null) {
                builder.field(Throttler.Field.THROTTLE_PERIOD.getPreferredName(), throttlePeriod);
            }
            if (transform != null) {
                builder.startObject(Transform.Field.TRANSFORM.getPreferredName())
                        .field(transform.type(), transform, params)
                        .endObject();
            }
            builder.field(action.type(), action, params);
            return builder.endObject();
        }
    }
}

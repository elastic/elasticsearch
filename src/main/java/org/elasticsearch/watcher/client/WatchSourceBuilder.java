/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.client;

import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.ConditionBuilders;
import org.elasticsearch.watcher.input.Input;
import org.elasticsearch.watcher.input.NoneInput;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class WatchSourceBuilder implements ToXContent {

    public static WatchSourceBuilder watchSourceBuilder() {
        return new WatchSourceBuilder();
    }

    private Trigger.SourceBuilder trigger;
    private Input.SourceBuilder input = NoneInput.SourceBuilder.INSTANCE;
    private Condition.SourceBuilder condition = ConditionBuilders.alwaysTrueCondition();
    private Transform.SourceBuilder transform = null;
    private Set<Action.SourceBuilder> actions = new HashSet<>();
    private TimeValue throttlePeriod = null;
    private Map<String, Object> metadata;

    public WatchSourceBuilder trigger(Trigger.SourceBuilder trigger) {
        this.trigger = trigger;
        return this;
    }

    public WatchSourceBuilder input(Input.SourceBuilder input) {
        this.input = input;
        return this;
    }

    public WatchSourceBuilder condition(Condition.SourceBuilder condition) {
        this.condition = condition;
        return this;
    }

    public WatchSourceBuilder transform(Transform.SourceBuilder transform) {
        this.transform = transform;
        return this;
    }

    public WatchSourceBuilder throttlePeriod(TimeValue throttlePeriod) {
        this.throttlePeriod = throttlePeriod;
        return this;
    }

    public WatchSourceBuilder addAction(Action.SourceBuilder action) {
        actions.add(action);
        return this;
    }

    public WatchSourceBuilder metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject(Watch.Parser.TRIGGER_FIELD.getPreferredName())
                .field(trigger.type(), trigger)
                .endObject();

        builder.startObject(Watch.Parser.INPUT_FIELD.getPreferredName())
                .field(input.type(), input)
                .endObject();

        builder.startObject(Watch.Parser.CONDITION_FIELD.getPreferredName())
                .field(condition.type(), condition)
                .endObject();

        if (transform != null) {
            builder.startObject(Watch.Parser.TRANSFORM_FIELD.getPreferredName())
                    .field(transform.type(), transform)
                    .endObject();
        }

        if (throttlePeriod != null) {
            builder.field(Watch.Parser.THROTTLE_PERIOD_FIELD.getPreferredName(), throttlePeriod.getMillis());
        }

        builder.startArray(Watch.Parser.ACTIONS_FIELD.getPreferredName());
        for (Action.SourceBuilder action : actions) {
            builder.startObject().field(action.type(), action).endObject();
        }
        builder.endArray();

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
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }
}

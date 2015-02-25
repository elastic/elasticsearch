/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionBuilders;
import org.elasticsearch.alerts.input.Input;
import org.elasticsearch.alerts.input.NoneInput;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
import org.elasticsearch.alerts.transform.Transform;
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
public class AlertSourceBuilder implements ToXContent {

    public static AlertSourceBuilder alertSourceBuilder() {
        return new AlertSourceBuilder();
    }

    private Schedule schedule;
    private Input.SourceBuilder input = NoneInput.SourceBuilder.INSTANCE;
    private Condition.SourceBuilder condition = ConditionBuilders.alwaysTrueCondition();
    private Transform.SourceBuilder transform = null;
    private Set<Action.SourceBuilder> actions = new HashSet<>();
    private TimeValue throttlePeriod = null;
    private Map<String, Object> metadata;

    public AlertSourceBuilder schedule(Schedule schedule) {
        this.schedule = schedule;
        return this;
    }

    public AlertSourceBuilder input(Input.SourceBuilder input) {
        this.input = input;
        return this;
    }

    public AlertSourceBuilder condition(Condition.SourceBuilder condition) {
        this.condition = condition;
        return this;
    }

    public AlertSourceBuilder transform(Transform.SourceBuilder transform) {
        this.transform = transform;
        return this;
    }

    public AlertSourceBuilder throttlePeriod(TimeValue throttlePeriod) {
        this.throttlePeriod = throttlePeriod;
        return this;
    }

    public AlertSourceBuilder addAction(Action.SourceBuilder action) {
        actions.add(action);
        return this;
    }

    public AlertSourceBuilder metadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject(Alert.Parser.SCHEDULE_FIELD.getPreferredName())
                .field(schedule.type(), schedule)
                .endObject();

        builder.startObject(Alert.Parser.INPUT_FIELD.getPreferredName())
                .field(input.type(), input)
                .endObject();

        builder.startObject(Alert.Parser.CONDITION_FIELD.getPreferredName())
                .field(condition.type(), condition)
                .endObject();

        if (transform != null) {
            builder.startObject(Alert.Parser.TRANSFORM_FIELD.getPreferredName())
                    .field(transform.type(), transform)
                    .endObject();
        }

        if (throttlePeriod != null) {
            builder.field(Alert.Parser.THROTTLE_PERIOD_FIELD.getPreferredName(), throttlePeriod.getMillis());
        }

        builder.startArray(Alert.Parser.ACTIONS_FIELD.getPreferredName());
        for (Action.SourceBuilder action : actions) {
            builder.startObject().field(action.type(), action).endObject();
        }
        builder.endArray();

        if (metadata != null) {
            builder.field(Alert.Parser.META_FIELD.getPreferredName(), metadata);
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

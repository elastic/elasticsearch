/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.actions;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.transform.Transform;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.io.IOException;
import java.util.Objects;

public class ActionWrapperResult implements ToXContentObject {

    private final String id;
    @Nullable
    private final Condition.Result condition;
    @Nullable
    private final Transform.Result transform;
    private final Action.Result action;

    public ActionWrapperResult(String id, Action.Result action) {
        this(id, null, null, action);
    }

    public ActionWrapperResult(String id, @Nullable Condition.Result condition, @Nullable Transform.Result transform,
                               Action.Result action) {
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

        ActionWrapperResult result = (ActionWrapperResult) o;

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
        builder.field(ActionWrapperField.ID.getPreferredName(), id);
        builder.field(ActionWrapperField.TYPE.getPreferredName(), action.type());
        builder.field(ActionWrapperField.STATUS.getPreferredName(), action.status().value());
        if (condition != null) {
            builder.field(WatchField.CONDITION.getPreferredName(), condition, params);
        }
        if (transform != null) {
            builder.field(Transform.TRANSFORM.getPreferredName(), transform, params);
        }
        action.toXContent(builder, params);
        return builder.endObject();
    }
}

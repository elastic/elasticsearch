/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.transform.Transform;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 */
public abstract class Action<R extends Action.Result> implements ToXContent {

    protected final ESLogger logger;
    protected final Transform transform;

    protected Action(ESLogger logger, Transform transform) {
        this.logger = logger;
        this.transform = transform;
    }

    /**
     * @return the transform associated with this action (may be {@code null})
     */
    public Transform transform() {
        return transform;
    }

    /**
     * @return the type of this action
     */
    public abstract String type();

    /**
     * Executes this action
     */
    public R execute(WatchExecutionContext context) throws IOException {
        Payload payload = context.payload();
        Transform.Result transformResult = null;
        if (transform != null) {
            transformResult = transform.apply(context, payload);
            payload = transformResult.payload();
        }
        R result = execute(context, payload);
        if (transformResult != null) {
            result.transformResult = transformResult;
        }
        return result;
    }

    protected abstract R execute(WatchExecutionContext context, Payload payload) throws IOException;

    /**
     * Parses xcontent to a concrete action of the same type.
     */
    public static interface Parser<R extends Result, T extends Action<R>> {

        /**
         * @return  The type of the action
         */
        String type();

        /**
         * Parses the given xcontent and creates a concrete action
         */
        T parse(XContentParser parser) throws IOException;

        R parseResult(XContentParser parser) throws IOException;
    }

    public static abstract class Result implements ToXContent {

        public static final ParseField SUCCESS_FIELD = new ParseField("success");

        protected final String type;
        protected final boolean success;

        protected Transform.Result transformResult;

        protected Result(String type, boolean success) {
            this.type = type;
            this.success = success;
        }

        public String type() {
            return type;
        }

        public boolean success() {
            return success;
        }

        public Transform.Result transformResult() {
            return transformResult;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SUCCESS_FIELD.getPreferredName(), success);
            if (transformResult != null) {
                builder.startObject(Transform.Parser.TRANSFORM_RESULT_FIELD.getPreferredName())
                        .field(transformResult.type(), transformResult)
                        .endObject();
            }
            xContentBody(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException;

    }

    public static interface SourceBuilder extends ToXContent {

        String type();

    }
}

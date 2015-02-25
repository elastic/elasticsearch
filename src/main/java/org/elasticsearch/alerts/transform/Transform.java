/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class Transform implements ToXContent {

    public static final Transform NOOP = new Transform() {
        @Override
        public String type() {
            return "noop";
        }

        @Override
        public Result apply(ExecutionContext context, Payload payload) throws IOException {
            return new Result("noop", payload);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    };

    public abstract String type();

    public abstract Result apply(ExecutionContext ctx, Payload payload) throws IOException;

    protected static Map<String, Object> createModel(ExecutionContext ctx, Payload payload) {
        Map<String, Object> model = new HashMap<>();
        model.put(Variables.SCHEDULED_FIRE_TIME, ctx.scheduledTime());
        model.put(Variables.FIRE_TIME, ctx.fireTime());
        model.put(Variables.PAYLOAD, payload.data());
        return model;
    }

    public static class Result {

        private final String type;
        private final Payload payload;

        public Result(String type, Payload payload) {
            this.type = type;
            this.payload = payload;
        }

        public String type() {
            return type;
        }

        public Payload payload() {
            return payload;
        }
    }

    public static interface Parser<T extends Transform> {

        String type();

        T parse(XContentParser parser) throws IOException;

    }

    public static interface SourceBuilder extends ToXContent {

        String type();
    }

}

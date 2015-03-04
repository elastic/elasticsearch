/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transform;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public abstract class Transform<R extends Transform.Result> implements ToXContent {

    public abstract String type();

    public abstract Result apply(ExecutionContext ctx, Payload payload) throws IOException;

    public static abstract class Result implements ToXContent {

        protected final String type;
        protected final Payload payload;

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

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Parser.PAYLOAD_FIELD.getPreferredName(), payload);
            xContentBody(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException;

    }

    public static interface Parser<R extends Transform.Result, T extends Transform<R>> {

        public static final ParseField PAYLOAD_FIELD = new ParseField("payload");
        public static final ParseField TRANSFORM_FIELD = new ParseField("transform");
        public static final ParseField TRANSFORM_RESULT_FIELD = new ParseField("transform_result");

        String type();

        T parse(XContentParser parser) throws IOException;

        R parseResult(XContentParser parser) throws IOException;

    }

    public static interface SourceBuilder extends ToXContent {

        String type();
    }

}

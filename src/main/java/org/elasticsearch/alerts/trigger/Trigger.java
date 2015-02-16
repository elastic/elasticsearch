/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public abstract class Trigger<R extends Trigger.Result> implements ToXContent {

    protected final ESLogger logger;

    protected Trigger(ESLogger logger) {
        this.logger = logger;
    }

    /**
     * @return the type of this trigger
     */
    public abstract String type();

    /**
     * Executes this trigger
     */
    public abstract R execute(ExecutionContext ctx) throws IOException;


    /**
     * Parses xcontent to a concrete trigger of the same type.
     */
    public static interface Parser<T extends Trigger> {

        /**
         * @return  The type of the trigger
         */
        String type();

        /**
         * Parses the given xcontent and creates a concrete trigger
         */
        T parse(XContentParser parser) throws IOException;

        /**
         * Parses the given xContent and creates a concrete result
         */
        T.Result parseResult(XContentParser parser) throws IOException;
    }

    public abstract static class Result implements ToXContent {
        public static final ParseField TYPE_FIELD = new ParseField("type");
        public static final ParseField TRIGGERED_FIELD = new ParseField("triggered");
        public static final ParseField PAYLOAD_FIELD = new ParseField("payload");

        private final String type;
        private final boolean triggered;
        private final Payload payload;

        public Result(String type, boolean triggered, Payload payload) {
            this.type = type;
            this.triggered = triggered;
            this.payload = payload;
        }

        public String type() {
            return type;
        }

        public boolean triggered() {
            return triggered;
        }

        public Payload payload() {
            return payload;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
            .field(TYPE_FIELD.getPreferredName(), type())
            .field(TRIGGERED_FIELD.getPreferredName(), triggered())
            .field(PAYLOAD_FIELD.getPreferredName(), payload());
            return toXContentBody(builder, params).endObject();
        }

        protected abstract XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException;
    }
}

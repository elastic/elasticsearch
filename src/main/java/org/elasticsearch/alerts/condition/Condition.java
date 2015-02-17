/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition;

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
public abstract class Condition<R extends Condition.Result> implements ToXContent {

    protected final ESLogger logger;

    protected Condition(ESLogger logger) {
        this.logger = logger;
    }

    /**
     * @return the type of this condition
     */
    public abstract String type();

    /**
     * Executes this condition
     */
    public abstract R execute(ExecutionContext ctx) throws IOException;


    /**
     * Parses xcontent to a concrete condition of the same type.
     */
    public static interface Parser<R extends Condition.Result, C extends Condition<R>> {

        /**
         * @return  The type of the condition
         */
        String type();

        /**
         * Parses the given xcontent and creates a concrete condition
         */
        C parse(XContentParser parser) throws IOException;

        /**
         * Parses the given xContent and creates a concrete result
         */
        R parseResult(XContentParser parser) throws IOException;
    }

    public abstract static class Result implements ToXContent {

        public static final ParseField MET_FIELD = new ParseField("met");
        public static final ParseField PAYLOAD_FIELD = new ParseField("payload");

        private final String type;
        private final boolean met;
        private final Payload payload;

        public Result(String type, boolean met, Payload payload) {
            this.type = type;
            this.met = met;
            this.payload = payload;
        }

        public String type() {
            return type;
        }

        public boolean met() {
            return met;
        }

        public Payload payload() {
            return payload;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject()
            .field(MET_FIELD.getPreferredName(), met)
            .field(PAYLOAD_FIELD.getPreferredName(), payload);
            return toXContentBody(builder, params).endObject();
        }

        protected abstract XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 */
public abstract class Action<R extends Action.Result> implements ToXContent {

    public static final String ALERT_NAME_VARIABLE_NAME = "alert_name";
    public static final String RESPONSE_VARIABLE_NAME = "response";

    protected final ESLogger logger;

    protected Action(ESLogger logger) {
        this.logger = logger;
    }

    /**
     * @return the type of this action
     */
    public abstract String type();

    /**
     * Executes this action
     */
    public abstract R execute(ExecutionContext context, Payload payload) throws IOException;


    /**
     * Parses xcontent to a concrete action of the same type.
     */
    protected static interface Parser<T extends Action> {

        /**
         * @return  The type of the action
         */
        String type();

        /**
         * Parses the given xcontent and creates a concrete action
         */
        T parse(XContentParser parser) throws IOException;

        T.Result parseResult(XContentParser parser) throws IOException;
    }



    public static abstract class Result implements ToXContent {

        public static final ParseField SUCCESS_FIELD = new ParseField("success");

        protected final String type;
        protected final boolean success;

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

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SUCCESS_FIELD.getPreferredName(), success);
            xContentBody(builder, params);
            return builder.endObject();
        }

        protected abstract XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException;
    }
}

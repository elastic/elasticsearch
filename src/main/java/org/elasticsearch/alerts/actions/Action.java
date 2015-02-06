/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
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
    public abstract R execute(Alert alert, Payload payload) throws IOException;


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
    }



    public static abstract class Result implements ToXContent {

        private final boolean success;

        private final String type;

        public Result(String type, boolean success) {
            this.type = type;
            this.success = success;
        }

        public String type() {
            return type;
        }

        public boolean success() {
            return success;
        }
    }
}

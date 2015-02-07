/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger;

import org.elasticsearch.alerts.AlertContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
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
    public abstract R execute(AlertContext ctx) throws IOException;


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
    }

    public static abstract class Result {

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

    }
}

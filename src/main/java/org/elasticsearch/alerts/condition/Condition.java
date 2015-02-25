/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public abstract class Condition<R extends Condition.Result> implements ToXContent {

    protected static final ParseField MET_FIELD = new ParseField("met");

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

        private final String type;
        private final boolean met;

        public Result(String type, boolean met) {
            this.type = type;
            this.met = met;
        }

        public String type() {
            return type;
        }

        public boolean met() { return met; }

    }

    public static interface SourceBuilder extends ToXContent {

        public String type();

    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.simple;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 */
public class AlwaysTrueCondition extends Condition<Condition.Result> {

    public static final String TYPE = "always_true";

    public static final Result RESULT = new Result(TYPE, true) {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    };

    public AlwaysTrueCondition(ESLogger logger) {
        super(logger);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {
        return RESULT;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AlwaysTrueCondition;
    }

    public static class Parser extends AbstractComponent implements Condition.Parser<Result, AlwaysTrueCondition> {

        @Inject
        public Parser(Settings settings) {
            super(settings);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public AlwaysTrueCondition parse(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT){
                throw new ConditionException("unable to parse [" + TYPE + "] condition. expected a start object token, found [" + parser.currentToken() + "]");
            }
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new ConditionException("unable to parse [" + TYPE + "] condition. expected an empty object, but found an object with [" + token + "]");
            }
            return new AlwaysTrueCondition(logger);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT){
                throw new ConditionException("unable to parse [" + TYPE + "] condition result. expected a start object token, found [" + parser.currentToken() + "]");
            }
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new ConditionException("unable to parse [" + TYPE + "] condition. expected an empty object, but found an object with [" + token + "]");
            }
            return RESULT;
        }
    }

    public static class SourceBuilder implements Condition.SourceBuilder {

        public static final SourceBuilder INSTANCE = new SourceBuilder();

        private SourceBuilder() {
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }
    }
}

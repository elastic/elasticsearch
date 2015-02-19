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
public class AlwaysFalseCondition extends Condition<Condition.Result> {

    public static final String TYPE = "always_false";
    public static final Result RESULT = new Result(TYPE, false) {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

    };

    public AlwaysFalseCondition(ESLogger logger) {
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

    public static class Parser extends AbstractComponent implements Condition.Parser<Result, AlwaysFalseCondition> {

        @Inject
        public Parser(Settings settings) {
            super(settings);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public AlwaysFalseCondition parse(XContentParser parser) throws IOException {
            return new AlwaysFalseCondition(logger);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT){
                throw new ConditionException("unable to parse [" + TYPE + "] condition result. expected a start object token, found [" + parser.currentToken() + "]");
            }
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.END_OBJECT) {
                throw new ConditionException("unable to parse [" + TYPE + "] condition result. expected an empty object, but found an object with [" + token + "]");
            }
            return RESULT;
        }
    }

}

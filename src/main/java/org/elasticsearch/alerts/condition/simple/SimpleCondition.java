/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.simple;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
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
 * A condition that is always met and returns a static/fixed payload
 */
public class SimpleCondition extends Condition<SimpleCondition.Result> {

    public static final String TYPE = "simple";

    private final Payload payload;

    public SimpleCondition(ESLogger logger, Payload payload) {
        super(logger);
        this.payload = payload;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {
        return new Result(payload);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return payload.toXContent(builder, params);
    }

    public static class Result extends Condition.Result {

        public Result(Payload payload) {
            super(TYPE, true, payload);
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public static class Parser extends AbstractComponent implements Condition.Parser<Result, SimpleCondition> {

        @Inject
        public Parser(Settings settings) {
            super(settings);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public SimpleCondition parse(XContentParser parser) throws IOException {
            return new SimpleCondition(logger, new Payload.XContent(parser));
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            String currentFieldName = null;
            XContentParser.Token token;
            Payload payload = null;
            boolean met = false;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Condition.Result.MET_FIELD.match(currentFieldName)) {
                            met = parser.booleanValue();
                        } else {
                            throw new ConditionException("unable to parse simple condition result. unexpected field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ConditionException("unable to parse simple condition result. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Condition.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.XContent(parser);
                    } else {
                        throw new ConditionException("unable to parse simple condition result. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ConditionException("unable to parse simple condition result. unexpected token [" + token + "]");
                }
            }

            if (!met) {
                throw new ConditionException("unable to parse simple condition result. simple condition always matches, yet [met] field is either missing or set to [false]");
            }

            return new Result(payload);
        }
    }
}

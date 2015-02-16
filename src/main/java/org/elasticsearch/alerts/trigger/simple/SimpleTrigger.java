/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger.simple;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.alerts.trigger.TriggerException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * A trigger that always triggered and returns a static/fixed data
 */
public class SimpleTrigger extends Trigger<SimpleTrigger.Result> {

    public static final String TYPE = "simple";

    private final Payload payload;

    public SimpleTrigger(ESLogger logger, Payload payload) {
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

    public static class Result extends Trigger.Result {

        public Result(Payload payload) {
            super(TYPE, true, payload);
        }

        @Override
        protected XContentBuilder toXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public static class Parser extends AbstractComponent implements Trigger.Parser<SimpleTrigger> {

        @Inject
        public Parser(Settings settings) {
            super(settings);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public SimpleTrigger parse(XContentParser parser) throws IOException {
            return new SimpleTrigger(logger, new Payload.XContent(parser));
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            String currentFieldName = null;
            XContentParser.Token token;
            String type = null;
            boolean triggered = false;
            Payload payload = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (Trigger.Result.TYPE_FIELD.match(currentFieldName)) {
                        type = parser.text();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Trigger.Result.TRIGGERED_FIELD.match(currentFieldName)) {
                            triggered = parser.booleanValue();
                        } else {
                            throw new TriggerException("unable to parse trigger result. unexpected field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new TriggerException("unable to parse trigger result. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Trigger.Result.PAYLOAD_FIELD.match(currentFieldName)) {
                        payload = new Payload.Simple(parser.map()); ///@TODO FIXME
                    } else {
                        throw new TriggerException("unable to parse trigger result. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new TriggerException("unable to parse trigger result. unexpected token [" + token + "]");
                }
            }
            return new Result(payload);
        }
    }
}

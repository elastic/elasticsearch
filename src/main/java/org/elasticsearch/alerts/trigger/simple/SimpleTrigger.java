/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger.simple;

import org.elasticsearch.alerts.AlertContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.trigger.Trigger;
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
    public Result execute(AlertContext ctx) throws IOException {
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
    }
}

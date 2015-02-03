/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.trigger.simple;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.trigger.Trigger;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.XContentSettingsLoader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * A trigger that always triggered and returns a static/fixed data
 */
public class SimpleTrigger extends Trigger<SimpleTrigger.Result> {

    public static final String TYPE = "simple";

    private final Map<String, Object> data;

    public SimpleTrigger(ESLogger logger, Settings settings) {
        super(logger);
        this.data = settings.getAsStructuredMap();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(Alert alert, DateTime scheduledFireTime, DateTime fireTime) throws IOException {
        return new Result(data);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(data);
    }

    public static class Result extends Trigger.Result {

        public Result(Map<String, Object> data) {
            super(TYPE, true, data);
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
            Map<String, String> data = new SettingsLoader(parser.contentType()).load(parser);
            return new SimpleTrigger(logger, ImmutableSettings.builder().put(data).build());
        }
    }

    static class SettingsLoader extends XContentSettingsLoader {

        private final XContentType type;

        public SettingsLoader(XContentType type) {
            this.type = type;
        }

        @Override
        public XContentType contentType() {
            return type;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler.schedule;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ScheduleRegistry {

    private final ImmutableMap<String, Schedule.Parser> parsers;

    @Inject
    public ScheduleRegistry(Map<String, Schedule.Parser> parsers) {
        this.parsers = ImmutableMap.copyOf(parsers);
    }

    public Schedule parse(XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Schedule schedule = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type != null) {
                schedule = parse(type, parser);
            } else {
                throw new AlertsSettingsException("could not parse schedule. expected a schedule type field, but found [" + token + "]");
            }
        }
        if (schedule == null) {
            throw new AlertsSettingsException("could not parse schedule. expected a schedule type field, but no fields were found");
        }
        return schedule;
    }

    public Schedule parse(String type, XContentParser parser) throws IOException {
        Schedule.Parser scheduleParser = parsers.get(type);
        if (scheduleParser == null) {
            throw new AlertsSettingsException("could not parse schedule. unknown schedule type [" + type + "]");
        }
        return scheduleParser.parse(parser);
    }
}

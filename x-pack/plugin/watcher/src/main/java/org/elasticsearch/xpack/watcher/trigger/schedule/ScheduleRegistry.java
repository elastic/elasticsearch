/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.trigger.schedule.support.TimezoneUtils;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScheduleRegistry {
    private final Map<String, Schedule.Parser<? extends Schedule>> parsers = new HashMap<>();

    public ScheduleRegistry(Set<Schedule.Parser<? extends Schedule>> parsers) {
        parsers.forEach(parser -> this.parsers.put(parser.type(), parser));
    }

    public Set<String> types() {
        return parsers.keySet();
    }

    public Schedule parse(String context, XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Schedule schedule = null;
        ZoneId timeZone = null; // Default to UTC
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                var fieldName = parser.currentName();
                if (fieldName.equals(ScheduleTrigger.TIMEZONE_FIELD)) {
                    timeZone = parseTimezone(parser);
                } else {
                    type = parser.currentName();
                }
            } else if (type != null) {
                schedule = parse(context, type, parser);
            } else {
                throw new ElasticsearchParseException(
                    "could not parse schedule. expected a schedule type field, but found [{}] instead",
                    token
                );
            }
        }
        if (schedule == null) {
            throw new ElasticsearchParseException("could not parse schedule. expected a schedule type field, but no fields were found");
        }

        if (timeZone != null && schedule instanceof CronnableSchedule cronnableSchedule) {
            cronnableSchedule.setTimeZone(timeZone);
        } else if (timeZone != null) {
            throw new ElasticsearchParseException(
                "could not parse schedule. Timezone is not supported for schedule type [{}]",
                schedule.type()
            );
        }

        return schedule;
    }

    private static ZoneId parseTimezone(XContentParser parser) throws IOException {
        ZoneId timeZone;
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            String text = parser.text();
            try {
                timeZone = TimezoneUtils.parse(text);
            } catch (DateTimeException e) {
                throw new ElasticsearchParseException("could not parse schedule. invalid timezone [{}]", e, text);
            }
        } else {
            throw new ElasticsearchParseException(
                "could not parse schedule. expected a string value for timezone, but found [{}] instead",
                token
            );
        }
        return timeZone;
    }

    public Schedule parse(String context, String type, XContentParser parser) throws IOException {
        Schedule.Parser<?> scheduleParser = parsers.get(type);
        if (scheduleParser == null) {
            throw new ElasticsearchParseException("could not parse schedule for [{}]. unknown schedule type [{}]", context, type);
        }
        return scheduleParser.parse(parser);
    }
}

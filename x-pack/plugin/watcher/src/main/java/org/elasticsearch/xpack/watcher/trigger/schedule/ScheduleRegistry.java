/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ScheduleRegistry {
    private final Map<String, Schedule.Parser> parsers = new HashMap<>();

    public ScheduleRegistry(Set<Schedule.Parser> parsers) {
        parsers.stream().forEach(parser -> this.parsers.put(parser.type(), parser));
    }

    public Set<String> types() {
        return parsers.keySet();
    }

    public Schedule parse(String context, XContentParser parser) throws IOException {
        String type = null;
        XContentParser.Token token;
        Schedule schedule = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                type = parser.currentName();
            } else if (type != null) {
                schedule = parse(context, type, parser);
            } else {
                throw new ElasticsearchParseException("could not parse schedule. expected a schedule type field, but found [{}] instead",
                        token);
            }
        }
        if (schedule == null) {
            throw new ElasticsearchParseException("could not parse schedule. expected a schedule type field, but no fields were found");
        }
        return schedule;
    }

    public Schedule parse(String context, String type, XContentParser parser) throws IOException {
        Schedule.Parser scheduleParser = parsers.get(type);
        if (scheduleParser == null) {
            throw new ElasticsearchParseException("could not parse schedule for [{}]. unknown schedule type [{}]", context, type);
        }
        return scheduleParser.parse(parser);
    }
}

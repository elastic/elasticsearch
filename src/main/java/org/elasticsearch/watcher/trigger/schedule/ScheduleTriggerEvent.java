/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.WatcherDateUtils;
import org.elasticsearch.watcher.trigger.TriggerEvent;

import java.io.IOException;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;

/**
 *
 */
public class ScheduleTriggerEvent extends TriggerEvent {

    public static final ParseField SCHEDULED_TIME_FIELD = new ParseField("scheduled_time");

    private final DateTime scheduledTime;

    public ScheduleTriggerEvent(DateTime triggeredTime, DateTime scheduledTime) {
        this(null, triggeredTime, scheduledTime);
    }

    public ScheduleTriggerEvent(String jobName, DateTime triggeredTime, DateTime scheduledTime) {
        super(jobName, triggeredTime);
        this.scheduledTime = scheduledTime;
        data.put(SCHEDULED_TIME_FIELD.getPreferredName(), scheduledTime);
    }

    @Override
    public String type() {
        return ScheduleTrigger.TYPE;
    }

    public DateTime scheduledTime() {
        return scheduledTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(TRIGGERED_TIME_FIELD.getPreferredName(), WatcherDateUtils.formatDate(triggeredTime))
                .field(SCHEDULED_TIME_FIELD.getPreferredName(), WatcherDateUtils.formatDate(scheduledTime))
                .endObject();
    }

    public static ScheduleTriggerEvent parse(String context, XContentParser parser) throws IOException {
        DateTime triggeredTime = null;
        DateTime scheduledTime = null;

        String currentFieldName = null;
        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if (token == XContentParser.Token.VALUE_STRING) {
                    if (TRIGGERED_TIME_FIELD.match(currentFieldName)) {
                        triggeredTime = WatcherDateUtils.parseDate(parser.text(), UTC);
                    } else if (SCHEDULED_TIME_FIELD.match(currentFieldName)) {
                        scheduledTime = WatcherDateUtils.parseDate(parser.text(), UTC);
                    } else {
                        throw new ParseException("could not parse trigger event for [" + context + "]. unknown string value field [" + currentFieldName + "]");
                    }
                } else {
                    throw new ParseException("could not parse trigger event for [" + context + "]. unexpected token [" + token + "]");
                }
            }
        }

        // should never be, it's fully controlled internally (not coming from the user)
        assert triggeredTime != null && scheduledTime != null;
        return new ScheduleTriggerEvent(triggeredTime, scheduledTime);
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg) {
            super(msg);
        }

        public ParseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}

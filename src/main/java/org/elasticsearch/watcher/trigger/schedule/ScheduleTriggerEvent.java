/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.ParseField;
import org.joda.time.DateTime;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class ScheduleTriggerEvent extends TriggerEvent {

    private final DateTime scheduledTime;

    public ScheduleTriggerEvent(DateTime triggeredTime, DateTime scheduledTime) {
        this(null, triggeredTime, scheduledTime);
    }

    public ScheduleTriggerEvent(String jobName, DateTime triggeredTime, DateTime scheduledTime) {
        super(jobName, triggeredTime);
        this.scheduledTime = scheduledTime;
        data.put(Field.SCHEDULED_TIME.getPreferredName(), scheduledTime);
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
        builder.startObject();
        WatcherDateTimeUtils.writeDate(Field.TRIGGERED_TIME.getPreferredName(), builder, triggeredTime);
        WatcherDateTimeUtils.writeDate(Field.SCHEDULED_TIME.getPreferredName(), builder, scheduledTime);
        return builder.endObject();
    }

    @Override
    public void recordDataXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ScheduleTrigger.TYPE);
        WatcherDateTimeUtils.writeDate(Field.SCHEDULED_TIME.getPreferredName(), builder, scheduledTime);
        builder.endObject();
    }

    public static ScheduleTriggerEvent parse(XContentParser parser, String watchId, String context, Clock clock) throws IOException {
        DateTime triggeredTime = null;
        DateTime scheduledTime = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TRIGGERED_TIME.match(currentFieldName)) {
                try {
                    triggeredTime = WatcherDateTimeUtils.parseDateMath(currentFieldName, parser, DateTimeZone.UTC, clock);
                } catch (WatcherDateTimeUtils.ParseException pe) {
                    //Failed to parse as a date try datemath parsing
                    throw new ParseException("could not parse [{}] trigger event for [{}] for watch [{}]. failed to parse date field [{}]", pe, ScheduleTriggerEngine.TYPE, context, watchId, currentFieldName);
                }
            }  else if (Field.SCHEDULED_TIME.match(currentFieldName)) {
                try {
                    scheduledTime = WatcherDateTimeUtils.parseDateMath(currentFieldName, parser, DateTimeZone.UTC, clock);
                } catch (WatcherDateTimeUtils.ParseException pe) {
                    throw new ParseException("could not parse [{}] trigger event for [{}] for watch [{}]. failed to parse date field [{}]", pe, ScheduleTriggerEngine.TYPE, context, watchId, currentFieldName);
                }
            }else {
                throw new ParseException("could not parse trigger event for [{}] for watch [{}]. unexpected token [{}]", context, watchId, token);
            }
        }

        // should never be, it's fully controlled internally (not coming from the user)
        assert triggeredTime != null && scheduledTime != null;
        return new ScheduleTriggerEvent(triggeredTime, scheduledTime);
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg, Object... args) {
            super(msg, args);
        }

        public ParseException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    interface Field extends TriggerEvent.Field {
        ParseField SCHEDULED_TIME = new ParseField("scheduled_time");
    }
}

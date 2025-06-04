/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class ScheduleTriggerEvent extends TriggerEvent {

    private final ZonedDateTime scheduledTime;

    public ScheduleTriggerEvent(ZonedDateTime triggeredTime, ZonedDateTime scheduledTime) {
        this(null, triggeredTime, scheduledTime);
    }

    public ScheduleTriggerEvent(String jobName, ZonedDateTime triggeredTime, ZonedDateTime scheduledTime) {
        super(jobName, triggeredTime);
        this.scheduledTime = scheduledTime;
        data.put(Field.SCHEDULED_TIME.getPreferredName(), ZonedDateTime.ofInstant(scheduledTime.toInstant(), ZoneOffset.UTC));
    }

    @Override
    public String type() {
        return ScheduleTrigger.TYPE;
    }

    public ZonedDateTime scheduledTime() {
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
        ZonedDateTime triggeredTime = null;
        ZonedDateTime scheduledTime = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TRIGGERED_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    triggeredTime = WatcherDateTimeUtils.parseDateMath(currentFieldName, parser, ZoneOffset.UTC, clock);
                } catch (ElasticsearchParseException pe) {
                    // Failed to parse as a date try datemath parsing
                    throw new ElasticsearchParseException(
                        "could not parse [{}] trigger event for [{}] for watch [{}]. failed to parse " + "date field [{}]",
                        pe,
                        ScheduleTriggerEngine.TYPE,
                        context,
                        watchId,
                        currentFieldName
                    );
                }
            } else if (Field.SCHEDULED_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                try {
                    scheduledTime = WatcherDateTimeUtils.parseDateMath(currentFieldName, parser, ZoneOffset.UTC, clock);
                } catch (ElasticsearchParseException pe) {
                    throw new ElasticsearchParseException(
                        "could not parse [{}] trigger event for [{}] for watch [{}]. failed to parse " + "date field [{}]",
                        pe,
                        ScheduleTriggerEngine.TYPE,
                        context,
                        watchId,
                        currentFieldName
                    );
                }
            } else {
                throw new ElasticsearchParseException(
                    "could not parse trigger event for [{}] for watch [{}]. unexpected token [{}]",
                    context,
                    watchId,
                    token
                );
            }
        }

        // should never be, it's fully controlled internally (not coming from the user)
        assert triggeredTime != null && scheduledTime != null;
        return new ScheduleTriggerEvent(triggeredTime, scheduledTime);
    }

    interface Field extends TriggerEvent.Field {
        ParseField SCHEDULED_TIME = new ParseField("scheduled_time");
    }
}

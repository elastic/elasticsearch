/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.manual;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.WatcherDateUtils;
import org.elasticsearch.watcher.trigger.TriggerEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.joda.time.DateTimeZone.UTC;
/**
 */
public class ManualTriggerEvent extends TriggerEvent {

    private final Map<String, Object> triggerData;

    public ManualTriggerEvent(DateTime triggeredTime, Map<String, Object> triggerData) {
        this(null, triggeredTime, triggerData);
    }

    public ManualTriggerEvent(String jobName, DateTime triggeredTime, Map<String, Object> triggerData) {
        super(jobName, triggeredTime);
        data.putAll(triggerData);
        this.triggerData = triggerData;
    }

    @Override
    public String type() {
        return ManualTriggerEngine.TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        WatcherDateUtils.writeDate(Field.TRIGGERED_TIME.getPreferredName(), builder, triggeredTime);
        builder.field(Field.TRIGGER_DATA.getPreferredName(), triggerData);
        return builder.endObject();
    }

    public static ManualTriggerEvent parse(String context, XContentParser parser) throws IOException {
        DateTime triggeredTime = null;
        Map<String, Object> triggerData = new HashMap<>();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TRIGGERED_TIME.match(currentFieldName)) {
                try {
                    triggeredTime = WatcherDateUtils.parseDate(currentFieldName, parser, UTC);
                } catch (WatcherDateUtils.ParseException pe) {
                    throw new ParseException("could not parse [{}] trigger event for [{}]. failed to parse date field [{}]", pe, ManualTriggerEngine.TYPE, context, currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Field.TRIGGER_DATA.match(currentFieldName)) {
                    triggerData = parser.map();
                } else {
                    throw new ParseException("could not parse trigger event for [{}]. unexpected object value field [{}]", context, currentFieldName);
                }
            } else {
                throw new ParseException("could not parse trigger event for [{}]. unexpected token [{}]", context, token);
            }
        }

        // should never be, it's fully controlled internally (not coming from the user)
        assert triggeredTime != null;
        return new ManualTriggerEvent(triggeredTime, triggerData);
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
        ParseField TRIGGER_DATA = new ParseField("trigger_data");
    }

}

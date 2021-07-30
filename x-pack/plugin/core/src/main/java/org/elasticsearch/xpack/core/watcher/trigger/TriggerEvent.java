/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.trigger;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public abstract class TriggerEvent implements ToXContentObject {

    private final String jobName;
    protected final ZonedDateTime triggeredTime;
    protected final Map<String, Object> data;

    public TriggerEvent(String jobName, ZonedDateTime triggeredTime) {
        this.jobName = jobName;
        this.triggeredTime = triggeredTime;
        this.data = new HashMap<>();
        data.put(Field.TRIGGERED_TIME.getPreferredName(),
            new JodaCompatibleZonedDateTime(triggeredTime.toInstant(), ZoneOffset.UTC));
    }

    public String jobName() {
        return jobName;
    }

    public abstract String type();

    public ZonedDateTime triggeredTime() {
        return triggeredTime;
    }

    public final Map<String, Object> data() {
        return data;
    }

    @Override
    public String toString() {
        return new StringBuilder("[")
                .append("name=[").append(jobName).append("],")
                .append("triggered_time=[").append(triggeredTime).append("],")
                .append("data=[").append(data).append("]")
                .append("]")
                .toString();
    }

    public void recordXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.TYPE.getPreferredName(), type());
        WatcherDateTimeUtils.writeDate(Field.TRIGGERED_TIME.getPreferredName(), builder, triggeredTime);
        recordDataXContent(builder, params);
        builder.endObject();
    }

    public abstract void recordDataXContent(XContentBuilder builder, Params params) throws IOException;

    protected interface Field {
        ParseField TYPE = new ParseField("type");
        ParseField TRIGGERED_TIME = new ParseField("triggered_time");
    }

}

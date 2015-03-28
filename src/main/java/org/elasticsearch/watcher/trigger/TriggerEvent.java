/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.support.WatcherDateUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class TriggerEvent implements ToXContent {

    public static final ParseField TRIGGERED_TIME_FIELD = new ParseField("triggered_time");

    protected final DateTime triggeredTime;
    protected final Map<String, Object> data;

    public TriggerEvent(DateTime triggeredTime) {
        this.triggeredTime = triggeredTime;
        this.data = new HashMap<>();
        data.put(TRIGGERED_TIME_FIELD.getPreferredName(), triggeredTime);
    }

    public abstract String type();

    public DateTime triggeredTime() {
        return triggeredTime;
    }

    public final Map<String, Object> data() {
        return data;
    }

}

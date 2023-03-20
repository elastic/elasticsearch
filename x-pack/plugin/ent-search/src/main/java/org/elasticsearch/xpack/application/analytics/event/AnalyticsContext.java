/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsContext implements Writeable {
    private final AnalyticsCollection eventCollection;
    private final long eventTime;

    private final AnalyticsEventType eventType;

    public AnalyticsContext(AnalyticsCollection eventCollection, AnalyticsEventType eventType, long eventTime) {
        this.eventCollection = eventCollection;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public AnalyticsContext(StreamInput in) throws IOException {
        this.eventCollection = new AnalyticsCollection(in);
        this.eventType = in.readEnum(AnalyticsEventType.class);
        this.eventTime = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.eventCollection.writeTo(out);
        out.writeEnum(eventType);
        out.writeLong(eventTime);
    }

    public long eventTime() {
        return eventTime;
    }

    public AnalyticsEventType eventType() {
        return eventType;
    }

    public AnalyticsCollection eventCollection() {
        return eventCollection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsContext that = (AnalyticsContext) o;
        return eventTime == that.eventTime && eventType == that.eventType && Objects.equals(eventCollection, that.eventCollection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventCollection, eventType, eventTime);
    }
}

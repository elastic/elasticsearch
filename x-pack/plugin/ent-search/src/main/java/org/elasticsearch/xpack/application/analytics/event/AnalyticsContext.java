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

/**
 * Analytics context is extracted from the HTTP POST request URL and allow to access basic event meta-data (collection name, event type,...)
 * without parsing the payload.
 */
public class AnalyticsContext implements Writeable {
    private final String eventCollectionName;
    private final long eventTime;
    private final AnalyticsEvent.Type eventType;

    public AnalyticsContext(String eventCollectionName, AnalyticsEvent.Type eventType, long eventTime) {
        this.eventCollectionName = Objects.requireNonNull(eventCollectionName);
        this.eventType = Objects.requireNonNull(eventType);
        this.eventTime = eventTime;
    }

    public AnalyticsContext(AnalyticsCollection eventCollection, AnalyticsEvent.Type eventType, long eventTime) {
        this(eventCollection.getName(), eventType, eventTime);
    }

    public AnalyticsContext(StreamInput in) throws IOException {
        this(in.readString(), in.readEnum(AnalyticsEvent.Type.class), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(eventCollectionName);
        out.writeEnum(eventType);
        out.writeLong(eventTime);
    }

    public long eventTime() {
        return eventTime;
    }

    public AnalyticsEvent.Type eventType() {
        return eventType;
    }

    public String eventCollectionName() {
        return eventCollectionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsContext that = (AnalyticsContext) o;
        return eventTime == that.eventTime && eventType == that.eventType && eventCollectionName.equals(that.eventCollectionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventCollectionName, eventType, eventTime);
    }
}

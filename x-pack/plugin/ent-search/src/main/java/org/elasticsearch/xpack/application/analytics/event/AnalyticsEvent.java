/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * This class represents Analytics events object meant to be emitted to the event queue.
 * Subclasses are implementing the different event types.
 */
public abstract class AnalyticsEvent implements Writeable, ToXContentObject {
    /**
     * Analytics context. Used to carry information to parsers.
     */
    public interface Context {
        long eventTime();

        Type eventType();

        String eventCollectionName();
    }

    /**
     * Analytics event types.
     */
    public enum Type {
        PAGEVIEW("pageview"),
        SEARCH("search"),
        INTERACTION("interaction");

        private final String typeName;

        Type(String typeName) {
            this.typeName = typeName;
        }

        @Override
        public String toString() {
            return typeName.toLowerCase(Locale.ROOT);
        }
    }

    private final String eventCollectionName;

    private final long eventTime;

    private final AnalyticsEventSessionData sessionData;

    private final AnalyticsEventUserData userData;

    protected AnalyticsEvent(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData sessionData,
        AnalyticsEventUserData userData
    ) {
        this.eventCollectionName = Strings.requireNonBlank(eventCollectionName, "eventCollectionName");
        this.eventTime = eventTime;
        this.sessionData = Objects.requireNonNull(sessionData, AnalyticsEventSessionData.SESSION_FIELD.getPreferredName());
        this.userData = Objects.requireNonNull(userData, AnalyticsEventUserData.USER_FIELD.getPreferredName());
    }

    protected AnalyticsEvent(Context analyticsContext, AnalyticsEventSessionData sessionData, AnalyticsEventUserData userData) {
        this(analyticsContext.eventCollectionName(), analyticsContext.eventTime(), sessionData, userData);
    }

    protected AnalyticsEvent(StreamInput in) throws IOException {
        this(in.readString(), in.readLong(), new AnalyticsEventSessionData(in), new AnalyticsEventUserData(in));
    }

    public abstract Type eventType();

    public String eventCollectionName() {
        return eventCollectionName;
    }

    public long eventTime() {
        return eventTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(eventCollectionName);
        out.writeLong(eventTime);
        sessionData.writeTo(out);
        userData.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("@timestamp", eventTime());

            builder.startObject("event");
            {
                builder.field("action", eventType());
            }
            builder.endObject();

            builder.startObject("data_stream");
            {
                builder.field("type", "behavioral_analytics");
                builder.field("dataset", "events");
                builder.field("namespace", eventCollectionName());

            }
            builder.endObject();

            // Render additional fields from the event payload (session, user, page, ...)
            addCustomFieldToXContent(builder, params);

        }
        builder.endObject();

        return builder;
    }

    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEvent that = (AnalyticsEvent) o;
        return eventCollectionName.equals(that.eventCollectionName)
            && eventTime == that.eventTime
            && Objects.equals(sessionData, that.sessionData)
            && Objects.equals(userData, that.userData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventCollectionName, eventTime, sessionData, userData);
    }
}

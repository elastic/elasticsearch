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
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public abstract class AnalyticsEvent implements Writeable, ToXContentObject {

    public static final ParseField SESSION_FIELD = new ParseField("session");

    public static final ParseField USER_FIELD = new ParseField("user");

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

    public static ConstructingObjectParser<? extends AnalyticsEvent, AnalyticsCollection> getParser(Type eventType) {
        if (eventType == Type.PAGEVIEW) return PageViewAnalyticsEvent.PARSER;

        if (eventType == Type.SEARCH) return SearchAnalyticsEvent.PARSER;

        if (eventType == Type.INTERACTION) return InteractionAnalyticsEvent.PARSER;

        throw new IllegalArgumentException(Strings.format("%s is not a supported event type", eventType));
    }

    private final AnalyticsCollection analyticsCollection;

    private final SessionData sessionData;

    private final UserData userData;

    public AnalyticsEvent(AnalyticsCollection analyticsCollection, SessionData sessionData, UserData userData) {
        this.analyticsCollection = analyticsCollection;
        this.sessionData = sessionData;
        this.userData = userData;
    }

    public AnalyticsEvent(StreamInput in) throws IOException {
        this(new AnalyticsCollection(in), new SessionData(in), new UserData(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        analyticsCollection.writeTo(out);
        sessionData.writeTo(out);
        userData.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("@timestamp", getTimestamp());

            builder.startObject("event").field("action", getType()).endObject();

            builder.startObject("data_stream");
            {
                builder.field("type", "behavioral_analytics");
                builder.field("dataset", "events");
                builder.field("namespace", analyticsCollection.getName());

            }
            builder.endObject();

            builder.field(SESSION_FIELD.getPreferredName(), sessionData);
            builder.field(USER_FIELD.getPreferredName(), sessionData);
            addFieldsToXContent(builder, params);
        }
        builder.endObject();

        return builder;
    }

    public void addFieldsToXContent(XContentBuilder builder, Params params) throws IOException {

    }

    protected abstract Type getType();

    private String getTimestamp() {
        ZonedDateTime currentTime = ZonedDateTime.now();

        return currentTime.format(DateTimeFormatter.ISO_INSTANT);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventSearch extends AnalyticsEvent {
    private static final ConstructingObjectParser<AnalyticsEventSearch, Context> PARSER = new ConstructingObjectParser<>(
        "search_event",
        false,
        (p, c) -> new AnalyticsEventSearch(
            c.eventCollectionName(),
            c.eventTime(),
            (AnalyticsEventSessionData) p[0],
            (AnalyticsEventUserData) p[1],
            (AnalyticsEventSearchData) p[2]
        )
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            AnalyticsEventSessionData::fromXContent,
            AnalyticsEventSessionData.SESSION_FIELD
        );
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            AnalyticsEventUserData::fromXContent,
            AnalyticsEventUserData.USER_FIELD
        );
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            AnalyticsEventSearchData::fromXContent,
            AnalyticsEventSearchData.SEARCH_FIELD
        );
    }

    private final AnalyticsEventSearchData search;

    public AnalyticsEventSearch(StreamInput in) throws IOException {
        super(in);
        this.search = new AnalyticsEventSearchData(in);
    }

    public AnalyticsEventSearch(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData sessionData,
        AnalyticsEventUserData userData,
        AnalyticsEventSearchData search
    ) {
        super(eventCollectionName, eventTime, sessionData, userData);
        this.search = search;
    }

    @Override
    public Type eventType() {
        return Type.SEARCH;
    }

    public static AnalyticsEventSearch fromXContent(XContentParser parser, Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public AnalyticsEventSearchData search() {
        return search;
    }

    @Override
    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(AnalyticsEventSearchData.SEARCH_FIELD.getPreferredName(), search());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        search.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearch that = (AnalyticsEventSearch) o;
        return super.equals(that) && Objects.equals(search, that.search);
    }

    @Override
    public int hashCode() {
        int parentHash = super.hashCode();
        return 31 * parentHash + Objects.hash(search);
    }
}

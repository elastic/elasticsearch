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

public class AnalyticsEventPageView extends AnalyticsEvent {
    private static final ConstructingObjectParser<AnalyticsEventPageView, Context> PARSER = new ConstructingObjectParser<>(
        AnalyticsEventPageData.PAGE_FIELD.getPreferredName(),
        false,
        (p, c) -> new AnalyticsEventPageView(
            c,
            (AnalyticsEventSessionData) p[0],
            (AnalyticsEventUserData) p[1],
            (AnalyticsEventPageData) p[2]
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
            AnalyticsEventPageData::fromXContent,
            AnalyticsEventPageData.PAGE_FIELD
        );
    }

    private final AnalyticsEventPageData pageData;

    public AnalyticsEventPageView(
        Context context,
        AnalyticsEventSessionData sessionData,
        AnalyticsEventUserData userData,
        AnalyticsEventPageData pageData
    ) {
        super(context, sessionData, userData);
        this.pageData = Objects.requireNonNull(pageData, AnalyticsEventPageData.PAGE_FIELD.getPreferredName());
    }

    public AnalyticsEventPageView(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData sessionData,
        AnalyticsEventUserData userData,
        AnalyticsEventPageData pageData
    ) {
        super(eventCollectionName, eventTime, sessionData, userData);
        this.pageData = pageData;
    }

    public AnalyticsEventPageView(StreamInput in) throws IOException {
        super(in);
        this.pageData = new AnalyticsEventPageData(in);
    }

    @Override
    public Type eventType() {
        return Type.PAGEVIEW;
    }

    public static AnalyticsEvent fromXContent(XContentParser parser, Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public AnalyticsEventPageData pageData() {
        return pageData;
    }

    @Override
    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(AnalyticsEventPageData.PAGE_FIELD.getPreferredName(), pageData());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        pageData.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventPageView that = (AnalyticsEventPageView) o;
        return super.equals(that) && Objects.equals(pageData, that.pageData);
    }

    @Override
    public int hashCode() {
        int parentHash = super.hashCode();
        return 31 * parentHash + Objects.hash(pageData);
    }
}

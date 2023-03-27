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
        "pageview_event",
        false,
        (p, c) -> new AnalyticsEventPageView(
            c.eventCollectionName(),
            c.eventTime(),
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

    private final AnalyticsEventPageData page;

    public AnalyticsEventPageView(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData session,
        AnalyticsEventUserData user,
        AnalyticsEventPageData page
    ) {
        super(eventCollectionName, eventTime, session, user);
        this.page = page;
    }

    public AnalyticsEventPageView(StreamInput in) throws IOException {
        super(in);
        this.page = new AnalyticsEventPageData(in);
    }

    @Override
    public Type eventType() {
        return Type.PAGEVIEW;
    }

    public static AnalyticsEventPageView fromXContent(XContentParser parser, Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public AnalyticsEventPageData page() {
        return page;
    }

    @Override
    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(AnalyticsEventPageData.PAGE_FIELD.getPreferredName(), page());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        page.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventPageView that = (AnalyticsEventPageView) o;
        return super.equals(that) && Objects.equals(page, that.page);
    }

    @Override
    public int hashCode() {
        int parentHash = super.hashCode();
        return 31 * parentHash + Objects.hash(page);
    }
}

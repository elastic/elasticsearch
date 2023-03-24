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

public class AnalyticsEventSearchClick extends AnalyticsEvent {
    private static final ConstructingObjectParser<AnalyticsEventSearchClick, Context> PARSER = new ConstructingObjectParser<>(
        "interaction_event",
        false,
        (p, c) -> new AnalyticsEventSearchClick(
            c,
            (AnalyticsEventSessionData) p[0],
            (AnalyticsEventUserData) p[1],
            (AnalyticsEventPageData) p[2],
            (AnalyticsEventSearchData) p[3]
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
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            AnalyticsEventSearchData::fromXContent,
            AnalyticsEventSearchData.SEARCH_FIELD
        );
    }

    private final AnalyticsEventPageData page;
    private final AnalyticsEventSearchData search;

    public AnalyticsEventSearchClick(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData session,
        AnalyticsEventUserData user,
        AnalyticsEventPageData page,
        AnalyticsEventSearchData search
    ) {
        super(eventCollectionName, eventTime, session, user);
        this.page = page;
        this.search = search;
    }

    public AnalyticsEventSearchClick(
        Context context,
        AnalyticsEventSessionData session,
        AnalyticsEventUserData user,
        AnalyticsEventPageData page,
        AnalyticsEventSearchData search
    ) {
        super(context, session, user);
        this.page = Objects.requireNonNull(page, "page");
        this.search = Objects.requireNonNull(search, "search");
    }

    public AnalyticsEventSearchClick(StreamInput in) throws IOException {
        super(in);
        this.page = new AnalyticsEventPageData(in);
        this.search = new AnalyticsEventSearchData(in);
    }

    @Override
    public Type eventType() {
        return Type.SEARCH_CLICK;
    }

    public static AnalyticsEvent fromXContent(XContentParser parser, Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public AnalyticsEventPageData page() {
        return page;
    }

    public AnalyticsEventSearchData search() {
        return search;
    }

    @Override
    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(AnalyticsEventPageData.PAGE_FIELD.getPreferredName(), page());
        builder.field(AnalyticsEventSearchData.SEARCH_FIELD.getPreferredName(), search());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        page.writeTo(out);
        search.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearchClick that = (AnalyticsEventSearchClick) o;
        return super.equals(that) && Objects.equals(page, that.page) && Objects.equals(search, that.search);
    }

    @Override
    public int hashCode() {
        int parentHash = super.hashCode();
        return 31 * parentHash + Objects.hash(page, search);
    }
}

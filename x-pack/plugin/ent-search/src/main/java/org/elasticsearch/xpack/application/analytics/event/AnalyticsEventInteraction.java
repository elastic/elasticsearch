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

public class AnalyticsEventInteraction extends AnalyticsEvent {
    private static final ConstructingObjectParser<AnalyticsEventInteraction, Context> PARSER = new ConstructingObjectParser<>(
        "interaction_event",
        false,
        (p, c) -> new AnalyticsEventInteraction(
            c,
            (AnalyticsEventSessionData) p[0],
            (AnalyticsEventUserData) p[1],
            (AnalyticsEventInteractionData) p[2],
            (AnalyticsEventPageData) p[3],
            (AnalyticsEventSearchData) p[4]
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
            AnalyticsEventInteractionData::fromXContent,
            AnalyticsEventInteractionData.INTERACTION_FIELD
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

    private final AnalyticsEventInteractionData interactionData;
    private final AnalyticsEventPageData pageData;
    private final AnalyticsEventSearchData searchData;

    public AnalyticsEventInteraction(
        Context context,
        AnalyticsEventSessionData sessionData,
        AnalyticsEventUserData userData,
        AnalyticsEventInteractionData interactionData,
        AnalyticsEventPageData pageData,
        AnalyticsEventSearchData searchData
    ) {
        super(context, sessionData, userData);
        this.interactionData = Objects.requireNonNull(interactionData, "interactionData");
        this.pageData = Objects.requireNonNull(pageData, "pageData");
        this.searchData = Objects.requireNonNull(searchData, "searchData");
    }

    public AnalyticsEventInteraction(StreamInput in) throws IOException {
        super(in);
        this.interactionData = new AnalyticsEventInteractionData(in);
        this.pageData = new AnalyticsEventPageData(in);
        this.searchData = new AnalyticsEventSearchData(in);
    }

    public AnalyticsEventInteraction(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData sessionData,
        AnalyticsEventUserData userData,
        AnalyticsEventInteractionData interactionData,
        AnalyticsEventPageData pageData,
        AnalyticsEventSearchData searchData
    ) {
        super(eventCollectionName, eventTime, sessionData, userData);
        this.interactionData = interactionData;
        this.pageData = pageData;
        this.searchData = searchData;
    }

    @Override
    public Type eventType() {
        return Type.INTERACTION;
    }

    public static AnalyticsEvent fromXContent(XContentParser parser, Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public AnalyticsEventInteractionData interactionData() {
        return interactionData;
    }

    public AnalyticsEventPageData pageData() {
        return pageData;
    }

    public AnalyticsEventSearchData searchData() {
        return searchData;
    }

    @Override
    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(AnalyticsEventInteractionData.INTERACTION_FIELD.getPreferredName(), interactionData());
        builder.field(AnalyticsEventPageData.PAGE_FIELD.getPreferredName(), pageData());
        builder.field(AnalyticsEventSearchData.SEARCH_FIELD.getPreferredName(), searchData());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        interactionData.writeTo(out);
        pageData.writeTo(out);
        searchData.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventInteraction that = (AnalyticsEventInteraction) o;
        return super.equals(that)
            && Objects.equals(interactionData, that.interactionData)
            && Objects.equals(pageData, that.pageData)
            && Objects.equals(searchData, that.searchData);
    }

    @Override
    public int hashCode() {
        int parentHash = super.hashCode();
        return 31 * parentHash + Objects.hash(interactionData, pageData, searchData);
    }
}

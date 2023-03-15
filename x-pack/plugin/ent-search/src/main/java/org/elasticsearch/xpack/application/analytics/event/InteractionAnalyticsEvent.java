/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;

public class InteractionAnalyticsEvent extends AnalyticsEvent {

    public static final ParseField INTERACTION_FIELD = new ParseField("interaction");

    public static final ConstructingObjectParser<InteractionAnalyticsEvent, AnalyticsCollection> PARSER = new ConstructingObjectParser<>(
        "search_event",
        false,
        (params, collection) -> new InteractionAnalyticsEvent(
            collection,
            (SessionData) params[0],
            (UserData) params[1],
            (InteractionData) params[2],
            (PageData) params[3],
            (SearchData) params[4]
        )
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SessionData.PARSER.parse(p, null), SESSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> UserData.PARSER.parse(p, null), USER_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> InteractionData.PARSER.parse(p, null), INTERACTION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> PageData.PARSER.parse(p, null), PageViewAnalyticsEvent.PAGE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SearchData.PARSER.parse(p, null), SearchAnalyticsEvent.SEARCH_FIELD);
    }

    private final InteractionData interactionData;
    private final PageData pageData;
    private final SearchData searchData;

    public InteractionAnalyticsEvent(
        AnalyticsCollection analyticsCollection,
        SessionData sessionData,
        UserData userData,
        InteractionData interactionData,
        PageData pageData,
        SearchData searchData
    ) {
        super(analyticsCollection, sessionData, userData);
        this.interactionData = interactionData;
        this.pageData = pageData;
        this.searchData = searchData;
    }

    public InteractionAnalyticsEvent(StreamInput in) throws IOException {
        super(in);
        this.interactionData = new InteractionData(in);
        this.pageData = new PageData(in);
        this.searchData = new SearchData(in);
    }

    @Override
    public void addFieldsToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INTERACTION_FIELD.getPreferredName(), interactionData);
        builder.field(PageViewAnalyticsEvent.PAGE_FIELD.getPreferredName(), pageData);
        builder.field(SearchAnalyticsEvent.SEARCH_FIELD.getPreferredName(), searchData);
    }

    @Override
    protected Type getType() {
        return Type.INTERACTION;
    }

}

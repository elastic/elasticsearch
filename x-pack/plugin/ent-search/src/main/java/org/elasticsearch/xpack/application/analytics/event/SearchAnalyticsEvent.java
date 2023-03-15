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
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.application.analytics.AnalyticsCollection;

import java.io.IOException;
import java.util.Objects;

public class SearchAnalyticsEvent extends AnalyticsEvent {
    public static final ParseField SEARCH_FIELD = new ParseField("search");

    public static final ConstructingObjectParser<SearchAnalyticsEvent, AnalyticsCollection> PARSER = new ConstructingObjectParser<>(
        "search_event",
        false,
        (params, collection) -> new SearchAnalyticsEvent(collection, (SessionData) params[0], (UserData) params[1], (SearchData) params[2])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SessionData.PARSER.parse(p, null), SESSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> UserData.PARSER.parse(p, null), USER_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SearchData.PARSER.parse(p, null), SEARCH_FIELD);
    }

    private SearchData searchData;

    public SearchAnalyticsEvent(AnalyticsCollection analyticsCollection, SessionData sessionData, UserData userData, SearchData pageData) {
        super(analyticsCollection, sessionData, userData);
        this.searchData = pageData;
    }

    public SearchAnalyticsEvent(StreamInput in) throws IOException {
        super(in);
        this.searchData = new SearchData(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.searchData.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchAnalyticsEvent that = (SearchAnalyticsEvent) o;
        return super.equals(o) && Objects.equals(searchData, that.searchData);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(searchData);
    }

    @Override
    public void addFieldsToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SEARCH_FIELD.getPreferredName(), searchData);
    }

    @Override
    protected Type getType() {
        return Type.SEARCH;
    }
}

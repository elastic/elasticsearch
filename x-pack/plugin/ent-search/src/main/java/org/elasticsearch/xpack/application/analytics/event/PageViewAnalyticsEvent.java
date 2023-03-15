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

public class PageViewAnalyticsEvent extends AnalyticsEvent {

    public static final ParseField PAGE_FIELD = new ParseField("page");

    public static final ConstructingObjectParser<PageViewAnalyticsEvent, AnalyticsCollection> PARSER = new ConstructingObjectParser<>(
        "pageview_event",
        false,
        (params, collection) -> new PageViewAnalyticsEvent(collection, (SessionData) params[0], (UserData) params[1], (PageData) params[2])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SessionData.PARSER.parse(p, null), SESSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> UserData.PARSER.parse(p, null), USER_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> PageData.PARSER.parse(p, null), PAGE_FIELD);
    }

    private final PageData pageData;

    public PageViewAnalyticsEvent(AnalyticsCollection analyticsCollection, SessionData sessionData, UserData userData, PageData pageData) {
        super(analyticsCollection, sessionData, userData);
        this.pageData = pageData;
    }

    public PageViewAnalyticsEvent(StreamInput in) throws IOException {
        super(in);
        this.pageData = new PageData(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.pageData.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageViewAnalyticsEvent that = (PageViewAnalyticsEvent) o;
        return super.equals(o) && Objects.equals(pageData, that.pageData);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(pageData);
    }

    @Override
    public void addFieldsToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(PAGE_FIELD.getPreferredName(), pageData);
    }

    @Override
    protected Type getType() {
        return Type.PAGEVIEW;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_FIELD;

public class AnalyticsEventSearchClick extends AnalyticsEvent {
    private static final ConstructingObjectParser<AnalyticsEventSearchClick, Context> PARSER = new ConstructingObjectParser<>(
        "interaction_event",
        false,
        (p, c) -> {
            if (Objects.isNull(p[3]) && Objects.isNull(p[4])) {
                LoggerMessageFormat.format(
                    "Either [{}] or [{}] is required",
                    PAGE_FIELD.getPreferredName(),
                    DOCUMENT_FIELD.getPreferredName()
                );
            }

            return new AnalyticsEventSearchClick(
                c.eventCollectionName(),
                c.eventTime(),
                (AnalyticsEventSessionData) p[0],
                (AnalyticsEventUserData) p[1],
                (AnalyticsEventSearchData) p[2],
                (AnalyticsEventPageData) p[3],
                (AnalyticsEventDocumentData) p[4]
            );
        }
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), AnalyticsEventSessionData::fromXContent, SESSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), AnalyticsEventUserData::fromXContent, USER_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AnalyticsEventSearchData::fromXContent, SEARCH_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AnalyticsEventPageData::fromXContent, PAGE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), AnalyticsEventDocumentData::fromXContent, DOCUMENT_FIELD);
    }

    private final AnalyticsEventPageData page;

    private final AnalyticsEventSearchData search;

    private final AnalyticsEventDocumentData document;

    public AnalyticsEventSearchClick(
        String eventCollectionName,
        long eventTime,
        AnalyticsEventSessionData session,
        AnalyticsEventUserData user,
        AnalyticsEventSearchData search,
        @Nullable AnalyticsEventPageData page,
        @Nullable AnalyticsEventDocumentData document
    ) {
        super(eventCollectionName, eventTime, session, user);
        this.search = Objects.requireNonNull(search);
        this.page = page;
        this.document = document;
    }

    public AnalyticsEventSearchClick(StreamInput in) throws IOException {
        super(in);
        this.search = new AnalyticsEventSearchData(in);
        this.page = in.readOptionalWriteable(AnalyticsEventPageData::new);
        this.document = in.readOptionalWriteable(AnalyticsEventDocumentData::new);
    }

    public static AnalyticsEventSearchClick fromXContent(XContentParser parser, Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    @Override
    public Type eventType() {
        return Type.SEARCH_CLICK;
    }

    public AnalyticsEventSearchData search() {
        return search;
    }

    public AnalyticsEventPageData page() {
        return page;
    }

    public AnalyticsEventDocumentData document() {
        return document;
    }

    @Override
    protected void addCustomFieldToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SEARCH_FIELD.getPreferredName(), search());

        if (Objects.nonNull(page)) {
            builder.field(PAGE_FIELD.getPreferredName(), page());
        }

        if (Objects.nonNull(document)) {
            builder.field(DOCUMENT_FIELD.getPreferredName(), document());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        search.writeTo(out);
        out.writeOptionalWriteable(page);
        out.writeOptionalWriteable(document);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearchClick that = (AnalyticsEventSearchClick) o;
        return super.equals(that)
            && Objects.equals(search, that.search)
            && Objects.equals(page, that.page)
            && Objects.equals(document, that.document);
    }

    @Override
    public int hashCode() {
        int parentHash = super.hashCode();
        return 31 * parentHash + Objects.hash(search, page, document);
    }
}

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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGINATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.SORT_FIELD;

public class AnalyticsEventSearchData implements ToXContent, Writeable {
    public static ParseField SEARCH_FIELD = new ParseField("search");

    public static ParseField SEARCH_QUERY_FIELD = new ParseField("query");

    public static ParseField SEARCH_APPLICATION_FIELD = new ParseField("search_application");

    public static ParseField SEARCH_RESULTS_FIELD = new ParseField("results");

    private static final ConstructingObjectParser<AnalyticsEventSearchData, AnalyticsEvent.Context> PARSER = new ConstructingObjectParser<>(
        SEARCH_FIELD.getPreferredName(),
        false,
        (p, c) -> new AnalyticsEventSearchData(
            (String) p[0],
            (String) p[1],
            (AnalyticsEventSortOrderData) p[2],
            (AnalyticsEventPaginationData) p[3],
            (AnalyticsEventSearchResultData) p[4]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SEARCH_QUERY_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SEARCH_APPLICATION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), AnalyticsEventSortOrderData::fromXContent, SORT_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            AnalyticsEventPaginationData::fromXContent,
            PAGINATION_FIELD
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            AnalyticsEventSearchResultData::fromXContent,
            SEARCH_RESULTS_FIELD
        );
    }

    private final String query;

    private final String searchApplication;

    private final AnalyticsEventSortOrderData sortOrder;

    private final AnalyticsEventPaginationData page;

    private final AnalyticsEventSearchResultData results;

    public AnalyticsEventSearchData(
        String query,
        @Nullable String searchApplication,
        @Nullable AnalyticsEventSortOrderData sortOrder,
        @Nullable AnalyticsEventPaginationData page,
        @Nullable AnalyticsEventSearchResultData results
    ) {
        this.query = Objects.requireNonNull(query, "search query can't be null");
        this.searchApplication = searchApplication;
        this.sortOrder = sortOrder;
        this.page = page;
        this.results = results;
    }

    protected AnalyticsEventSearchData(String query) {
        this(query, null, null, null, null);
    }

    public AnalyticsEventSearchData(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readOptionalString(),
            in.readOptionalWriteable(AnalyticsEventSortOrderData::new),
            in.readOptionalWriteable(AnalyticsEventPaginationData::new),
            in.readOptionalWriteable(AnalyticsEventSearchResultData::new)
        );
    }

    public static AnalyticsEventSearchData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String query() {
        return query;
    }

    public String searchApplication() {
        return searchApplication;
    }

    public AnalyticsEventSortOrderData sortOrder() {
        return sortOrder;
    }

    public AnalyticsEventPaginationData page() {
        return page;
    }

    public AnalyticsEventSearchResultData results() {
        return results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SEARCH_QUERY_FIELD.getPreferredName(), query());

            if (Objects.nonNull(searchApplication)) {
                builder.field(SEARCH_APPLICATION_FIELD.getPreferredName(), searchApplication());
            }

            if (Objects.nonNull(sortOrder)) {
                builder.field(SORT_FIELD.getPreferredName(), sortOrder());
            }

            if (Objects.nonNull(page)) {
                builder.field(PAGINATION_FIELD.getPreferredName(), page());
            }

            if (Objects.nonNull(results)) {
                builder.field(SEARCH_RESULTS_FIELD.getPreferredName(), results());
            }
        }
        return builder.endObject();
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(query);
        out.writeOptionalString(searchApplication);
        out.writeOptionalWriteable(sortOrder);
        out.writeOptionalWriteable(page);
        out.writeOptionalWriteable(results);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearchData that = (AnalyticsEventSearchData) o;
        return query.equals(that.query)
            && Objects.equals(searchApplication, that.searchApplication)
            && Objects.equals(sortOrder, that.sortOrder)
            && Objects.equals(page, that.page)
            && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, searchApplication, sortOrder, page, results);
    }
}

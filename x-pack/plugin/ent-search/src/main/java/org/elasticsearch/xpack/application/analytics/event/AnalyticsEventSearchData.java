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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventSearchData implements ToXContent, Writeable {
    public static ParseField SEARCH_FIELD = new ParseField("search");

    public static ParseField SEARCH_QUERY_FIELD = new ParseField("query");

    private static final ConstructingObjectParser<AnalyticsEventSearchData, AnalyticsContext> PARSER = new ConstructingObjectParser<>(
        SEARCH_FIELD.getPreferredName(),
        false,
        (p, c) -> new AnalyticsEventSearchData((String) p[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SEARCH_QUERY_FIELD);
    }

    private final String query;

    public AnalyticsEventSearchData(String query) {
        this.query = Objects.requireNonNull(query, "search query can't be null");
    }

    public AnalyticsEventSearchData(StreamInput in) throws IOException {
        this(in.readString());
    }

    public static AnalyticsEventSearchData fromXContent(XContentParser parser, AnalyticsContext context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public String query() {
        return query;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SEARCH_QUERY_FIELD.getPreferredName(), query());
        }
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(query);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearchData that = (AnalyticsEventSearchData) o;
        return query.equals(that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query);
    }
}

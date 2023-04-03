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
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AnalyticsEventPaginationData implements ToXContent, Writeable {

    public static ParseField PAGINATION_FIELD = new ParseField("page");

    public static ParseField CURRENT_PAGE_FIELD = new ParseField("current");

    public static ParseField PAGE_SIZE_FIELD = new ParseField("size");

    private static final ConstructingObjectParser<AnalyticsEventPaginationData, AnalyticsEvent.Context> PARSER =
        new ConstructingObjectParser<>(PAGINATION_FIELD.getPreferredName(), false, (p, c) -> {
            int currentPage = (int) p[0], pageSize = (int) p[1];
            if (currentPage < 0) {
                throw new IllegalArgumentException(LoggerMessageFormat.format("Field [{}] must be positive", currentPage));
            }
            if (pageSize < 0) {
                throw new IllegalArgumentException(LoggerMessageFormat.format("Field [{}] must be positive", pageSize));
            }
            return new AnalyticsEventPaginationData(currentPage, pageSize);
        });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), CURRENT_PAGE_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), PAGE_SIZE_FIELD);
    }

    private final int current;

    private final int size;

    public AnalyticsEventPaginationData(int current, int size) {
        this.current = current;
        this.size = size;
    }

    public AnalyticsEventPaginationData(StreamInput in) throws IOException {
        this(in.readInt(), in.readInt());
    }

    public static AnalyticsEventPaginationData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public int current() {
        return current;
    }

    public int size() {
        return size;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(CURRENT_PAGE_FIELD.getPreferredName(), current());
            builder.field(PAGE_SIZE_FIELD.getPreferredName(), size());
        }
        return builder.endObject();
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(current);
        out.writeInt(size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventPaginationData that = (AnalyticsEventPaginationData) o;
        return current == that.current && size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, size);
    }
}

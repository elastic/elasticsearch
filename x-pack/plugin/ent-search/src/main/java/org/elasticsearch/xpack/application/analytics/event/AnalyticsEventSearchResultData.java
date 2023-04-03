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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class AnalyticsEventSearchResultData implements ToXContent, Writeable {
    public static ParseField SEARCH_RESULTS_TOTAL_FIELD = new ParseField("total_results");

    public static ParseField SEARCH_RESULT_ITEMS_FIELD = new ParseField("items");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AnalyticsEventSearchResultData, AnalyticsEvent.Context> PARSER =
        new ConstructingObjectParser<>(
            "search_results",
            false,
            (p, c) -> new AnalyticsEventSearchResultData(
                Objects.requireNonNullElse((List<AnalyticsEventSearchResultItemData>) p[0], Collections.emptyList()),
                (Integer) p[1]
            )
        );

    static {
        // TODO: Use a parser for simplified page data
        PARSER.declareObjectArray(
            ConstructingObjectParser.optionalConstructorArg(),
            AnalyticsEventSearchResultItemData::fromXContent,
            SEARCH_RESULT_ITEMS_FIELD
        );
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), SEARCH_RESULTS_TOTAL_FIELD);
    }

    private final List<AnalyticsEventSearchResultItemData> items;

    private final Integer totalResults;

    public AnalyticsEventSearchResultData(List<AnalyticsEventSearchResultItemData> items, @Nullable Integer totalResults) {
        this.items = Objects.requireNonNull(items);
        this.totalResults = totalResults;
    }

    public AnalyticsEventSearchResultData(StreamInput in) throws IOException {
        this(in.readImmutableList(AnalyticsEventSearchResultItemData::new), in.readOptionalInt());
    }

    public static AnalyticsEventSearchResultData fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context);
    }

    public List<AnalyticsEventSearchResultItemData> items() {
        return items;
    }

    public Integer totalResults() {
        return totalResults;
    }

    public boolean isEmpty() {
        return Objects.isNull(totalResults) && items.isEmpty();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            if (Objects.nonNull(totalResults)) {
                builder.field(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName(), totalResults());
            }

            if (items.isEmpty() == false) {
                builder.startArray(SEARCH_RESULT_ITEMS_FIELD.getPreferredName());
                for (var item : items()) {
                    item.toXContent(builder, params);
                }
                builder.endArray();
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
        out.writeList(items);
        out.writeOptionalInt(totalResults);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsEventSearchResultData that = (AnalyticsEventSearchResultData) o;
        return items.equals(that.items) && Objects.equals(totalResults, that.totalResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items, totalResults);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.parser.field.PaginationAnalyticsEventField.PAGINATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchFiltersAnalyticsEventField.SEARCH_FILTERS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SortOrderAnalyticsEventField.SORT_FIELD;

public class SearchAnalyticsEventField {
    public static ParseField SEARCH_FIELD = new ParseField("search");

    public static ParseField SEARCH_QUERY_FIELD = new ParseField("query");

    public static ParseField SEARCH_APPLICATION_FIELD = new ParseField("search_application");

    public static ParseField SEARCH_RESULTS_FIELD = new ParseField("results");

    private static final ObjectParser<Map<String, Object>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        SEARCH_FIELD.getPreferredName(),
        HashMap::new
    );

    static {
        PARSER.declareString((b, v) -> b.put(SEARCH_QUERY_FIELD.getPreferredName(), v), SEARCH_QUERY_FIELD);
        PARSER.declareString((b, v) -> b.put(SEARCH_APPLICATION_FIELD.getPreferredName(), v), SEARCH_APPLICATION_FIELD);
        PARSER.declareObject((b, v) -> b.put(SORT_FIELD.getPreferredName(), v), SortOrderAnalyticsEventField::fromXContent, SORT_FIELD);
        PARSER.declareObject(
            (b, v) -> b.put(SEARCH_FILTERS_FIELD.getPreferredName(), v),
            SearchFiltersAnalyticsEventField::fromXContent,
            SEARCH_FILTERS_FIELD
        );
        PARSER.declareObject(
            (b, v) -> b.put(PAGINATION_FIELD.getPreferredName(), v),
            PaginationAnalyticsEventField::fromXContent,
            PAGINATION_FIELD
        );
        PARSER.declareObject(
            (b, v) -> b.put(SEARCH_RESULTS_FIELD.getPreferredName(), v),
            SearchResultAnalyticsEventField::fromXContent,
            SEARCH_RESULTS_FIELD
        );

        PARSER.declareRequiredFieldSet(SEARCH_QUERY_FIELD.getPreferredName());
    }

    private SearchAnalyticsEventField() {}

    public static Map<String, Object> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }
}

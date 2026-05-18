/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PaginationAnalyticsEventField.PAGINATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PaginationAnalyticsEventFieldTests.randomEventSearchPaginationField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventField.SEARCH_APPLICATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventField.SEARCH_QUERY_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventField.SEARCH_RESULTS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchFiltersAnalyticsEventField.SEARCH_FILTERS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchFiltersAnalyticsEventFieldTests.randomEventSearchFiltersField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchResultAnalyticsEventFieldTests.randomEventSearchResultField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SortOrderAnalyticsEventField.SORT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SortOrderAnalyticsEventFieldTests.randomEventSearchSortOrderField;

public class SearchAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<Object> {
    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(SEARCH_QUERY_FIELD.getPreferredName());
    }

    @Override
    protected Map<String, Object> createTestInstance() {
        return new HashMap<>(randomEventSearchField());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, Object>> parser() {
        return SearchAnalyticsEventField::fromXContent;
    }

    public static Map<String, Object> randomEventSearchField() {
        return Map.ofEntries(
            entry(SEARCH_QUERY_FIELD.getPreferredName(), randomIdentifier()),
            entry(SEARCH_APPLICATION_FIELD.getPreferredName(), randomIdentifier()),
            entry(SORT_FIELD.getPreferredName(), randomEventSearchSortOrderField()),
            entry(PAGINATION_FIELD.getPreferredName(), randomEventSearchPaginationField()),
            entry(SEARCH_RESULTS_FIELD.getPreferredName(), randomEventSearchResultField()),
            entry(SEARCH_FILTERS_FIELD.getPreferredName(), randomEventSearchFiltersField())
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ContextParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.CURRENT_PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGINATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_APPLICATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_QUERY_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_RESULTS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULTS_TOTAL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULT_ITEMS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.DIRECTION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.NAME_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.SORT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSearchData;

public class AnalyticsEventSearchDataTests extends AbstractEventDataTestCase<AnalyticsEventSearchData> {

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventSearchData> parser() {
        return AnalyticsEventSearchData::fromXContent;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void assertXContentData(AnalyticsEventSearchData searchData, Map<String, Object> objectAsMap) {
        assertEquals(5, objectAsMap.size());

        assertTrue(objectAsMap.containsKey(SEARCH_QUERY_FIELD.getPreferredName()));
        assertEquals(searchData.query(), objectAsMap.get(SEARCH_QUERY_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(SEARCH_APPLICATION_FIELD.getPreferredName()));
        assertEquals(searchData.searchApplication(), objectAsMap.get(SEARCH_APPLICATION_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(SORT_FIELD.getPreferredName()));
        Map<String, Object> sortOrderMap = (Map<String, Object>) objectAsMap.get(SORT_FIELD.getPreferredName());
        assertEquals(searchData.sortOrder().name(), sortOrderMap.get(NAME_FIELD.getPreferredName()));
        assertEquals(searchData.sortOrder().direction(), sortOrderMap.get(DIRECTION_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(PAGINATION_FIELD.getPreferredName()));
        Map<String, Object> paginationDataMap = (Map<String, Object>) objectAsMap.get(PAGINATION_FIELD.getPreferredName());
        assertEquals(searchData.page().current(), paginationDataMap.get(CURRENT_PAGE_FIELD.getPreferredName()));
        assertEquals(searchData.page().size(), paginationDataMap.get(PAGE_SIZE_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(SEARCH_RESULTS_FIELD.getPreferredName()));
        Map<String, Object> searchResultsMap = (Map<String, Object>) objectAsMap.get(SEARCH_RESULTS_FIELD.getPreferredName());
        assertEquals(searchData.results().totalResults(), searchResultsMap.get(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName()));
        assertEquals(
            searchData.results().items().size(),
            ((List<?>) searchResultsMap.get(SEARCH_RESULT_ITEMS_FIELD.getPreferredName())).size()
        );
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.singletonList(SEARCH_QUERY_FIELD.getPreferredName());
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSearchData> instanceReader() {
        return AnalyticsEventSearchData::new;
    }

    @Override
    protected AnalyticsEventSearchData createTestInstance() {
        return randomEventSearchData();
    }

    @Override
    protected AnalyticsEventSearchData mutateInstance(AnalyticsEventSearchData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}

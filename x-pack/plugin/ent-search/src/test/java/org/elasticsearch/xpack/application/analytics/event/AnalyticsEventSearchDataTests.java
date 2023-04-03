/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.CURRENT_PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGINATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_APPLICATION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_QUERY_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_RESULTS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULTS_TOTAL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULT_ITEMS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.DIRECTION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.NAME_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.SORT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSearchData;

public class AnalyticsEventSearchDataTests extends AbstractWireSerializingTestCase<AnalyticsEventSearchData> {

    @SuppressWarnings("unchecked")
    public void testToXContentWithAllFields() throws IOException {
        AnalyticsEventSearchData search = createTestInstance();

        // Serialize the search data
        BytesReference json = XContentHelper.toXContent(search, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(5, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(SEARCH_QUERY_FIELD.getPreferredName()));
        assertEquals(search.query(), contentAsMap.get(SEARCH_QUERY_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(SEARCH_APPLICATION_FIELD.getPreferredName()));
        assertEquals(search.searchApplication(), contentAsMap.get(SEARCH_APPLICATION_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(SORT_FIELD.getPreferredName()));
        Map<String, Object> sortOrderMap = (Map<String, Object>) contentAsMap.get(SORT_FIELD.getPreferredName());
        assertEquals(search.sortOrder().name(), sortOrderMap.get(NAME_FIELD.getPreferredName()));
        assertEquals(search.sortOrder().direction(), sortOrderMap.get(DIRECTION_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(PAGINATION_FIELD.getPreferredName()));
        Map<String, Object> paginationDataMap = (Map<String, Object>) contentAsMap.get(PAGINATION_FIELD.getPreferredName());
        assertEquals(search.page().current(), paginationDataMap.get(CURRENT_PAGE_FIELD.getPreferredName()));
        assertEquals(search.page().size(), paginationDataMap.get(PAGE_SIZE_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(SEARCH_RESULTS_FIELD.getPreferredName()));
        Map<String, Object> searchResultsMap = (Map<String, Object>) contentAsMap.get(SEARCH_RESULTS_FIELD.getPreferredName());
        assertEquals(search.results().totalResults(), searchResultsMap.get(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName()));
        assertEquals(
            search.results().items().size(),
            ((List<?>) searchResultsMap.get(SEARCH_RESULT_ITEMS_FIELD.getPreferredName())).size()
        );

        // Check we can re-serialize with fromXContent and object are equals
        assertEquals(search, parseSearchData(json));
    }

    public void testToXContentWithOnlyRequiredFields() throws IOException {
        AnalyticsEventSearchData search = new AnalyticsEventSearchData(randomIdentifier());

        // Serialize the search data
        BytesReference json = XContentHelper.toXContent(search, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(1, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(SEARCH_QUERY_FIELD.getPreferredName()));
        assertEquals(search.query(), contentAsMap.get(SEARCH_QUERY_FIELD.getPreferredName()));

        // Check we can re-serialize with fromXContent and object are equals
        assertEquals(search, parseSearchData(json));
    }

    public void testFromXContentWithAllFields() throws IOException {
        Map<String, String> sortOrderMap = MapBuilder.<String, String>newMapBuilder()
            .put(NAME_FIELD.getPreferredName(), randomIdentifier())
            .put(DIRECTION_FIELD.getPreferredName(), randomIdentifier())
            .map();

        Map<String, Integer> paginationDataMap = MapBuilder.<String, Integer>newMapBuilder()
            .put(CURRENT_PAGE_FIELD.getPreferredName(), randomInt())
            .put(PAGE_SIZE_FIELD.getPreferredName(), randomInt())
            .map();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(SEARCH_QUERY_FIELD.getPreferredName(), randomIdentifier())
            .put(SEARCH_APPLICATION_FIELD.getPreferredName(), randomIdentifier())
            .put(SORT_FIELD.getPreferredName(), sortOrderMap)
            .put(PAGINATION_FIELD.getPreferredName(), paginationDataMap)
            .map();

        AnalyticsEventSearchData search = parseSearchData(convertMapToJson(jsonMap));

        assertEquals(jsonMap.get(SEARCH_QUERY_FIELD.getPreferredName()), search.query());
        assertEquals(jsonMap.get(SEARCH_APPLICATION_FIELD.getPreferredName()), search.searchApplication());

        assertEquals(sortOrderMap.get(NAME_FIELD.getPreferredName()), search.sortOrder().name());
        assertEquals(sortOrderMap.get(DIRECTION_FIELD.getPreferredName()), search.sortOrder().direction());

        assertEquals(paginationDataMap.get(CURRENT_PAGE_FIELD.getPreferredName()).intValue(), search.page().current());
        assertEquals(paginationDataMap.get(PAGE_SIZE_FIELD.getPreferredName()).intValue(), search.page().size());
    }

    public void testFromXContentWithOnlyRequiredFields() throws IOException {
        String query = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SEARCH_QUERY_FIELD.getPreferredName(), query).map();

        AnalyticsEventSearchData search = parseSearchData(convertMapToJson(jsonMap));

        assertEquals(query, search.query());
        assertNull(search.searchApplication());
        assertNull(search.sortOrder());
        assertNull(search.page());
    }

    public void testFromXContentWhenQueryFieldIsMissing() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}}]", SEARCH_QUERY_FIELD),
            () -> parseSearchData(new BytesArray("{}"))
        );
    }

    public void testFromXContentWithAnInvalidFields() {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(SEARCH_QUERY_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", SEARCH_FIELD, invalidFieldName),
            () -> parseSearchData(convertMapToJson(jsonMap))
        );
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

    private AnalyticsEventSearchData parseSearchData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventSearchData.fromXContent(contentParser, null);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

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

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULTS_TOTAL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULT_ITEMS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSearchResultItem;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSearchResults;

public class AnalyticsEventSearchResultDataTests extends AbstractWireSerializingTestCase<AnalyticsEventSearchResultData> {

    public void testToXContent() throws IOException {
        AnalyticsEventSearchResultData searchResult = createTestInstance();

        // Serialize the paginationData data
        BytesReference json = XContentHelper.toXContent(searchResult, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(2, contentAsMap.size());

        assertEquals(searchResult.totalResults(), contentAsMap.get(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName()));
        assertEquals(searchResult.items().size(), ((List<?>) contentAsMap.get(SEARCH_RESULT_ITEMS_FIELD.getPreferredName())).size());

        // Check we can serialize again with fromXContent and object are equals
        assertEquals(searchResult, parseSearchResultData(json));
    }

    public void testFromXContentWithAnInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .putAll(
                XContentHelper.convertToMap(
                    XContentHelper.toXContent(randomEventSearchResultItem(), XContentType.JSON, false),
                    false,
                    JsonXContent.jsonXContent.type()
                ).v2()
            )
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", "search_result", invalidFieldName),
            () -> parseSearchResultData(convertMapToJson(jsonMap))
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSearchResultData> instanceReader() {
        return AnalyticsEventSearchResultData::new;
    }

    @Override
    protected AnalyticsEventSearchResultData createTestInstance() {
        return randomEventSearchResults();
    }

    @Override
    protected AnalyticsEventSearchResultData mutateInstance(AnalyticsEventSearchResultData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventSearchResultData parseSearchResultData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventSearchResultData.fromXContent(contentParser, null);
        }
    }
}

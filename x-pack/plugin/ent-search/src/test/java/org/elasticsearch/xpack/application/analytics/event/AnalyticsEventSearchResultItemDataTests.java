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
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_ID_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_INDEX_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_TITLE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_URL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventDocumentData;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventPageData;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSearchResultItem;

public class AnalyticsEventSearchResultItemDataTests extends AbstractWireSerializingTestCase<AnalyticsEventSearchResultItemData> {

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        AnalyticsEventSearchResultItemData item = randomEventSearchResultItem();

        // Serialize the paginationData data
        BytesReference json = XContentHelper.toXContent(item, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(2, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(DOCUMENT_FIELD.getPreferredName()));
        Map<String, String> documentDataMap = (Map<String, String>) contentAsMap.get(DOCUMENT_FIELD.getPreferredName());
        assertEquals(item.document().id(), documentDataMap.get(DOCUMENT_ID_FIELD.getPreferredName()));
        assertEquals(item.document().index(), documentDataMap.get(DOCUMENT_INDEX_FIELD.getPreferredName()));

        assertTrue(contentAsMap.containsKey(PAGE_FIELD.getPreferredName()));
        Map<String, String> pageDataMap = (Map<String, String>) contentAsMap.get(PAGE_FIELD.getPreferredName());
        assertEquals(item.page().url(), pageDataMap.get(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(item.page().title(), pageDataMap.get(PAGE_TITLE_FIELD.getPreferredName()));

        // Check we can serialize again with fromXContent and object are equals
        assertEquals(item, parseSearchResultData(json));
    }

    public void testFromXContent() throws IOException {
        AnalyticsEventDocumentData documentData = randomEventDocumentData();
        AnalyticsEventPageData pageData = randomEventPageData();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(
                DOCUMENT_FIELD.getPreferredName(),
                XContentHelper.convertToMap(
                    XContentHelper.toXContent(documentData, XContentType.JSON, false),
                    false,
                    JsonXContent.jsonXContent.type()
                ).v2()
            )
            .put(
                PAGE_FIELD.getPreferredName(),
                XContentHelper.convertToMap(
                    XContentHelper.toXContent(pageData, XContentType.JSON, false),
                    false,
                    JsonXContent.jsonXContent.type()
                ).v2()
            )
            .map();

        AnalyticsEventSearchResultItemData item = parseSearchResultData(convertMapToJson(jsonMap));

        assertEquals(documentData, item.document());
        assertEquals(pageData, item.page());
    }

    public void testFromXContentWhenDocumentFieldIsMissing() throws IOException {
        AnalyticsEventPageData pageData = randomEventPageData();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(
                PAGE_FIELD.getPreferredName(),
                XContentHelper.convertToMap(
                    XContentHelper.toXContent(pageData, XContentType.JSON, false),
                    false,
                    JsonXContent.jsonXContent.type()
                ).v2()
            )
            .map();

        AnalyticsEventSearchResultItemData item = parseSearchResultData(convertMapToJson(jsonMap));

        assertNull(item.document());
        assertEquals(pageData, item.page());
    }

    public void testFromXContentWhenPageFieldIsMissing() throws IOException {
        AnalyticsEventDocumentData documentData = randomEventDocumentData();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(
                DOCUMENT_FIELD.getPreferredName(),
                XContentHelper.convertToMap(
                    XContentHelper.toXContent(documentData, XContentType.JSON, false),
                    false,
                    JsonXContent.jsonXContent.type()
                ).v2()
            )
            .map();

        AnalyticsEventSearchResultItemData item = parseSearchResultData(convertMapToJson(jsonMap));

        assertNull(item.page());
        assertEquals(documentData, item.document());
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
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", "search_result_item", invalidFieldName),
            () -> parseSearchResultData(convertMapToJson(jsonMap))
        );
    }

    public void testFromXContentWithoutPageOrDocument() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format(
                "Either [{}}] or [{}] field is required",
                DOCUMENT_FIELD.getPreferredName(),
                PAGE_FIELD.getPreferredName()
            ),
            () -> parseSearchResultData(convertMapToJson(Collections.emptyMap()))
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSearchResultItemData> instanceReader() {
        return AnalyticsEventSearchResultItemData::new;
    }

    @Override
    protected AnalyticsEventSearchResultItemData createTestInstance() {
        return randomEventSearchResultItem();
    }

    @Override
    protected AnalyticsEventSearchResultItemData mutateInstance(AnalyticsEventSearchResultItemData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventSearchResultItemData parseSearchResultData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventSearchResultItemData.fromXContent(contentParser, null);
        }
    }
}

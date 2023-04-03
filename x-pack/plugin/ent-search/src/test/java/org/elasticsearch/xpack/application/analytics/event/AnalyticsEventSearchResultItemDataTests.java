/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ContextParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
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

public class AnalyticsEventSearchResultItemDataTests extends AbstractEventDataTestCase<AnalyticsEventSearchResultItemData> {

    public void testFromXContentWhenDocumentFieldIsMissing() throws IOException {
        AnalyticsEventPageData pageData = randomEventPageData();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(PAGE_FIELD.getPreferredName(), mapFromObject(pageData))
            .map();

        System.out.println(convertMapToJson(jsonMap));

        AnalyticsEventSearchResultItemData item = parseJson(convertMapToJson(jsonMap));

        assertNull(item.document());
        assertEquals(pageData, item.page());
    }

    public void testFromXContentWhenPageFieldIsMissing() throws IOException {
        AnalyticsEventDocumentData documentData = randomEventDocumentData();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(DOCUMENT_FIELD.getPreferredName(), mapFromObject(documentData))
            .map();

        System.out.println(convertMapToJson(jsonMap));

        AnalyticsEventSearchResultItemData item = parseJson(convertMapToJson(jsonMap));

        assertNull(item.page());
        assertEquals(documentData, item.document());
    }

    public void testFromXContentWithoutPageOrDocument() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format(
                "Either [{}}] or [{}] field is required",
                DOCUMENT_FIELD.getPreferredName(),
                PAGE_FIELD.getPreferredName()
            ),
            () -> parseJson(new BytesArray("{}"))
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void assertXContentData(AnalyticsEventSearchResultItemData item, Map<String, Object> objectAsMap) {
        assertEquals(2, objectAsMap.size());

        assertTrue(objectAsMap.containsKey(DOCUMENT_FIELD.getPreferredName()));
        Map<String, String> documentDataMap = (Map<String, String>) objectAsMap.get(DOCUMENT_FIELD.getPreferredName());
        assertEquals(item.document().id(), documentDataMap.get(DOCUMENT_ID_FIELD.getPreferredName()));
        assertEquals(item.document().index(), documentDataMap.get(DOCUMENT_INDEX_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(PAGE_FIELD.getPreferredName()));
        Map<String, String> pageDataMap = (Map<String, String>) objectAsMap.get(PAGE_FIELD.getPreferredName());
        assertEquals(item.page().url(), pageDataMap.get(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(item.page().title(), pageDataMap.get(PAGE_TITLE_FIELD.getPreferredName()));
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventSearchResultItemData> parser() {
        return AnalyticsEventSearchResultItemData::fromXContent;
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.emptyList();
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
}

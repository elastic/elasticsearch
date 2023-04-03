/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_REFERRER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_TITLE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_URL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventPageData;

public class AnalyticsEventPageDataTests extends AbstractEventDataTestCase<AnalyticsEventPageData> {

    public void testToXContentWithOnlyRequiredFields() throws IOException {
        AnalyticsEventPageData page = new AnalyticsEventPageData(randomIdentifier());

        // Serialize the page.
        BytesReference json = XContentHelper.toXContent(page, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();

        assertEquals(1, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(page.url(), contentAsMap.get(PAGE_URL_FIELD.getPreferredName()));

        // Check we can re-serialize with fromXContent and object are equals.
        assertEquals(page, parsePageEventData(json));
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventPageData> parser() {
        return AnalyticsEventPageData::fromXContent;
    }

    @Override
    protected void assertXContentData(AnalyticsEventPageData page, Map<String, Object> objectAsMap) {
        assertEquals(3, objectAsMap.size());

        assertTrue(objectAsMap.containsKey(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(page.url(), objectAsMap.get(PAGE_URL_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(PAGE_TITLE_FIELD.getPreferredName()));
        assertEquals(page.title(), objectAsMap.get(PAGE_TITLE_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(PAGE_REFERRER_FIELD.getPreferredName()));
        assertEquals(page.referrer(), objectAsMap.get(PAGE_REFERRER_FIELD.getPreferredName()));
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.singletonList(PAGE_URL_FIELD.getPreferredName());
    }

    @Override
    protected Writeable.Reader<AnalyticsEventPageData> instanceReader() {
        return AnalyticsEventPageData::new;
    }

    @Override
    protected AnalyticsEventPageData createTestInstance() {
        return randomEventPageData();
    }

    @Override
    protected AnalyticsEventPageData mutateInstance(AnalyticsEventPageData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private AnalyticsEventPageData parsePageEventData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventPageData.fromXContent(contentParser, null);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ContextParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_REFERRER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_TITLE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPageData.PAGE_URL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createAnalyticsContextMockFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createPayloadFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomPageViewEvent;

public class AnalyticsEventPageViewTests extends AbstractEventTestCase<AnalyticsEventPageView> {

    @SuppressWarnings("unchecked")
    protected void assertToXContentAdditionalFields(Map<String, Object> eventMap, AnalyticsEventPageView event) {
        // Check page field content.
        assertTrue(eventMap.containsKey(PAGE_FIELD.getPreferredName()));
        Map<String, String> pageDataMap = (Map<String, String>) eventMap.get(PAGE_FIELD.getPreferredName());
        assertEquals(event.page().url(), pageDataMap.get(PAGE_URL_FIELD.getPreferredName()));
        assertEquals(event.page().title(), pageDataMap.get(PAGE_TITLE_FIELD.getPreferredName()));
        assertEquals(event.page().referrer(), pageDataMap.get(PAGE_REFERRER_FIELD.getPreferredName()));
    }

    public void testFromXContentFailsWhenPageDataAreMissing() throws IOException {
        AnalyticsEvent event = createTestInstance();
        BytesReference payload = createPayloadFromEvent(event, PAGE_FIELD.getPreferredName());

        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(event);

        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", PAGE_FIELD.getPreferredName()),
            () -> parsePageEventData(context, payload)
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventPageView> instanceReader() {
        return AnalyticsEventPageView::new;
    }

    @Override
    protected AnalyticsEventPageView createTestInstance() {
        return randomPageViewEvent();
    }

    @Override
    protected AnalyticsEventPageView mutateInstance(AnalyticsEventPageView instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventPageView> parser() {
        return AnalyticsEventPageView::fromXContent;
    }
}

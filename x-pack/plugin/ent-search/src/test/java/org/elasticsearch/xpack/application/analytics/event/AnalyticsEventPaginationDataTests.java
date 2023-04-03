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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.CURRENT_PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventPaginationData.PAGE_SIZE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventPaginationData;

public class AnalyticsEventPaginationDataTests extends AbstractEventDataTestCase<AnalyticsEventPaginationData> {

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventPaginationData> parser() {
        return AnalyticsEventPaginationData::fromXContent;
    }

    @Override
    protected void assertXContentData(AnalyticsEventPaginationData paginationObject, Map<String, Object> objectAsMap) {
        assertEquals(2, objectAsMap.size());

        assertTrue(objectAsMap.containsKey(CURRENT_PAGE_FIELD.getPreferredName()));
        assertEquals(paginationObject.current(), objectAsMap.get(CURRENT_PAGE_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(PAGE_SIZE_FIELD.getPreferredName()));
        assertEquals(paginationObject.size(), objectAsMap.get(PAGE_SIZE_FIELD.getPreferredName()));
    }

    @Override
    protected List<String> requiredFields() {
        return Arrays.asList(CURRENT_PAGE_FIELD.getPreferredName(), PAGE_SIZE_FIELD.getPreferredName());
    }

    @Override
    protected Writeable.Reader<AnalyticsEventPaginationData> instanceReader() {
        return AnalyticsEventPaginationData::new;
    }

    @Override
    protected AnalyticsEventPaginationData createTestInstance() {
        return randomEventPaginationData();
    }

    @Override
    protected AnalyticsEventPaginationData mutateInstance(AnalyticsEventPaginationData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}

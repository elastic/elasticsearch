/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.DIRECTION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.NAME_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSortOrderData.SORT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSortOrderData;

public class AnalyticsEventSortOrderDataTests extends AbstractEventDataTestCase<AnalyticsEventSortOrderData> {
    public void testFromXContentWhenNameIsBlank() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(NAME_FIELD.getPreferredName(), "").map();

        Exception e = expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}] failed to parse field [{}]", SORT_FIELD.getPreferredName(), NAME_FIELD),
            () -> parseJson(convertMapToJson(jsonMap))
        );

        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", NAME_FIELD), e.getCause().getMessage());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventSortOrderData> parser() {
        return AnalyticsEventSortOrderData::fromXContent;
    }

    @Override
    protected void assertXContentData(AnalyticsEventSortOrderData sortOrder, Map<String, Object> objectAsMap) {
        assertEquals(2, objectAsMap.size());

        assertTrue(objectAsMap.containsKey(NAME_FIELD.getPreferredName()));
        assertEquals(sortOrder.name(), objectAsMap.get(NAME_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(DIRECTION_FIELD.getPreferredName()));
        assertEquals(sortOrder.direction(), objectAsMap.get(DIRECTION_FIELD.getPreferredName()));
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.singletonList(NAME_FIELD.getPreferredName());
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSortOrderData> instanceReader() {
        return AnalyticsEventSortOrderData::new;
    }

    @Override
    protected AnalyticsEventSortOrderData createTestInstance() {
        return randomEventSortOrderData();
    }

    @Override
    protected AnalyticsEventSortOrderData mutateInstance(AnalyticsEventSortOrderData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}

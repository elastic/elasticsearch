/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsEventTestUtils {
    public static BytesReference convertMapToJson(Map<String, ?> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder);
        }
    }

    public static AnalyticsEvent.Context createAnalyticsContextMockFromEvent(AnalyticsEvent event) {
        AnalyticsEvent.Context context = mock(AnalyticsEvent.Context.class);

        when(context.eventCollectionName()).thenReturn(event.eventCollectionName());
        when(context.eventTime()).thenReturn(event.eventTime());
        when(context.eventType()).thenReturn(event.eventType());

        return context;
    }

    public static AnalyticsEvent randomAnalyticsEvent() {
        return new AnalyticsEvent(
            randomIdentifier(),
            randomLong(),
            randomFrom(AnalyticsEvent.Type.values()),
            XContentType.JSON,
            new BytesArray("{}")
        );
    }
}

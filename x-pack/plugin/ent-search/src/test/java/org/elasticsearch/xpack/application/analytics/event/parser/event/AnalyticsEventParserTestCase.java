/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.createAnalyticsContextMockFromEvent;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomInetAddress;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomUserAgent;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AnalyticsEventParserTestCase extends ESTestCase {
    public void testParsingWithAllFields() throws IOException {
        AnalyticsEvent sourceEvent = this.createTestInstance();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(sourceEvent);
        BytesReference json = sourceEvent.payload();
        try (XContentParser xContentParser = createXContentParser(json)) {
            AnalyticsEvent parsedEvent = parser().parse(xContentParser, context);

            assertEquals(sourceEvent, parsedEvent);
        }
    }

    public void testParsingWithRequiredFieldOnly() throws IOException {
        AnalyticsEvent sourceEvent = this.createTestInstance();
        AnalyticsEvent.Context context = createAnalyticsContextMockFromEvent(sourceEvent);

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            Map<String, Object> payloadAsMap = sourceEvent.payloadAsMap()
                .entrySet()
                .stream()
                .filter(e -> isFieldRequired().test(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            BytesReference json = BytesReference.bytes(builder.map(payloadAsMap));

            try (XContentParser xContentParser = createXContentParser(json)) {
                AnalyticsEvent parsedEvent = parser().parse(xContentParser, context);

                assertEquals(sourceEvent.eventCollectionName(), parsedEvent.eventCollectionName());
                assertEquals(sourceEvent.eventType(), parsedEvent.eventType());
                assertEquals(sourceEvent.eventTime(), parsedEvent.eventTime());

                for (String field : requiredFields()) {
                    assertEquals(sourceEvent.payloadAsMap().get(field), parsedEvent.payloadAsMap().get(field));
                }
            }
        }
    }

    public void testParsingWithMissingRequiredField() throws IOException {
        AnalyticsEvent.Context context = mock(AnalyticsEvent.Context.class);

        for (String field : requiredFields()) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                Map<String, Object> payloadAsMap = this.createTestInstance()
                    .payloadAsMap()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getKey().equals(field) == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                BytesReference json = BytesReference.bytes(builder.map(payloadAsMap));

                try (XContentParser xContentParser = createXContentParser(json)) {
                    Exception e = expectThrows(IllegalArgumentException.class, () -> parser().parse(xContentParser, context));
                    assertEquals(Strings.format("Required one of fields [%s], but none were specified. ", field), e.getMessage());
                }
            }
        }
    }

    public void testParsingWithInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();
        AnalyticsEvent.Context context = mock(AnalyticsEvent.Context.class);

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            Map<String, Object> payloadAsMap = MapBuilder.<String, Object>newMapBuilder()
                .putAll(this.createTestInstance().payloadAsMap())
                .put(invalidFieldName, randomIdentifier())
                .map();

            BytesReference json = BytesReference.bytes(builder.map(payloadAsMap));

            try (XContentParser xContentParser = createXContentParser(json)) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> parser().parse(xContentParser, context));
                assertTrue(e.getMessage().contains(Strings.format("[%s] unknown field [%s]", parserName(), invalidFieldName)));
            }
        }
    }

    protected abstract ContextParser<AnalyticsEvent.Context, AnalyticsEvent> parser();

    protected abstract AnalyticsEvent createTestInstance() throws IOException;

    protected abstract List<String> requiredFields();

    protected abstract String parserName();

    protected Predicate<String> isFieldRequired() {
        return requiredFields()::contains;
    }

    private XContentParser createXContentParser(BytesReference json) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.streamInput());
    }

    public static AnalyticsEvent randomAnalyticsEvent(AnalyticsEvent.Type eventType, Map<String, Object> payload) throws IOException {
        AnalyticsEvent.Context context = mock(AnalyticsEvent.Context.class);
        when(context.eventType()).thenReturn(eventType);
        when(context.eventTime()).thenReturn(randomLong());
        when(context.eventCollectionName()).thenReturn(randomIdentifier());
        when(context.userAgent()).thenReturn(randomUserAgent());
        when(context.clientAddress()).thenReturn(randomInetAddress());

        return AnalyticsEvent.builder(context).with(payload).build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.mockito.Mockito.mock;

public abstract class AnalyticsEventFieldParserTestCase<T> extends ESTestCase {
    public void testParsingWithAllFields() throws IOException {
        Map<String, T> jsonMap = createTestInstance();
        BytesReference json = convertMapToJson(jsonMap);
        try (XContentParser xContentParser = createXContentParser(json)) {
            Map<String, T> parsedData = parser().parse(xContentParser, mock(AnalyticsEvent.Context.class));

            assertEquals(jsonMap, parsedData);
        }
    }

    public void testParsingWithOnlyRequiredFields() throws IOException {
        Map<String, T> jsonMap = createTestInstance().entrySet()
            .stream()
            .filter(e -> requiredFields().contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        BytesReference json = convertMapToJson(jsonMap);

        try (XContentParser xContentParser = createXContentParser(json)) {
            Map<String, T> parsedData = parser().parse(xContentParser, mock(AnalyticsEvent.Context.class));

            assertEquals(requiredFields().size(), parsedData.size());
            assertEquals(jsonMap, parsedData);
        }
    }

    public void testParsingWithInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();
        Map<String, T> jsonMap = createTestInstance();
        jsonMap.put(invalidFieldName, null);

        BytesReference json = convertMapToJson(jsonMap);

        try (XContentParser xContentParser = createXContentParser(json)) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> parser().parse(xContentParser, mock(AnalyticsEvent.Context.class))
            );
            assertTrue(e.getMessage().contains(Strings.format("unknown field [%s]", invalidFieldName)));
        }
    }

    public void testParsingWhenRequiredFieldIsMissing() throws IOException {
        for (String field : requiredFields()) {
            Map<String, T> jsonMap = createTestInstance();
            jsonMap.remove(field);

            BytesReference json = convertMapToJson(jsonMap);

            try (XContentParser xContentParser = createXContentParser(json)) {
                Exception e = expectThrows(
                    IllegalArgumentException.class,
                    () -> parser().parse(xContentParser, mock(AnalyticsEvent.Context.class))
                );
                assertTrue(e.getMessage().contains(Strings.format("Required one of fields [%s], but none were specified.", field)));
            }
        }
    }

    public abstract List<String> requiredFields();

    protected XContentParser createXContentParser(BytesReference json) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json.streamInput());
    }

    protected abstract Map<String, T> createTestInstance();

    protected abstract ContextParser<AnalyticsEvent.Context, Map<String, T>> parser();
}

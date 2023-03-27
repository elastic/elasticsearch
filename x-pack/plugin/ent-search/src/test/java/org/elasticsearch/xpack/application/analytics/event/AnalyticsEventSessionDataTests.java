/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_ID_FIELD;

public class AnalyticsEventSessionDataTests extends AbstractWireSerializingTestCase<AnalyticsEventSessionData> {

    public static AnalyticsEventSessionData randomEventSessionData() {
        return new AnalyticsEventSessionData(randomIdentifier());
    }

    public void testToXContent() throws IOException {
        AnalyticsEventSessionData session = randomEventSessionData();

        // Serialize the session data
        BytesReference json = XContentHelper.toXContent(session, XContentType.JSON, false);

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(1, contentAsMap.size());
        assertTrue(contentAsMap.containsKey(SESSION_ID_FIELD.getPreferredName()));
        assertEquals(session.id(), contentAsMap.get(SESSION_ID_FIELD.getPreferredName()));

        // Check we can serialize again with fromXContent and object are equals
        assertEquals(session, parseSessionData(json));
    }

    public void testFromXContent() throws IOException {
        String sessionId = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SESSION_ID_FIELD.getPreferredName(), sessionId).map();

        AnalyticsEventSessionData session = parseSessionData(convertMapToJson(jsonMap));
        assertEquals(sessionId, session.id());
    }

    public void testFromXContentWhenIdFieldIsMissing() {
        expectThrows(
            IllegalArgumentException.class,
            LoggerMessageFormat.format("Required [{}]", SESSION_ID_FIELD),
            () -> parseSessionData(new BytesArray("{}"))
        );
    }

    public void testFromXContentWhenSessionIdIsBlank() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SESSION_ID_FIELD.getPreferredName(), "").map();

        Exception e = expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}] failed to parse field [{}]", SESSION_FIELD, SESSION_ID_FIELD),
            () -> parseSessionData(convertMapToJson(jsonMap))
        );

        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", SESSION_ID_FIELD), e.getCause().getMessage());
    }

    public void testFromXContentWithAnInvalidField() {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(SESSION_ID_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", SESSION_FIELD, invalidFieldName),
            () -> parseSessionData(convertMapToJson(jsonMap))
        );
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSessionData> instanceReader() {
        return AnalyticsEventSessionData::new;
    }

    @Override
    protected AnalyticsEventSessionData createTestInstance() {
        return randomEventSessionData();
    }

    @Override
    protected AnalyticsEventSessionData mutateInstance(AnalyticsEventSessionData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventSessionData parseSessionData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventSessionData.fromXContent(contentParser, null);
        }
    }

    private static BytesReference convertMapToJson(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
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
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            BytesReference json = BytesReference.bytes(session.toXContent(builder, ToXContent.EMPTY_PARAMS));

            // Check the content that have been processed.
            Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
            assertEquals(1, contentAsMap.size());
            assertTrue(contentAsMap.containsKey(SESSION_ID_FIELD.getPreferredName()));
            assertEquals(session.id(), contentAsMap.get(SESSION_ID_FIELD.getPreferredName()));

            // Check we can serialize again with fromXContent and object are equals
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
                assertEquals(session, AnalyticsEventSessionData.fromXContent(parser, null));
            }
        }
    }

    public void testFromXContent() throws IOException {
        String sessionId = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SESSION_ID_FIELD.getPreferredName(), sessionId).map();
        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            AnalyticsEventSessionData session = AnalyticsEventSessionData.fromXContent(parser, null);
            assertEquals(sessionId, session.id());
        }
    }

    public void testFromXContentWhenIdFieldIsMissing() throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, "{}")) {
            expectThrows(
                IllegalArgumentException.class,
                LoggerMessageFormat.format("Required [{}]", SESSION_ID_FIELD),
                () -> AnalyticsEventSessionData.fromXContent(parser, null)
            );
        }
    }

    public void testFromXContentWhenSessionIdIsBlank() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SESSION_ID_FIELD.getPreferredName(), "").map();
        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {

            Exception e = expectThrows(
                XContentParseException.class,
                LoggerMessageFormat.format("[{}] failed to parse field [{}]", SESSION_FIELD, SESSION_ID_FIELD),
                () -> AnalyticsEventSessionData.fromXContent(parser, null)
            );

            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
            assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", SESSION_ID_FIELD), e.getCause().getMessage());
        }
    }

    public void testFromXContentWithAnInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(SESSION_ID_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();
        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            expectThrows(
                XContentParseException.class,
                LoggerMessageFormat.format("[{}}] failed to parse field [{}]", SESSION_FIELD, invalidFieldName),
                () -> AnalyticsEventSessionData.fromXContent(parser, null)
            );
        }
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
}

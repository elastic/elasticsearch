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

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_ID_FIELD;

public class AnalyticsEventUserDataTests extends AbstractWireSerializingTestCase<AnalyticsEventUserData> {

    public static AnalyticsEventUserData randomEventUserData() {
        return new AnalyticsEventUserData(randomIdentifier());
    }

    public void testToXContent() throws IOException {
        AnalyticsEventUserData user = randomEventUserData();
        BytesReference json = BytesReference.bytes(user.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS));

        // Check the content that have been processed.
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();
        assertEquals(1, contentAsMap.size());
        assertTrue(contentAsMap.containsKey(USER_ID_FIELD.getPreferredName()));
        assertEquals(user.id(), contentAsMap.get(USER_ID_FIELD.getPreferredName()));

        // Check we can reserialize with fromXContent and object are equals
        XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array());
        assertEquals(user, AnalyticsEventUserData.fromXContent(parser, null));
    }

    public void testFromXContent() throws IOException {
        String userId = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(USER_ID_FIELD.getPreferredName(), userId).map();
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap)) {
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder));
            AnalyticsEventUserData user = AnalyticsEventUserData.fromXContent(parser, null);
            assertEquals(userId, user.id());
        }
    }

    public void testFromXContentWhenIdFieldIsMissing() throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, "{}")) {
            expectThrows(
                IllegalArgumentException.class,
                LoggerMessageFormat.format("Required [{}}]", USER_ID_FIELD),
                () -> AnalyticsEventUserData.fromXContent(parser, null)
            );
        }
    }

    public void testFromXContentWhenSessionIdIsBlank() throws IOException {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(USER_ID_FIELD.getPreferredName(), "").map();
        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            Exception e = expectThrows(
                XContentParseException.class,
                LoggerMessageFormat.format("[{}}] failed to parse field [{}}]", USER_FIELD, USER_ID_FIELD),
                () -> AnalyticsEventUserData.fromXContent(parser, null)
            );

            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
            assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", USER_ID_FIELD), e.getCause().getMessage());
        }
    }

    public void testFromXContentWithAnInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .put(USER_ID_FIELD.getPreferredName(), randomIdentifier())
            .put(invalidFieldName, "")
            .map();

        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(jsonMap);
            XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder))
        ) {
            expectThrows(
                XContentParseException.class,
                LoggerMessageFormat.format("[{}}] failed to parse field [{}]", USER_FIELD, invalidFieldName),
                () -> AnalyticsEventUserData.fromXContent(parser, null)
            );
        }
    }

    @Override
    protected Writeable.Reader<AnalyticsEventUserData> instanceReader() {
        return AnalyticsEventUserData::new;
    }

    @Override
    protected AnalyticsEventUserData createTestInstance() {
        return randomEventUserData();
    }

    @Override
    protected AnalyticsEventUserData mutateInstance(AnalyticsEventUserData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}

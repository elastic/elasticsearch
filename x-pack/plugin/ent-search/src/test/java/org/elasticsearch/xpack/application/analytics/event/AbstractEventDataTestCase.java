/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;

public abstract class AbstractEventDataTestCase<T extends ToXContent & Writeable> extends AbstractWireSerializingTestCase<T> {
    public void testToXContent() throws IOException {
        T dataObject = createTestInstance();
        BytesReference json = XContentHelper.toXContent(dataObject, XContentType.JSON, false);

        // Assert XContent contains expected data.
        this.assertXContentData(dataObject, XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2());

        // Check we can serialize again with fromXContent and object are equals.
        assertEquals(dataObject, parseJson(json));
    }

    public void testFromXContentWithAnInvalidField() throws IOException {
        String invalidFieldName = randomIdentifier();

        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
            .putAll(mapFromObject(createTestInstance()))
            .put(invalidFieldName, randomIdentifier())
            .map();

        expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}]", invalidFieldName),
            () -> parseJson(convertMapToJson(jsonMap))
        );
    }

    public void testMissingRequiredField() throws IOException {
        for (String currentField : requiredFields()) {
            Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder()
                .putAll(mapFromObject(createTestInstance()))
                .remove(currentField)
                .map();

            expectThrows(
                IllegalArgumentException.class,
                LoggerMessageFormat.format("Required [{}]", currentField),
                () -> parseJson(convertMapToJson(jsonMap))
            );
        }
    }

    protected abstract ContextParser<AnalyticsEvent.Context, T> parser();

    protected abstract void assertXContentData(T object, Map<String, Object> objectAsMap);

    protected abstract List<String> requiredFields();

    protected T parseJson(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return parser().parse(contentParser, null);
        }
    }

    protected Map<String, Object> mapFromObject(ToXContent object) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            object.toXContent(builder, EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InferenceServiceConfiguration.Features;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class InferenceServiceConfigurationFeaturesTests extends AbstractBWCSerializationTestCase<Features> {

    public static Features randomInstance() {
        return new Features(randomBoolean());
    }

    @Override
    protected Features createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Features doParseInstance(XContentParser parser) throws IOException {
        return Features.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Writeable.Reader<Features> instanceReader() {
        return Features::new;
    }

    @Override
    protected Features mutateInstance(Features instance) {
        return new Features(instance.supportsNonStreamingChat() == false);
    }

    @Override
    protected Features mutateInstanceForVersion(Features instance, TransportVersion version) {
        return instance;
    }

    public void testToXContent_SupportsNonStreamingChatTrue() throws IOException {
        var builder = XContentFactory.jsonBuilder();
        new Features(true).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {"supports_non_streaming_chat":true}
            """)));
    }

    public void testToXContent_SupportsNonStreamingChatFalse() throws IOException {
        var builder = XContentFactory.jsonBuilder();
        new Features(false).toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {"supports_non_streaming_chat":false}
            """)));
    }

    public void testToMap() {
        assertThat(new Features(true).toMap(), is(Map.of("supports_non_streaming_chat", true)));
    }

    public void testFromXContent_ThrowsWhenSupportsNonStreamingChatIsMissing() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), "{}")) {
            expectThrows(IllegalArgumentException.class, () -> Features.fromXContent(parser));
        }
    }

    public void testFromXContent_NestedParseFailureIsWrappedByOuterParser() throws IOException {
        var json = XContentHelper.stripWhitespace("""
            {
              "service": "openai",
              "name": "OpenAI",
              "task_types": ["completion"],
              "configurations": {},
              "features": {}
            }
            """);
        var ex = expectThrows(
            XContentParseException.class,
            () -> InferenceServiceConfiguration.fromXContentBytes(new org.elasticsearch.common.bytes.BytesArray(json), XContentType.JSON)
        );
        assertThat(ex.getMessage(), containsString("[features]"));
    }

    public void testFromXContent_IgnoresUnknownFields() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), """
            {"supports_non_streaming_chat": true, "some_future_field": "x"}
            """)) {
            assertThat(Features.fromXContent(parser), is(new Features(true)));
        }
    }
}

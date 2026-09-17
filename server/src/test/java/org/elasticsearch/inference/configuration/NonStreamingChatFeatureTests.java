/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class NonStreamingChatFeatureTests extends AbstractBWCSerializationTestCase<NonStreamingChatFeature> {

    public static NonStreamingChatFeature randomInstance() {
        return randomBoolean() ? NonStreamingChatFeature.SUPPORTED_INSTANCE : NonStreamingChatFeature.UNSUPPORTED_INSTANCE;
    }

    @Override
    protected NonStreamingChatFeature createTestInstance() {
        return randomInstance();
    }

    @Override
    protected NonStreamingChatFeature doParseInstance(XContentParser parser) throws IOException {
        return NonStreamingChatFeature.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Writeable.Reader<NonStreamingChatFeature> instanceReader() {
        return NonStreamingChatFeature::new;
    }

    @Override
    protected NonStreamingChatFeature mutateInstance(NonStreamingChatFeature instance) {
        return instance.isSupported() ? NonStreamingChatFeature.UNSUPPORTED_INSTANCE : NonStreamingChatFeature.SUPPORTED_INSTANCE;
    }

    @Override
    protected NonStreamingChatFeature mutateInstanceForVersion(NonStreamingChatFeature instance, TransportVersion version) {
        return instance;
    }

    /**
     * Parsing resolves to one of the shared instances, so an XContent round trip returns the very same object while a
     * wire round trip allocates a new one. Identity is therefore not a stable property to assert here — equality is,
     * and {@link #testEquals_HoldsAcrossAWireRoundTrip} pins the allocating path explicitly.
     */
    @Override
    protected void assertEqualInstances(NonStreamingChatFeature expectedInstance, NonStreamingChatFeature newInstance) {
        assertThat(newInstance, is(expectedInstance));
        assertThat(newInstance.hashCode(), is(expectedInstance.hashCode()));
    }

    public void testGetWriteableName() {
        assertThat(NonStreamingChatFeature.SUPPORTED_INSTANCE.getWriteableName(), is("non_streaming_chat"));
    }

    public void testFromXContent_ReturnsTheSharedInstances() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), """
            {"supported": true}
            """)) {
            assertSame(NonStreamingChatFeature.SUPPORTED_INSTANCE, NonStreamingChatFeature.fromXContent(parser));
        }
        try (var parser = createParser(XContentType.JSON.xContent(), """
            {"supported": false}
            """)) {
            assertSame(NonStreamingChatFeature.UNSUPPORTED_INSTANCE, NonStreamingChatFeature.fromXContent(parser));
        }
    }

    public void testEquals_HoldsAcrossAWireRoundTrip() throws IOException {
        // deserialization always allocates, so equality must not rely on identity
        var deserialized = copyInstance(NonStreamingChatFeature.SUPPORTED_INSTANCE);
        assertNotSame(NonStreamingChatFeature.SUPPORTED_INSTANCE, deserialized);
        assertThat(deserialized, is(NonStreamingChatFeature.SUPPORTED_INSTANCE));
    }

    public void testToXContent_Supported() throws IOException {
        var builder = XContentFactory.jsonBuilder();
        NonStreamingChatFeature.SUPPORTED_INSTANCE.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {"supported":true}
            """)));
    }

    public void testToXContent_Unsupported() throws IOException {
        var builder = XContentFactory.jsonBuilder();
        NonStreamingChatFeature.UNSUPPORTED_INSTANCE.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {"supported":false}
            """)));
    }

    public void testFromXContent_ThrowsWhenSupportedIsMissing() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), "{}")) {
            var exception = expectThrows(IllegalArgumentException.class, () -> NonStreamingChatFeature.fromXContent(parser));
            assertThat(exception.getMessage(), containsString("Missing required field [supported]"));
        }
    }

    public void testFromXContent_IgnoresUnknownFields() throws IOException {
        try (var parser = createParser(XContentType.JSON.xContent(), """
            {"supported": true, "some_future_field": "x"}
            """)) {
            assertThat(NonStreamingChatFeature.fromXContent(parser), is(NonStreamingChatFeature.SUPPORTED_INSTANCE));
        }
    }
}

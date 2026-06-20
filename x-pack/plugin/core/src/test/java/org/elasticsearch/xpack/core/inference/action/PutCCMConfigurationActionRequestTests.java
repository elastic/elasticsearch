/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction.API_KEY_FIELD_ERROR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class PutCCMConfigurationActionRequestTests extends AbstractBWCWireSerializationTestCase<PutCCMConfigurationAction.Request> {

    public void testValidate_ApiKeyNull_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(null, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString(API_KEY_FIELD_ERROR));
    }

    public void testValidate_ApiKeyEmpty() {
        var req = new PutCCMConfigurationAction.Request(new SecureString("".toCharArray()), randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString(API_KEY_FIELD_ERROR));
    }

    public void testValidate_ApiKeyValid_DoesNotReturnAnException() {
        var req = new PutCCMConfigurationAction.Request(new SecureString("validkey".toCharArray()), randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNull(ex);
    }

    public void testParse_SuccessfullyParsesApiKeyRequest() throws IOException {
        var testKey = "test_key";
        var requestJson = Strings.format("""
            {
                "api_key": "%s"
            }
            """, testKey);

        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var request = PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser);
            assertThat(request.getApiKey(), is(testKey));
        }
    }

    public void testParse_UnknowField_ReturnsException() throws IOException {
        var requestJson = """
            {
                "api_key": "test_key",
                "some_extra_field": true
            }
            """;
        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var ex = expectThrows(
                XContentParseException.class,
                () -> PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser)
            );
            assertNotNull(ex);
            assertThat(ex.getMessage(), containsString("unknown field [some_extra_field]"));
        }
    }

    public void testParse_EmptyBody_ReturnsException() throws IOException {
        var requestJson = "{}";
        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var request = PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser);
            var ex = request.validate();
            assertNotNull(ex);
            assertThat(ex.getMessage(), containsString(API_KEY_FIELD_ERROR));
        }
    }

    public void testParse_ApiKeyEmpty_ReturnsException() throws IOException {
        var requestJson = """
            {
                "api_key": ""
            }
            """;
        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var request = PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser);
            var ex = request.validate();
            assertNotNull(ex);
            assertThat(ex.getMessage(), containsString(API_KEY_FIELD_ERROR));
        }
    }

    @Override
    protected PutCCMConfigurationAction.Request mutateInstanceForVersion(
        PutCCMConfigurationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected Writeable.Reader<PutCCMConfigurationAction.Request> instanceReader() {
        return PutCCMConfigurationAction.Request::new;
    }

    @Override
    protected PutCCMConfigurationAction.Request createTestInstance() {
        return new PutCCMConfigurationAction.Request(
            new SecureString(randomAlphaOfLength(10).toCharArray()),
            randomTimeValue(),
            randomTimeValue()
        );
    }

    @Override
    protected PutCCMConfigurationAction.Request mutateInstance(PutCCMConfigurationAction.Request instance) throws IOException {
        var apiKey = instance.getApiKey();
        var masterNodeTimeout = instance.masterNodeTimeout();
        var ackTimeout = instance.ackTimeout();

        var newApiKey = apiKey == null ? new SecureString(randomAlphaOfLength(12).toCharArray()) : null;
        return new PutCCMConfigurationAction.Request(newApiKey, masterNodeTimeout, ackTimeout);
    }
}

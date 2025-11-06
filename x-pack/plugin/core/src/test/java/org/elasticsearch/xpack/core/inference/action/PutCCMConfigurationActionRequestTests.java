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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class PutCCMConfigurationActionRequestTests extends AbstractBWCWireSerializationTestCase<PutCCMConfigurationAction.Request> {

    public void testValidate_BothFieldsNull_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(null, null, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("At least one of [api_key] or [enabled] must be provided"));
    }

    public void testValidate_BothFieldsSet_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(
            new SecureString("key".toCharArray()),
            Boolean.TRUE,
            randomTimeValue(),
            randomTimeValue()
        );
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("Only one of [api_key] or [enabled] can be provided but not both"));
    }

    public void testValidate_EnabledTrue_ReturnsException() {
        var req = new PutCCMConfigurationAction.Request(null, Boolean.TRUE, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("The [enabled] field must be set to [false] when disabling CCM"));
    }

    public void testValidate_ApiKeyEmpty() {
        var req = new PutCCMConfigurationAction.Request(new SecureString("".toCharArray()), null, randomTimeValue(), randomTimeValue());
        var ex = req.validate();
        assertNotNull(ex);
        assertThat(ex.getMessage(), containsString("The [api_key] field cannot be an empty string"));
    }

    public void testValidate_ApiKeyValid_DoesNotReturnAnException() {
        var req = new PutCCMConfigurationAction.Request(
            new SecureString("validkey".toCharArray()),
            null,
            randomTimeValue(),
            randomTimeValue()
        );
        var ex = req.validate();
        assertNull(ex);
    }

    public void testValidate_EnabledFalse_DoesNotReturnAnException() {
        var req = new PutCCMConfigurationAction.Request(null, Boolean.FALSE, randomTimeValue(), randomTimeValue());
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
            assertNull(request.getEnabledField());
        }
    }

    public void testParse_SuccessfullyParsesEnabledRequest() throws IOException {
        var requestJson = """
            {
                "enabled": false
            }
            """;
        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var request = PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser);
            assertNull(request.getApiKey());
            assertFalse(request.getEnabledField());
        }
    }

    public void testParse_BothFieldsSet_ReturnsException() throws IOException {
        var requestJson = """
            {
                "api_key": "test_key",
                "enabled": true
            }
            """;
        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var request = PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser);
            var ex = request.validate();
            assertNotNull(ex);
            assertThat(ex.getMessage(), containsString("Only one of [api_key] or [enabled] can be provided but not both"));
        }
    }

    public void testParse_BothFieldsSetWithEnabledFalse_ReturnsException() throws IOException {
        var requestJson = """
            {
                "api_key": "test_key",
                "enabled": false
            }
            """;
        try (
            var parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, requestJson.getBytes(StandardCharsets.UTF_8))
        ) {
            var request = PutCCMConfigurationAction.Request.parseRequest(TimeValue.THIRTY_SECONDS, TimeValue.ZERO, parser);
            var ex = request.validate();
            assertNotNull(ex);
            assertThat(ex.getMessage(), containsString("Only one of [api_key] or [enabled] can be provided but not both"));
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
            assertThat(ex.getMessage(), containsString("At least one of [api_key] or [enabled] must be provided"));
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
            assertThat(ex.getMessage(), containsString("The [api_key] field cannot be an empty string"));
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
            randomOptionalBoolean(),
            randomTimeValue(),
            randomTimeValue()
        );
    }

    @Override
    protected PutCCMConfigurationAction.Request mutateInstance(PutCCMConfigurationAction.Request instance) throws IOException {
        var apiKey = instance.getApiKey();
        var enabled = instance.getEnabledField();
        var masterNodeTimeout = instance.masterNodeTimeout();
        var ackTimeout = instance.ackTimeout();

        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                var newApiKey = apiKey == null ? new SecureString(randomAlphaOfLength(12).toCharArray()) : null;
                yield new PutCCMConfigurationAction.Request(newApiKey, enabled, masterNodeTimeout, ackTimeout);
            }
            case 1 -> {
                var newEnabled = (enabled == null) ? randomBoolean() : null;
                yield new PutCCMConfigurationAction.Request(apiKey, newEnabled, masterNodeTimeout, ackTimeout);
            }
            case 2 -> new PutCCMConfigurationAction.Request(null, null, masterNodeTimeout, ackTimeout);
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}

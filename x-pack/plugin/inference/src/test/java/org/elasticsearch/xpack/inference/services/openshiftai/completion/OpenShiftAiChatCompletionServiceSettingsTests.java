/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class OpenShiftAiChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    OpenShiftAiChatCompletionServiceSettings> {

    public static final String MODEL_ID = "some model";
    public static final String CORRECT_URL = "https://www.elastic.co";
    public static final int RATE_LIMIT = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_ID,
                    ServiceFields.URL,
                    CORRECT_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new OpenShiftAiChatCompletionServiceSettings(MODEL_ID, CORRECT_URL, new RateLimitSettings(RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_Success() {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    CORRECT_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new OpenShiftAiChatCompletionServiceSettings(null, CORRECT_URL, new RateLimitSettings(RATE_LIMIT))));
    }

    public void testFromMap_MissingUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenShiftAiChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        MODEL_ID,
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [url];")
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID, ServiceFields.URL, CORRECT_URL)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new OpenShiftAiChatCompletionServiceSettings(MODEL_ID, CORRECT_URL, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_ID,
                    ServiceFields.URL,
                    CORRECT_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "model_id": "some model",
                "url": "https://www.elastic.co",
                "rate_limit": {
                    "requests_per_minute": 2
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = OpenShiftAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, CORRECT_URL)),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "url": "https://www.elastic.co",
                "rate_limit": {
                    "requests_per_minute": 3000
                }
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<OpenShiftAiChatCompletionServiceSettings> instanceReader() {
        return OpenShiftAiChatCompletionServiceSettings::new;
    }

    @Override
    protected OpenShiftAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected OpenShiftAiChatCompletionServiceSettings mutateInstance(OpenShiftAiChatCompletionServiceSettings instance)
        throws IOException {
        return randomValueOtherThan(instance, OpenShiftAiChatCompletionServiceSettingsTests::createRandom);
    }

    @Override
    protected OpenShiftAiChatCompletionServiceSettings mutateInstanceForVersion(
        OpenShiftAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static OpenShiftAiChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLengthOrNull(8);
        var url = randomAlphaOfLength(15);
        return new OpenShiftAiChatCompletionServiceSettings(modelId, ServiceUtils.createUri(url), RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(String model, String url) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.MODEL_ID, model);
        map.put(ServiceFields.URL, url);

        return map;
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completion;

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
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class MistralChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<MistralChatCompletionServiceSettings> {

    public static final String MODEL_ID = "some model";
    public static final int RATE_LIMIT = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    MistralConstants.MODEL_FIELD,
                    MODEL_ID,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new MistralChatCompletionServiceSettings(
                    MODEL_ID,

                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT)))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model];")
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(MistralConstants.MODEL_FIELD, MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new MistralChatCompletionServiceSettings(MODEL_ID, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    MistralConstants.MODEL_FIELD,
                    MODEL_ID,
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
                "model": "some model",
                "rate_limit": {
                    "requests_per_minute": 2
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = MistralChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(MistralConstants.MODEL_FIELD, MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "model": "some model",
                "rate_limit": {
                    "requests_per_minute": 240
                }
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<MistralChatCompletionServiceSettings> instanceReader() {
        return MistralChatCompletionServiceSettings::new;
    }

    @Override
    protected MistralChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MistralChatCompletionServiceSettings mutateInstance(MistralChatCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, MistralChatCompletionServiceSettingsTests::createRandom);
    }

    @Override
    protected MistralChatCompletionServiceSettings mutateInstanceForVersion(
        MistralChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static MistralChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);

        return new MistralChatCompletionServiceSettings(modelId, RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        var map = new HashMap<String, Object>();

        map.put(MistralConstants.MODEL_FIELD, model);

        return map;
    }
}

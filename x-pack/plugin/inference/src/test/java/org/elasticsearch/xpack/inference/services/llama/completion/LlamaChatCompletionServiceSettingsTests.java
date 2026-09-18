/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.llama.AbstractLlamaServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.is;

public class LlamaChatCompletionServiceSettingsTests extends AbstractLlamaServiceSettingsTests<LlamaChatCompletionServiceSettings> {

    @Override
    protected LlamaChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return LlamaChatCompletionServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable Integer rateLimit
    ) {
        return buildServiceSettingsMap(modelId, url, rateLimit);
    }

    @Override
    protected LlamaChatCompletionServiceSettings createServiceSettings(String modelId, URI uri, RateLimitSettings rateLimitSettings) {
        return new LlamaChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = LlamaChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new LlamaChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = LlamaChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<LlamaChatCompletionServiceSettings> instanceReader() {
        return LlamaChatCompletionServiceSettings::new;
    }

    @Override
    protected LlamaChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected LlamaChatCompletionServiceSettings mutateInstance(LlamaChatCompletionServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(10) + ".example"));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new LlamaChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected LlamaChatCompletionServiceSettings mutateInstanceForVersion(
        LlamaChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected LlamaChatCompletionServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return LlamaChatCompletionServiceSettings.createParser(true).apply(parser, ConfigurationParseContext.PERSISTENT).build();
    }

    private static LlamaChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var uri = createUri("https://" + randomAlphaOfLength(10) + ".example");
        return new LlamaChatCompletionServiceSettings(modelId, uri, RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> buildServiceSettingsMap(@Nullable String modelId, @Nullable String url, @Nullable Integer rateLimit) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class CohereRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereRerankServiceSettings> {

    private static final TransportVersion ML_INFERENCE_COHERE_API_VERSION = TransportVersion.fromName("ml_inference_cohere_api_version");

    public static CohereRerankServiceSettings createRandom() {
        return createRandom(randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }));
    }

    public static CohereRerankServiceSettings createRandom(@Nullable RateLimitSettings rateLimitSettings) {
        return new CohereRerankServiceSettings(
            randomFrom(new String[] { null, Strings.format("http://%s.com", randomAlphaOfLength(8)) }),
            randomAlphaOfLengthOrNull(10),
            rateLimitSettings,
            CohereServiceSettings.CohereApiVersion.V2
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var url = "http://www.abc.com";
        var model = "model";

        var serviceSettings = new CohereRerankServiceSettings(url, model, null, CohereServiceSettings.CohereApiVersion.V2);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "url":"http://www.abc.com",
                "model_id":"model",
                "rate_limit": {
                    "requests_per_minute": 10000
                },
                "api_version": "V2"
            }
            """));
    }

    @Override
    protected Writeable.Reader<CohereRerankServiceSettings> instanceReader() {
        return CohereRerankServiceSettings::new;
    }

    @Override
    protected CohereRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereRerankServiceSettings mutateInstance(CohereRerankServiceSettings instance) throws IOException {
        URI uri = instance.uri();
        var uriString = uri == null ? null : uri.toString();
        var modelId = instance.modelId();
        var rateLimitSettings = instance.rateLimitSettings();
        var apiVersion = instance.apiVersion();
        switch (randomInt(3)) {
            case 0 -> uriString = randomValueOtherThan(uriString, () -> randomAlphaOfLengthOrNull(8));
            case 1 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 3 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomFrom(CohereServiceSettings.CohereApiVersion.values()));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CohereRerankServiceSettings(uriString, modelId, rateLimitSettings, apiVersion);
    }

    @Override
    protected CohereRerankServiceSettings mutateInstanceForVersion(CohereRerankServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereRerankServiceSettings(
                instance.uri(),
                instance.modelId(),
                instance.rateLimitSettings(),
                CohereServiceSettings.CohereApiVersion.V1
            );
        }
        return instance;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, @Nullable String model) {
        return new HashMap<>(CohereServiceSettingsTests.getServiceSettingsMap(url, model));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CohereCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<CohereCompletionServiceSettings> {

    public static CohereCompletionServiceSettings createRandom() {
        return new CohereCompletionServiceSettings(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            RateLimitSettingsTests.createRandom(),
            randomFrom(CohereServiceSettings.CohereApiVersion.values())
        );
    }

    public void testFromMap_WithRateLimitSettingsNull() {
        var url = "https://www.abc.com";
        var model = "model";

        var serviceSettings = CohereCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, url, ServiceFields.MODEL_ID, model)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new CohereCompletionServiceSettings(url, model, null, CohereServiceSettings.CohereApiVersion.V1)));
    }

    public void testFromMap_WithRateLimitSettings() {
        var url = "https://www.abc.com";
        var model = "model";
        var requestsPerMinute = 100;

        var serviceSettings = CohereCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    ServiceFields.MODEL_ID,
                    model,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, requestsPerMinute))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereCompletionServiceSettings(
                    url,
                    model,
                    new RateLimitSettings(requestsPerMinute),
                    CohereServiceSettings.CohereApiVersion.V1
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereCompletionServiceSettings(
            "url",
            "model",
            new RateLimitSettings(3),
            CohereServiceSettings.CohereApiVersion.V1
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"url":"url","model_id":"model","rate_limit":{"requests_per_minute":3},"api_version":"V1"}"""));
    }

    @Override
    protected Writeable.Reader<CohereCompletionServiceSettings> instanceReader() {
        return CohereCompletionServiceSettings::new;
    }

    @Override
    protected CohereCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereCompletionServiceSettings mutateInstance(CohereCompletionServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected CohereCompletionServiceSettings mutateInstanceForVersion(CohereCompletionServiceSettings instance, TransportVersion version) {
        if (version.before(TransportVersions.ML_INFERENCE_COHERE_API_VERSION)
            || (version.isPatchFrom(TransportVersions.ML_INFERENCE_COHERE_API_VERSION_8_19) == false)) {
            return new CohereCompletionServiceSettings(
                instance.uri(),
                instance.modelId(),
                instance.rateLimitSettings(),
                CohereServiceSettings.CohereApiVersion.V1
            );
        }

        return instance;
    }
}

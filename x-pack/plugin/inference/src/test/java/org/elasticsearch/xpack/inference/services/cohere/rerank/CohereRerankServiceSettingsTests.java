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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.AbstractCohereServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.hamcrest.Matchers.is;

public class CohereRerankServiceSettingsTests extends AbstractCohereServiceSettingsTests<CohereRerankServiceSettings> {

    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final RateLimitSettings DEFAULT_COHERE_RERANK_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereRerankServiceSettings createRandom() {
        var apiVersion = randomFrom(CohereCommonServiceSettings.CohereApiVersion.values());
        var modelId = apiVersion == CohereCommonServiceSettings.CohereApiVersion.V2
            ? randomAlphaOfLength(10)
            : randomAlphaOfLengthOrNull(10);
        return new CohereRerankServiceSettings(
            new CohereCommonServiceSettings(modelId, randomFrom(RateLimitSettingsTests.createRandom(), null), apiVersion)
        );
    }

    @Override
    protected CohereRerankServiceSettings createGivenCommonSettings(Map<String, Object> commonSettings, ConfigurationParseContext context) {
        return CohereRerankServiceSettings.fromMap(new HashMap<>(commonSettings), context);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(CohereRerankServiceSettings instance, XContentBuilder builder)
        throws IOException {
        return instance.toXContentFragmentOfExposedFields(builder, null);
    }

    public void testUpdateServiceSettings_AllUpdatableFields() {
        var originalServiceSettings = new CohereRerankServiceSettings(
            new CohereCommonServiceSettings(
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
            )
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
        );

        assertThat(
            updatedServiceSettings,
            is(
                new CohereRerankServiceSettings(
                    new CohereCommonServiceSettings(
                        INITIAL_TEST_MODEL_ID,
                        new RateLimitSettings(TEST_RATE_LIMIT),
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    )
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new CohereRerankServiceSettings(
            new CohereCommonServiceSettings(
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
            )
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Persistent_EmptyMap_CreatesSettingsCorrectly() {
        var serviceSettings = CohereRerankServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new CohereRerankServiceSettings(
                    new CohereCommonServiceSettings(
                        null,
                        DEFAULT_COHERE_RERANK_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    )
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereRerankServiceSettings.fromMap(
            buildServiceSettingsMap(CohereCommonServiceSettings.CohereApiVersion.V2.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereRerankServiceSettings(
                    new CohereCommonServiceSettings(
                        TEST_MODEL_ID,
                        new RateLimitSettings(TEST_RATE_LIMIT),
                        CohereCommonServiceSettings.CohereApiVersion.V2
                    )
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new CohereRerankServiceSettings(
            new CohereCommonServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V2
            )
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "model_id": "%s",
              "rate_limit": {
                "requests_per_minute": %d
              },
              "api_version": "%s"
            }
            """, TEST_MODEL_ID, TEST_RATE_LIMIT, CohereCommonServiceSettings.CohereApiVersion.V2))));
    }

    private static HashMap<String, Object> buildServiceSettingsMap(@Nullable String apiVersion) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.MODEL_ID, CohereRerankServiceSettingsTests.TEST_MODEL_ID);
        if (apiVersion != null) {
            result.put(CohereCommonServiceSettings.API_VERSION, apiVersion);
        }
        result.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, CohereRerankServiceSettingsTests.TEST_RATE_LIMIT))
        );
        return result;
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
        var commonSettings = instance.commonSettings();
        commonSettings = randomValueOtherThan(instance.commonSettings(), () -> CohereCommonServiceSettingsTests.createRandom());
        return new CohereRerankServiceSettings(commonSettings);
    }

    @Override
    protected CohereRerankServiceSettings mutateInstanceForVersion(CohereRerankServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereRerankServiceSettings(
                new CohereCommonServiceSettings(
                    instance.modelId(),
                    instance.rateLimitSettings(),
                    CohereCommonServiceSettings.CohereApiVersion.V1
                )
            );
        }
        return instance;
    }

    @Override
    protected CohereRerankServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return CohereRerankServiceSettings.createParser(ignoreUnknownFields, PARSE_CONTEXT).apply(parser, PARSE_CONTEXT).build();
    }
}

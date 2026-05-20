/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.completion;

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

public class CohereCompletionServiceSettingsTests extends AbstractCohereServiceSettingsTests<CohereCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final RateLimitSettings DEFAULT_COHERE_COMPLETION_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CohereCompletionServiceSettings createRandom() {
        var apiVersion = randomFrom(CohereCommonServiceSettings.CohereApiVersion.values());
        var modelId = apiVersion == CohereCommonServiceSettings.CohereApiVersion.V2 ? randomAlphaOfLength(8) : randomAlphaOfLengthOrNull(8);
        return new CohereCompletionServiceSettings(
            new CohereCommonServiceSettings(modelId, RateLimitSettingsTests.createRandom(), apiVersion)
        );
    }

    @Override
    protected CohereCompletionServiceSettings createGivenCommonSettings(
        Map<String, Object> commonSettings,
        ConfigurationParseContext context
    ) {
        return CohereCompletionServiceSettings.fromMap(new HashMap<>(commonSettings), context);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(CohereCompletionServiceSettings instance, XContentBuilder builder)
        throws IOException {
        return instance.toXContentFragmentOfExposedFields(builder, null);
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(CohereCommonServiceSettings.CohereApiVersion.V2.toString());

        var originalServiceSettings = new CohereCompletionServiceSettings(
            new CohereCommonServiceSettings(
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
            )
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new CohereCompletionServiceSettings(
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
        var originalServiceSettings = new CohereCompletionServiceSettings(
            new CohereCommonServiceSettings(
                INITIAL_TEST_MODEL_ID,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
            )
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Persistent_EmptyMap_CreatesSettingsCorrectly() {
        var serviceSettings = CohereCompletionServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(
                new CohereCompletionServiceSettings(
                    new CohereCommonServiceSettings(
                        null,
                        DEFAULT_COHERE_COMPLETION_RATE_LIMIT_SETTINGS,
                        CohereCommonServiceSettings.CohereApiVersion.V1
                    )
                )
            )
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = CohereCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(CohereCommonServiceSettings.CohereApiVersion.V2.toString()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new CohereCompletionServiceSettings(
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
        var serviceSettings = new CohereCompletionServiceSettings(
            new CohereCommonServiceSettings(
                TEST_MODEL_ID,
                new RateLimitSettings(TEST_RATE_LIMIT),
                CohereCommonServiceSettings.CohereApiVersion.V1
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
            """, TEST_MODEL_ID, TEST_RATE_LIMIT, CohereCommonServiceSettings.CohereApiVersion.V1))));
    }

    private static HashMap<String, Object> buildServiceSettingsMap(@Nullable String apiVersion) {
        var result = new HashMap<String, Object>();
        result.put(ServiceFields.MODEL_ID, CohereCompletionServiceSettingsTests.TEST_MODEL_ID);
        if (apiVersion != null) {
            result.put(CohereCommonServiceSettings.API_VERSION, apiVersion);
        }
        result.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, CohereCompletionServiceSettingsTests.TEST_RATE_LIMIT))
        );
        return result;
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
        var commonSettings = instance.commonSettings();
        commonSettings = randomValueOtherThan(instance.commonSettings(), () -> CohereCommonServiceSettingsTests.createRandom());
        return new CohereCompletionServiceSettings(commonSettings);
    }

    @Override
    protected CohereCompletionServiceSettings mutateInstanceForVersion(CohereCompletionServiceSettings instance, TransportVersion version) {
        if (version.supports(ML_INFERENCE_COHERE_API_VERSION) == false) {
            return new CohereCompletionServiceSettings(
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
    protected CohereCompletionServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return CohereCompletionServiceSettings.createParser(ignoreUnknownFields, PARSE_CONTEXT)
            .apply(parser, PARSE_CONTEXT)
            .build(PARSE_CONTEXT);
    }
}

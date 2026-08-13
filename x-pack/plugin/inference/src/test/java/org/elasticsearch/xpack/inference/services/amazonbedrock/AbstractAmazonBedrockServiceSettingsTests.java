/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.REGION_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

/**
 * Base test case for {@link AmazonBedrockServiceSettings} subclasses. Holds the assertions for the fields common to every Bedrock task
 * (model identity, endpoint URL, and rate limiting) so they are exercised once for each task type instead of being duplicated in
 * every concrete settings test. Task-specific tests live in the concrete subclasses.
 */
public abstract class AbstractAmazonBedrockServiceSettingsTests<T extends AmazonBedrockServiceSettings> extends
    AbstractBWCSerializationTestCase<T> {

    public static final String TEST_REGION = "test-region";
    protected static final String INITIAL_TEST_REGION = "initial-test-region";
    public static final String TEST_MODEL_ID = "test-model-id";
    protected static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    public static final AmazonBedrockProvider TEST_PROVIDER = AmazonBedrockProvider.AMAZONTITAN;
    protected static final AmazonBedrockProvider INITIAL_TEST_PROVIDER = AmazonBedrockProvider.AI21LABS;
    public static final int TEST_RATE_LIMIT = 20;
    protected static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 240;

    /**
     * Parses a settings instance from a settings map, mirroring the concrete subclass's {@code fromMap} entry point.
     */
    protected abstract T fromMap(Map<String, Object> map, ConfigurationParseContext context);

    /**
     * Builds a settings map populated with only the common fields, leaving any task-specific fields unset.
     */
    protected abstract Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String region,
        @Nullable String model,
        @Nullable String provider,
        @Nullable Integer rateLimit
    );

    /**
     * Creates a settings instance with the given common fields and defaults (typically {@code null}) for any task-specific fields.
     */
    protected abstract T createServiceSettings(
        String region,
        String model,
        AmazonBedrockProvider provider,
        RateLimitSettings rateLimitSettings
    );

    /**
     * The task-specific immutable fields an update request must reject, in addition to the common {@code region}, {@code model} and
     * {@code provider} fields. Subclasses override this when they declare additional immutable fields.
     */
    protected List<String> additionalImmutableFields() {
        return List.of();
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = fromMap(
            buildCommonServiceSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString(), DEFAULT_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(createServiceSettings(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_EmptyRateLimitObject_UsesDefaultValue() {
        var map = buildCommonServiceSettingsMap(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER.toString(), null);
        map.put(RateLimitSettings.FIELD_NAME, new HashMap<>());

        var serviceSettings = fromMap(map, randomFrom(ConfigurationParseContext.values()));

        assertThat(
            serviceSettings,
            is(createServiceSettings(TEST_REGION, TEST_MODEL_ID, TEST_PROVIDER, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_NoRegion_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(null, TEST_MODEL_ID, TEST_PROVIDER.toString(), TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[%s] does not contain the required setting [%s]", ModelConfigurations.SERVICE_SETTINGS, REGION_FIELD))
        );
    }

    public void testFromMap_NoModel_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_REGION, null, TEST_PROVIDER.toString(), TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[%s] does not contain the required setting [%s]", ModelConfigurations.SERVICE_SETTINGS, MODEL_FIELD))
        );
    }

    public void testFromMap_NoProvider_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_REGION, TEST_MODEL_ID, null, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[%s] does not contain the required setting [%s]", ModelConfigurations.SERVICE_SETTINGS, PROVIDER_FIELD))
        );
    }

    public void testFromMap_InvalidProvider_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_REGION, TEST_MODEL_ID, "invalid_provider", TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.getMessage(), containsString("No enum constant"));
    }

    public void testUpdateServiceSettings_RateLimit_IsUpdated() {
        var originalServiceSettings = createServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)))
        );

        assertThat(
            updatedServiceSettings,
            is(
                createServiceSettings(
                    INITIAL_TEST_REGION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROVIDER,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = createServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var serviceSettings = createServiceSettings(
            INITIAL_TEST_REGION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROVIDER,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var immutableFields = new ArrayList<>(List.of(REGION_FIELD, MODEL_FIELD, PROVIDER_FIELD));
        immutableFields.addAll(additionalImmutableFields());
        for (String immutableField : immutableFields) {
            var e = expectThrows(
                XContentParseException.class,
                () -> serviceSettings.updateServiceSettings(new HashMap<>(Map.of(immutableField, "value")))
            );
            assertThat(
                e.getMessage(),
                endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, immutableField))
            );
        }
    }
}

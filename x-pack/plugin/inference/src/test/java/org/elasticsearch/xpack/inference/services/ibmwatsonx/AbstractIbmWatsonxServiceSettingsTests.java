/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

/**
 * Base test case for {@link IbmWatsonxServiceSettings} subclasses. Holds the assertions for the fields common to every IBM watsonx
 * task (endpoint URL, API version, model identity, project, and rate limiting) so they are exercised once for each task type instead
 * of being duplicated in every concrete settings test. Task-specific tests live in the concrete subclasses.
 */
public abstract class AbstractIbmWatsonxServiceSettingsTests<T extends IbmWatsonxServiceSettings> extends AbstractBWCSerializationTestCase<
    T> {

    protected static final URI TEST_URI = URI.create("https://www.test.com");
    protected static final URI INITIAL_TEST_URI = URI.create("https://www.initial.com");

    protected static final String TEST_MODEL_ID = "test-model";
    protected static final String INITIAL_TEST_MODEL_ID = "initial-model";

    protected static final String TEST_PROJECT_ID = "test-project";
    protected static final String INITIAL_TEST_PROJECT_ID = "initial-project";

    protected static final String TEST_API_VERSION = "2024-06-01";
    protected static final String INITIAL_TEST_API_VERSION = "2024-05-02";

    protected static final int TEST_RATE_LIMIT = 2;
    protected static final int INITIAL_TEST_RATE_LIMIT = 5;
    protected static final int DEFAULT_RATE_LIMIT = 120;

    /**
     * Parses a settings instance from a settings map, mirroring the concrete subclass's {@code fromMap} entry point.
     */
    protected abstract T fromMap(Map<String, Object> map, ConfigurationParseContext context);

    /**
     * Builds a settings map populated with only the common fields, leaving any task-specific fields unset.
     */
    protected abstract Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String projectId,
        @Nullable String url,
        @Nullable String apiVersion,
        @Nullable Integer rateLimit
    );

    /**
     * Creates a settings instance with the given common fields and defaults (typically {@code null}) for any task-specific fields.
     */
    protected abstract T createServiceSettings(
        URI uri,
        String apiVersion,
        String modelId,
        String projectId,
        RateLimitSettings rateLimitSettings
    );

    /**
     * The task-specific immutable fields an update request must reject, in addition to the common {@code url}, {@code api_version},
     * {@code model_id}, and {@code project_id} fields. Subclasses override this when they declare additional immutable fields.
     */
    protected List<String> additionalImmutableFields() {
        return List.of();
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = fromMap(
            buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, TEST_URI.toString(), TEST_API_VERSION, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(createServiceSettings(TEST_URI, TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_EmptyRateLimitObject_UsesDefaultValue() {
        var map = buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, TEST_URI.toString(), TEST_API_VERSION, null);
        map.put(RateLimitSettings.FIELD_NAME, new HashMap<>());

        var serviceSettings = fromMap(map, randomFrom(ConfigurationParseContext.values()));

        assertThat(
            serviceSettings,
            is(createServiceSettings(TEST_URI, TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_NoUrl_ThrowsException() {
        assertFromMap_MissingRequiredField_ThrowsException(
            buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, null, TEST_API_VERSION, TEST_RATE_LIMIT),
            ServiceFields.URL
        );
    }

    public void testFromMap_NoApiVersion_ThrowsException() {
        assertFromMap_MissingRequiredField_ThrowsException(
            buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, TEST_URI.toString(), null, TEST_RATE_LIMIT),
            IbmWatsonxServiceFields.API_VERSION
        );
    }

    public void testFromMap_NoModelId_ThrowsException() {
        assertFromMap_MissingRequiredField_ThrowsException(
            buildCommonServiceSettingsMap(null, TEST_PROJECT_ID, TEST_URI.toString(), TEST_API_VERSION, TEST_RATE_LIMIT),
            ServiceFields.MODEL_ID
        );
    }

    public void testFromMap_NoProjectId_ThrowsException() {
        assertFromMap_MissingRequiredField_ThrowsException(
            buildCommonServiceSettingsMap(TEST_MODEL_ID, null, TEST_URI.toString(), TEST_API_VERSION, TEST_RATE_LIMIT),
            IbmWatsonxServiceFields.PROJECT_ID
        );
    }

    private void assertFromMap_MissingRequiredField_ThrowsException(Map<String, Object> map, String missingFieldName) {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(map, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[%s] does not contain the required setting [%s]", ModelConfigurations.SERVICE_SETTINGS, missingFieldName))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, "", TEST_API_VERSION, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        var invalidUrl = "^^^";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_MODEL_ID, TEST_PROJECT_ID, invalidUrl, TEST_API_VERSION, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("unable to parse url [%s]. Reason: Illegal character in path", invalidUrl))
        );
    }

    public void testUpdateServiceSettings_RateLimit_IsUpdated() {
        var originalServiceSettings = createServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)))
        );

        assertThat(
            updatedServiceSettings,
            is(
                createServiceSettings(
                    INITIAL_TEST_URI,
                    INITIAL_TEST_API_VERSION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROJECT_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = createServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_EmptyRateLimitObject_RevertsToDefault() {
        var originalServiceSettings = createServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(RateLimitSettings.FIELD_NAME, new HashMap<>()))
        );

        assertThat(
            updatedServiceSettings,
            is(
                createServiceSettings(
                    INITIAL_TEST_URI,
                    INITIAL_TEST_API_VERSION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROJECT_ID,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_ExplicitNullRateLimit_RevertsToDefault() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(RateLimitSettings.FIELD_NAME, null);
        var originalServiceSettings = createServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        assertThat(
            originalServiceSettings.updateServiceSettings(settingsMap),
            is(
                createServiceSettings(
                    INITIAL_TEST_URI,
                    INITIAL_TEST_API_VERSION,
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_PROJECT_ID,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var serviceSettings = createServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var immutableFields = new ArrayList<>(
            List.of(ServiceFields.URL, IbmWatsonxServiceFields.API_VERSION, ServiceFields.MODEL_ID, IbmWatsonxServiceFields.PROJECT_ID)
        );
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

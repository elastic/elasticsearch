/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

/**
 * Base test case for the task-specific AlibabaCloud AI Search service settings. Holds the assertions for the fields common to every
 * AlibabaCloud AI Search task (service id, host, workspace, HTTP schema, and rate limiting) so they are exercised once for each task
 * type instead of being duplicated in every concrete settings test. Task-specific tests live in the concrete subclasses.
 */
public abstract class AbstractAlibabaCloudSearchServiceSettingsTests<T extends ServiceSettings> extends
    AbstractBWCWireSerializationTestCase<T> {

    public static final String TEST_SERVICE_ID = "test-service-id";
    protected static final String INITIAL_TEST_SERVICE_ID = "initial-test-service-id";
    public static final String TEST_HOST = "test-host";
    protected static final String INITIAL_TEST_HOST = "initial-test-host";
    public static final String TEST_WORKSPACE_NAME = "test-workspace-name";
    protected static final String INITIAL_TEST_WORKSPACE_NAME = "initial-test-workspace-name";
    public static final String TEST_HTTP_SCHEMA = "https";
    protected static final String INITIAL_TEST_HTTP_SCHEMA = "http";
    public static final int TEST_RATE_LIMIT = 20;
    protected static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 1_000;

    /**
     * Parses a settings instance from a settings map, mirroring the concrete subclass's {@code fromMap} entry point.
     */
    protected abstract T fromMap(Map<String, Object> map, ConfigurationParseContext context);

    /**
     * Creates a settings instance wrapping the given common settings and defaults (typically {@code null}) for any task-specific
     * fields.
     */
    protected abstract T createServiceSettings(AlibabaCloudSearchServiceSettings commonSettings);

    /**
     * The task-specific immutable fields an update request must reject, in addition to the common {@code service_id}, {@code host} and
     * {@code workspace} fields. Subclasses override this when they declare additional immutable fields.
     */
    protected List<String> additionalImmutableFields() {
        return List.of();
    }

    /**
     * Builds a settings map populated with only the common fields, leaving any task-specific fields unset.
     */
    public static Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String serviceId,
        @Nullable String host,
        @Nullable String workspaceName,
        @Nullable String httpSchema,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (serviceId != null) {
            map.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, serviceId);
        }
        if (host != null) {
            map.put(AlibabaCloudSearchServiceSettings.HOST, host);
        }
        if (workspaceName != null) {
            map.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, workspaceName);
        }
        if (httpSchema != null) {
            map.put(AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME, httpSchema);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }

    protected static AlibabaCloudSearchServiceSettings initialCommonServiceSettings() {
        return new AlibabaCloudSearchServiceSettings(
            INITIAL_TEST_SERVICE_ID,
            INITIAL_TEST_HOST,
            INITIAL_TEST_WORKSPACE_NAME,
            INITIAL_TEST_HTTP_SCHEMA,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = fromMap(
            buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(createServiceSettings(new AlibabaCloudSearchServiceSettings(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null)))
        );
    }

    public void testFromMap_AllCommonFields_Success() {
        var serviceSettings = fromMap(
            buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, TEST_HTTP_SCHEMA, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                createServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        TEST_SERVICE_ID,
                        TEST_HOST,
                        TEST_WORKSPACE_NAME,
                        TEST_HTTP_SCHEMA,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    )
                )
            )
        );
    }

    public void testFromMap_EmptyRateLimitObject_UsesDefaultValue() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null);
        map.put(RateLimitSettings.FIELD_NAME, new HashMap<>());

        var serviceSettings = fromMap(map, randomFrom(ConfigurationParseContext.values()));

        assertThat(
            serviceSettings,
            is(
                createServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        TEST_SERVICE_ID,
                        TEST_HOST,
                        TEST_WORKSPACE_NAME,
                        null,
                        new RateLimitSettings(DEFAULT_RATE_LIMIT)
                    )
                )
            )
        );
    }

    public void testFromMap_ApiKeyInServiceSettings_IsIgnored() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null);
        map.put(DefaultSecretSettings.API_KEY, "secret");

        var serviceSettings = fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(createServiceSettings(new AlibabaCloudSearchServiceSettings(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null)))
        );
    }

    public void testFromMap_NoServiceId_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(null, TEST_HOST, TEST_WORKSPACE_NAME, TEST_HTTP_SCHEMA, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] does not contain the required setting [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID
                )
            )
        );
    }

    public void testFromMap_NoHost_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_SERVICE_ID, null, TEST_WORKSPACE_NAME, TEST_HTTP_SCHEMA, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] does not contain the required setting [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    AlibabaCloudSearchServiceSettings.HOST
                )
            )
        );
    }

    public void testFromMap_NoWorkspace_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, null, TEST_HTTP_SCHEMA, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] does not contain the required setting [%s]",
                    ModelConfigurations.SERVICE_SETTINGS,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME
                )
            )
        );
    }

    public void testFromMap_InvalidHttpSchema_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> fromMap(
                buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, "invalid-http-schema", TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.getMessage(), is("Invalid value for [http_schema]. Must be one of [https, http]"));
    }

    public void testFromMap_UnknownField_RequestContext_ThrowsException() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null);
        map.put("extra_key", "value");

        var thrownException = expectThrows(XContentParseException.class, () -> fromMap(map, ConfigurationParseContext.REQUEST));

        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] unknown field [extra_key]", ModelConfigurations.SERVICE_SETTINGS))
        );
    }

    public void testFromMap_UnknownField_PersistentContext_IsIgnored() {
        var map = buildCommonServiceSettingsMap(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null);
        map.put("extra_key", "value");

        var serviceSettings = fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(createServiceSettings(new AlibabaCloudSearchServiceSettings(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, null, null)))
        );
    }

    public void testUpdateServiceSettings_ApiKey_IsIgnored() {
        var originalServiceSettings = createServiceSettings(initialCommonServiceSettings());
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(Map.of(DefaultSecretSettings.API_KEY, "secret-key"))
        );

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_MutableCommonFields_AreUpdated() {
        var originalServiceSettings = createServiceSettings(initialCommonServiceSettings());
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                createServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        INITIAL_TEST_SERVICE_ID,
                        INITIAL_TEST_HOST,
                        INITIAL_TEST_WORKSPACE_NAME,
                        TEST_HTTP_SCHEMA,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    )
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = createServiceSettings(initialCommonServiceSettings());

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_ExplicitNulls_ResetMutableCommonFieldsToDefaults() {
        var originalServiceSettings = createServiceSettings(initialCommonServiceSettings());

        var update = new HashMap<String, Object>();
        update.put(AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME, null);
        update.put(RateLimitSettings.FIELD_NAME, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(update);

        assertThat(
            updatedServiceSettings,
            is(
                createServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        INITIAL_TEST_SERVICE_ID,
                        INITIAL_TEST_HOST,
                        INITIAL_TEST_WORKSPACE_NAME,
                        null,
                        new RateLimitSettings(DEFAULT_RATE_LIMIT)
                    )
                )
            )
        );
    }

    public void testUpdateServiceSettings_InvalidHttpSchema_ThrowsException() {
        var originalServiceSettings = createServiceSettings(initialCommonServiceSettings());

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> originalServiceSettings.updateServiceSettings(
                new HashMap<>(Map.of(AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME, "invalid-http-schema"))
            )
        );

        assertThat(
            thrownException.getCause().getMessage(),
            containsString("Invalid value for [http_schema]. Must be one of [https, http]")
        );
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var serviceSettings = createServiceSettings(initialCommonServiceSettings());

        var immutableFields = new ArrayList<>(
            List.of(
                AlibabaCloudSearchServiceSettings.SERVICE_ID,
                AlibabaCloudSearchServiceSettings.HOST,
                AlibabaCloudSearchServiceSettings.WORKSPACE_NAME
            )
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

    @Override
    protected T mutateInstanceForVersion(T instance, TransportVersion version) {
        return instance;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.is;

public class IbmWatsonxRerankServiceSettingsTests extends AbstractWireSerializingTestCase<IbmWatsonxRerankServiceSettings> {

    private static final URI TEST_URI = URI.create("https://test.rerank.example");
    private static final URI INITIAL_TEST_URI = URI.create("https://initial.rerank.example");

    private static final String TEST_MODEL_ID = "test-model";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model";

    private static final String TEST_PROJECT_ID = "test-project";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-project";

    private static final String TEST_API_VERSION = "2024-06-01";
    private static final String INITIAL_TEST_API_VERSION = "2024-05-02";

    private static final int TEST_RATE_LIMIT = 500;
    private static final int INITIAL_TEST_RATE_LIMIT = 250;
    private static final int DEFAULT_RATE_LIMIT = 120;

    private static IbmWatsonxRerankServiceSettings createRandom() {
        return new IbmWatsonxRerankServiceSettings(
            createUri("https://" + randomAlphaOfLength(10) + ".example"),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            RateLimitSettingsTests.createRandom()
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new IbmWatsonxRerankServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(TEST_URI.toString(), TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, TEST_RATE_LIMIT)
        );

        assertThat(
            updatedServiceSettings,
            is(
                new IbmWatsonxRerankServiceSettings(
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
        var originalServiceSettings = new IbmWatsonxRerankServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = IbmWatsonxRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );
        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxRerankServiceSettings(
                    TEST_URI,
                    TEST_API_VERSION,
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = IbmWatsonxRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );
        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxRerankServiceSettings(
                    TEST_URI,
                    TEST_API_VERSION,
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoUrl_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> IbmWatsonxRerankServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_NoApiVersion_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> IbmWatsonxRerankServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), null, TEST_MODEL_ID, TEST_PROJECT_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", IbmWatsonxServiceFields.API_VERSION))
        );
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> IbmWatsonxRerankServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), TEST_API_VERSION, null, TEST_PROJECT_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.MODEL_ID))
        );
    }

    public void testFromMap_NoProjectId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> IbmWatsonxRerankServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), TEST_API_VERSION, TEST_MODEL_ID, null, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", IbmWatsonxServiceFields.PROJECT_ID))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new IbmWatsonxRerankServiceSettings(
            TEST_URI,
            TEST_API_VERSION,
            TEST_MODEL_ID,
            TEST_PROJECT_ID,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "url": "%s",
                "api_version": "%s",
                "model_id": "%s",
                "project_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_URI.toString(), TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<IbmWatsonxRerankServiceSettings> instanceReader() {
        return IbmWatsonxRerankServiceSettings::new;
    }

    @Override
    protected IbmWatsonxRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected IbmWatsonxRerankServiceSettings mutateInstance(IbmWatsonxRerankServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var apiVersion = instance.apiVersion();
        var modelId = instance.modelId();
        var projectId = instance.projectId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(4)) {
            case 0 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(12) + ".example"));
            case 1 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 2 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 3 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(8));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new IbmWatsonxRerankServiceSettings(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String url,
        @Nullable String apiVersion,
        @Nullable String modelId,
        @Nullable String projectId,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (apiVersion != null) {
            map.put(IbmWatsonxServiceFields.API_VERSION, apiVersion);
        }
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (projectId != null) {
            map.put(IbmWatsonxServiceFields.PROJECT_ID, projectId);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}

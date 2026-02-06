/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AzureOpenAiCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<AzureOpenAiCompletionServiceSettings> {
    private static final String TEST_RESOURCE_NAME = "test-resource-name";
    private static final String INITIAL_TEST_RESOURCE_NAME = "initial-resource-name";
    private static final String TEST_DEPLOYMENT_ID = "test-deployment-id";
    private static final String INITIAL_TEST_DEPLOYMENT_ID = "initial-deployment-id";
    private static final String TEST_API_VERSION = "test-api-version";
    private static final String INITIAL_TEST_API_VERSION = "initial-api-version";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static AzureOpenAiCompletionServiceSettings createRandom() {
        var resourceName = randomAlphaOfLength(8);
        var deploymentId = randomAlphaOfLength(8);
        var apiVersion = randomAlphaOfLength(8);

        return new AzureOpenAiCompletionServiceSettings(resourceName, deploymentId, apiVersion, RateLimitSettingsTests.createRandom());
    }

    private static Map<String, Object> createRequestSettingsMap() {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(AzureOpenAiServiceFields.RESOURCE_NAME, TEST_RESOURCE_NAME);
        settingsMap.put(AzureOpenAiServiceFields.DEPLOYMENT_ID, TEST_DEPLOYMENT_ID);
        settingsMap.put(AzureOpenAiServiceFields.API_VERSION, TEST_API_VERSION);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        return settingsMap;
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        var settingsMap = createRequestSettingsMap();
        var serviceSettings = new AzureOpenAiCompletionServiceSettings(
            INITIAL_TEST_RESOURCE_NAME,
            INITIAL_TEST_DEPLOYMENT_ID,
            INITIAL_TEST_API_VERSION,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.CHAT_COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiCompletionServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new AzureOpenAiCompletionServiceSettings(
            INITIAL_TEST_RESOURCE_NAME,
            INITIAL_TEST_DEPLOYMENT_ID,
            INITIAL_TEST_API_VERSION,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(new HashMap<>(), TaskType.CHAT_COMPLETION);

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiCompletionServiceSettings(
                    INITIAL_TEST_RESOURCE_NAME,
                    INITIAL_TEST_DEPLOYMENT_ID,
                    INITIAL_TEST_API_VERSION,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        testFromMap_CreatesSettingsCorrectly(ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        testFromMap_CreatesSettingsCorrectly(ConfigurationParseContext.PERSISTENT);
    }

    private static void testFromMap_CreatesSettingsCorrectly(ConfigurationParseContext configurationParseContext) {
        var serviceSettings = AzureOpenAiCompletionServiceSettings.fromMap(createRequestSettingsMap(), configurationParseContext);

        assertThat(
            serviceSettings,
            is(
                new AzureOpenAiCompletionServiceSettings(
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testToXContent_NullRateLimitSettings_WritesAllValuesWithDefaultRateLimit() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(TEST_RESOURCE_NAME, TEST_DEPLOYMENT_ID, TEST_API_VERSION, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"resource_name":"%s","deployment_id":"%s","api_version":"%s","rate_limit":{"requests_per_minute":120}}""",
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION
                )
            )
        );
    }

    public void testToXContent_WithRateLimitSettings_WritesAllValues() throws IOException {
        var entity = new AzureOpenAiCompletionServiceSettings(
            TEST_RESOURCE_NAME,
            TEST_DEPLOYMENT_ID,
            TEST_API_VERSION,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"resource_name":"%s","deployment_id":"%s","api_version":"%s","rate_limit":{"requests_per_minute":%d}}""",
                    TEST_RESOURCE_NAME,
                    TEST_DEPLOYMENT_ID,
                    TEST_API_VERSION,
                    TEST_RATE_LIMIT
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AzureOpenAiCompletionServiceSettings> instanceReader() {
        return AzureOpenAiCompletionServiceSettings::new;
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureOpenAiCompletionServiceSettings mutateInstance(AzureOpenAiCompletionServiceSettings instance) throws IOException {
        var resourceName = instance.resourceName();
        var deploymentId = instance.deploymentId();
        var apiVersion = instance.apiVersion();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(3)) {
            case 0 -> resourceName = randomValueOtherThan(resourceName, () -> randomAlphaOfLength(8));
            case 1 -> deploymentId = randomValueOtherThan(deploymentId, () -> randomAlphaOfLength(8));
            case 2 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 3 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new AzureOpenAiCompletionServiceSettings(resourceName, deploymentId, apiVersion, rateLimitSettings);
    }
}

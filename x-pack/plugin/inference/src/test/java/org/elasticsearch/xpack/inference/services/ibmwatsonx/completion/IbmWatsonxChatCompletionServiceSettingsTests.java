/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.AbstractIbmWatsonxServiceSettingsTests;
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

public class IbmWatsonxChatCompletionServiceSettingsTests extends AbstractIbmWatsonxServiceSettingsTests<
    IbmWatsonxChatCompletionServiceSettings> {

    @Override
    protected IbmWatsonxChatCompletionServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return IbmWatsonxChatCompletionServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String projectId,
        @Nullable String url,
        @Nullable String apiVersion,
        @Nullable Integer rateLimit
    ) {
        return buildServiceSettingsMap(url, apiVersion, modelId, projectId, rateLimit);
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings createServiceSettings(
        URI uri,
        String apiVersion,
        String modelId,
        String projectId,
        RateLimitSettings rateLimitSettings
    ) {
        return new IbmWatsonxChatCompletionServiceSettings(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = IbmWatsonxChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), TEST_API_VERSION, TEST_MODEL_ID, TEST_PROJECT_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );
        assertThat(
            serviceSettings,
            is(
                new IbmWatsonxChatCompletionServiceSettings(
                    TEST_URI,
                    TEST_API_VERSION,
                    TEST_MODEL_ID,
                    TEST_PROJECT_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new IbmWatsonxChatCompletionServiceSettings(
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
    protected Writeable.Reader<IbmWatsonxChatCompletionServiceSettings> instanceReader() {
        return IbmWatsonxChatCompletionServiceSettings::new;
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings mutateInstance(IbmWatsonxChatCompletionServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var apiVersion = instance.apiVersion();
        var modelId = instance.modelId();
        var projectId = instance.projectId();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(4)) {
            case 0 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(10) + ".example"));
            case 1 -> apiVersion = randomValueOtherThan(apiVersion, () -> randomAlphaOfLength(8));
            case 2 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 3 -> projectId = randomValueOtherThan(projectId, () -> randomAlphaOfLength(8));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new IbmWatsonxChatCompletionServiceSettings(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings mutateInstanceForVersion(
        IbmWatsonxChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    @Override
    protected IbmWatsonxChatCompletionServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return IbmWatsonxChatCompletionServiceSettings.createParser(true).apply(parser, ConfigurationParseContext.PERSISTENT).build();
    }

    private static IbmWatsonxChatCompletionServiceSettings createRandom() {
        return new IbmWatsonxChatCompletionServiceSettings(
            createUri("https://" + randomAlphaOfLength(10) + ".example"),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            RateLimitSettingsTests.createRandom()
        );
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

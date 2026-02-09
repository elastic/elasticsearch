/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GroqChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<GroqChatCompletionServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final String TEST_ORGANIZATION_ID = "test-organization-id";
    private static final String INITIAL_TEST_ORGANIZATION_ID = "initial-test-organization-id";
    private static final URI TEST_URI = ServiceUtils.createUri("https://test-uri.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://initial-test-uri.com");
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                OpenAiServiceFields.ORGANIZATION,
                TEST_ORGANIZATION_ID,
                ServiceFields.URL,
                TEST_URI.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = createInitialGroqChatCompletionServiceSettings().updateServiceSettings(settingsMap, TaskType.COMPLETION);

        MatcherAssert.assertThat(
            serviceSettings,
            is(new GroqChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, TEST_ORGANIZATION_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialGroqChatCompletionServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        MatcherAssert.assertThat(serviceSettings, is(createInitialGroqChatCompletionServiceSettings()));
    }

    private static GroqChatCompletionServiceSettings createInitialGroqChatCompletionServiceSettings() {
        return new GroqChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_ORGANIZATION_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
    }

    public void testFromMapRequiresModelId() {
        var map = new HashMap<String, Object>();
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> GroqChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );
        assertThat(e.validationErrors().isEmpty(), is(false));
    }

    public void testFromMapParsesFields() {
        var modelId = randomAlphaOfLength(8);
        var organization = randomBoolean() ? randomAlphaOfLength(6) : null;
        var url = randomBoolean() ? "https://api.groq.com/openai/v1/chat/completions" : null;

        Map<String, Object> map = new HashMap<>();
        map.put(ServiceFields.MODEL_ID, modelId);
        if (organization != null) {
            map.put(OpenAiServiceFields.ORGANIZATION, organization);
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        var settings = GroqChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);
        assertThat(settings.modelId(), equalTo(modelId));
        assertThat(settings.organizationId(), equalTo(organization));
        if (url == null) {
            assertNull(settings.uri());
        } else {
            assertEquals(url, settings.uri().toString());
        }
    }

    @Override
    protected Writeable.Reader<GroqChatCompletionServiceSettings> instanceReader() {
        return GroqChatCompletionServiceSettings::new;
    }

    @Override
    protected GroqChatCompletionServiceSettings createTestInstance() {
        String modelId = randomAlphaOfLength(8);
        URI uri = randomBoolean() ? URI.create("https://api.groq.com/" + randomAlphaOfLength(5)) : null;
        String organizationId = randomBoolean() ? randomAlphaOfLength(6) : null;
        RateLimitSettings rateLimitSettings = RateLimitSettingsTests.createRandom();
        return new GroqChatCompletionServiceSettings(modelId, uri, organizationId, rateLimitSettings);
    }

    @Override
    protected GroqChatCompletionServiceSettings mutateInstance(GroqChatCompletionServiceSettings instance) {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        String organizationId = instance.organizationId();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 3)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> URI.create("https://api.groq.com/" + randomAlphaOfLength(6)));
            case 2 -> organizationId = randomValueOtherThan(organizationId, () -> randomAlphaOfLength(5));
            case 3 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Unsupported branch");
        }

        return new GroqChatCompletionServiceSettings(modelId, uri, organizationId, rateLimitSettings);
    }
}

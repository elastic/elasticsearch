/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GroqChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<GroqChatCompletionServiceSettings> {

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

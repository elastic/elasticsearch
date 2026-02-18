/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FireworksAiChatCompletionServiceSettingsTests extends AbstractWireSerializingTestCase<
    FireworksAiChatCompletionServiceSettings> {

    public void testFromMapRequiresModelId() {
        var map = new HashMap<String, Object>();
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> FireworksAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );
        assertThat(e.validationErrors().isEmpty(), is(false));
    }

    public void testFromMapParsesFields() {
        var modelId = randomAlphaOfLength(8);
        var url = randomBoolean() ? "https://api.fireworks.ai/inference/v1/chat/completions" : null;

        Map<String, Object> map = new HashMap<>();
        map.put(ServiceFields.MODEL_ID, modelId);
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        var settings = FireworksAiChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);
        assertThat(settings.modelId(), equalTo(modelId));
        if (url == null) {
            assertNull(settings.uri());
        } else {
            assertEquals(url, settings.uri().toString());
        }
    }

    @Override
    protected Writeable.Reader<FireworksAiChatCompletionServiceSettings> instanceReader() {
        return FireworksAiChatCompletionServiceSettings::new;
    }

    @Override
    protected FireworksAiChatCompletionServiceSettings createTestInstance() {
        String modelId = randomAlphaOfLength(8);
        URI uri = randomBoolean() ? URI.create("https://api.fireworks.ai/" + randomAlphaOfLength(5)) : null;
        RateLimitSettings rateLimitSettings = RateLimitSettingsTests.createRandom();
        return new FireworksAiChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected FireworksAiChatCompletionServiceSettings mutateInstance(FireworksAiChatCompletionServiceSettings instance) {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> URI.create("https://api.fireworks.ai/" + randomAlphaOfLength(6)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Unsupported branch");
        }

        return new FireworksAiChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GroqChatCompletionServiceSettingsTests extends ESTestCase {

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

    public void testSerializationRoundTrip() throws IOException {
        var rateLimit = new RateLimitSettings(randomIntBetween(1, 1000));
        var settings = new GroqChatCompletionServiceSettings("model", (java.net.URI) null, null, rateLimit);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            settings.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            var restored = new GroqChatCompletionServiceSettings(in);
            assertThat(restored, equalTo(settings));
        }
    }
}



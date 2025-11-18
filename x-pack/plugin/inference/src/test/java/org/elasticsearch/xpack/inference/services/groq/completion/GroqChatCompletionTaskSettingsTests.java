/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class GroqChatCompletionTaskSettingsTests extends ESTestCase {

    public void testFromMapParsesValues() {
        var user = randomAlphaOfLength(8);
        Map<String, Object> map = new HashMap<>();
        map.put(OpenAiServiceFields.USER, user);
        map.put(OpenAiServiceFields.HEADERS, Map.of("X-Test", "value"));

        var settings = new GroqChatCompletionTaskSettings(map);
        assertThat(settings.user(), equalTo(user));
        assertThat(settings.headers(), equalTo(Map.of("X-Test", "value")));
    }

    public void testSerializationRoundTrip() throws IOException {
        var headers = randomBoolean() ? Map.of("X-Test", "value") : null;
        var user = randomBoolean() ? randomAlphaOfLength(8) : null;
        var settings = new GroqChatCompletionTaskSettings(user, headers);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            settings.writeTo(out);
            var restored = new GroqChatCompletionTaskSettings(out.bytes().streamInput());
            assertThat(restored.user(), equalTo(user));
            assertThat(restored.headers(), equalTo(headers));
        }
    }

    public void testUpdatedTaskSettingsOverridesValues() {
        var original = new GroqChatCompletionTaskSettings("user-1", Map.of("X-Test", "value"));
        var updated = original.updatedTaskSettings(Map.of(OpenAiServiceFields.USER, "user-2"));
        assertThat(updated.user(), equalTo("user-2"));
        assertThat(updated.headers(), equalTo(Map.of("X-Test", "value")));
    }
}



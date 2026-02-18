/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FireworksAiChatCompletionTaskSettingsTests extends AbstractWireSerializingTestCase<FireworksAiChatCompletionTaskSettings> {

    public void testFromMapParsesValues() {
        var user = randomAlphaOfLength(8);
        Map<String, Object> map = new HashMap<>();
        map.put(OpenAiServiceFields.USER, user);
        map.put(OpenAiServiceFields.HEADERS, Map.of("X-Test", "value"));

        var settings = new FireworksAiChatCompletionTaskSettings(map);
        assertThat(settings.user(), equalTo(user));
        assertThat(settings.headers(), equalTo(Map.of("X-Test", "value")));
    }

    public void testUpdatedTaskSettingsOverridesValues() {
        var original = new FireworksAiChatCompletionTaskSettings("user-1", Map.of("X-Test", "value"));
        var updated = original.updatedTaskSettings(Map.of(OpenAiServiceFields.USER, "user-2"));
        assertThat(updated.user(), equalTo("user-2"));
        assertThat(updated.headers(), equalTo(Map.of("X-Test", "value")));
    }

    @Override
    protected Writeable.Reader<FireworksAiChatCompletionTaskSettings> instanceReader() {
        return FireworksAiChatCompletionTaskSettings::new;
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings createTestInstance() {
        var user = randomBoolean() ? randomAlphaOfLength(8) : null;
        Map<String, String> headers = randomBoolean() ? Map.of("X-" + randomAlphaOfLength(4), randomAlphaOfLength(6)) : null;
        return new FireworksAiChatCompletionTaskSettings(user, headers);
    }

    @Override
    protected FireworksAiChatCompletionTaskSettings mutateInstance(FireworksAiChatCompletionTaskSettings instance) {
        var user = instance.user();
        Map<String, String> headers = instance.headers();

        switch (between(0, 1)) {
            case 0 -> user = randomValueOtherThan(user, () -> randomAlphaOfLength(8));
            case 1 -> headers = randomValueOtherThan(
                headers,
                () -> randomBoolean() ? Map.of("X-" + randomAlphaOfLength(4), randomAlphaOfLength(5)) : null
            );
            default -> throw new AssertionError("unexpected branch");
        }

        return new FireworksAiChatCompletionTaskSettings(user, headers);
    }
}

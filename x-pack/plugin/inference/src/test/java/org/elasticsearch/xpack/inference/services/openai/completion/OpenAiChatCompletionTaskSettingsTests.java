/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.TransportVersions.INFERENCE_API_OPENAI_HEADERS;
import static org.hamcrest.Matchers.is;

public class OpenAiChatCompletionTaskSettingsTests extends AbstractBWCWireSerializationTestCase<OpenAiChatCompletionTaskSettings> {

    public static OpenAiChatCompletionTaskSettings createRandomWithUser() {
        return new OpenAiChatCompletionTaskSettings(
            randomBoolean() ? null : randomAlphaOfLength(15),
            randomBoolean() ? null : Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15))
        );
    }

    public void testIsEmpty() {
        var randomSettings = new OpenAiChatCompletionTaskSettings(
            randomBoolean() ? null : "username",
            randomBoolean() ? null : Map.of("key", "value")
        );
        var stringRep = Strings.toString(randomSettings);

        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandomWithUser();
        var newSettings = createRandomWithUser();

        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user() != null) {
            newSettingsMap.put(OpenAiServiceFields.USER, newSettings.user());
        }

        if (newSettings.headers() != null && newSettings.headers().isEmpty() == false) {
            newSettingsMap.put(OpenAiServiceFields.HEADERS, newSettings.headers());
        }

        OpenAiChatCompletionTaskSettings updatedSettings = (OpenAiChatCompletionTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );

        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }

        if (newSettings.headers() == null) {
            assertEquals(initialSettings.headers(), updatedSettings.headers());
        } else {
            assertEquals(newSettings.headers(), updatedSettings.headers());
        }
    }

    public void testFromMap_WithUser() {
        assertEquals(
            new OpenAiChatCompletionTaskSettings("user", null),
            OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")))
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "")))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertNull(taskSettings.user());
    }

    public void testOf_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        var overriddenTaskSettings = OpenAiChatCompletionTaskSettings.of(
            taskSettings,
            OpenAiChatCompletionRequestTaskSettings.EMPTY_SETTINGS
        );
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOf_UsesOverriddenSettings() {
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")));

        var requestTaskSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user2")));

        var overriddenTaskSettings = OpenAiChatCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new OpenAiChatCompletionTaskSettings("user2", null)));
    }

    public void testOf_UsesOverriddenSettings_ForHeaders() {
        var user = "user";
        var taskSettings = OpenAiChatCompletionTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, user)));

        var headers = Map.of("key", "value");
        var requestTaskSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiServiceFields.HEADERS, headers))
        );

        var overriddenTaskSettings = OpenAiChatCompletionTaskSettings.of(taskSettings, requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new OpenAiChatCompletionTaskSettings(user, headers)));
    }

    @Override
    protected Writeable.Reader<OpenAiChatCompletionTaskSettings> instanceReader() {
        return OpenAiChatCompletionTaskSettings::new;
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstance(OpenAiChatCompletionTaskSettings instance) throws IOException {
        var setNull = randomBoolean();
        var fieldToMutate = randomIntBetween(0, 1);
        return switch (fieldToMutate) {
            case 0 -> new OpenAiChatCompletionTaskSettings(
                instance.user() == null ? randomAlphaOfLength(15) : (setNull ? null : instance.user() + "modified"),
                instance.headers()
            );
            case 1 -> {
                if (instance.headers() == null) {
                    yield new OpenAiChatCompletionTaskSettings(instance.user(), Map.of(randomAlphaOfLength(15), randomAlphaOfLength(15)));
                } else if (setNull) {
                    yield new OpenAiChatCompletionTaskSettings(instance.user(), null);
                } else {
                    var instanceHeaders = new HashMap<>(instance.headers() == null ? Map.of() : instance.headers());
                    instanceHeaders.put(randomAlphaOfLength(15), randomAlphaOfLength(15));
                    yield new OpenAiChatCompletionTaskSettings(instance.user(), instanceHeaders);
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + fieldToMutate);
        };
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstanceForVersion(
        OpenAiChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        if (version.onOrAfter(INFERENCE_API_OPENAI_HEADERS)) {
            return instance;
        }

        return new OpenAiChatCompletionTaskSettings(instance.user(), null);
    }
}

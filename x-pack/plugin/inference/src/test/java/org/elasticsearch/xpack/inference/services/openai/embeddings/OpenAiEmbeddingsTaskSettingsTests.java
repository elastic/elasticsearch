/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<OpenAiEmbeddingsTaskSettings> {

    public static OpenAiEmbeddingsTaskSettings createRandomWithUser() {
        return new OpenAiEmbeddingsTaskSettings(randomAlphaOfLength(15));
    }

    /**
     * The created settings can have the user set to null.
     */
    public static OpenAiEmbeddingsTaskSettings createRandom() {
        var user = randomBoolean() ? randomAlphaOfLength(15) : null;
        return new OpenAiEmbeddingsTaskSettings(user);
    }

    public void testIsEmpty() {
        var randomSettings = new OpenAiChatCompletionTaskSettings(randomBoolean() ? null : "username");
        var stringRep = Strings.toString(randomSettings);
        assertEquals(stringRep, randomSettings.isEmpty(), stringRep.equals("{}"));
    }

    public void testUpdatedTaskSettings() {
        var initialSettings = createRandom();
        var newSettings = createRandom();
        Map<String, Object> newSettingsMap = new HashMap<>();
        if (newSettings.user() != null) {
            newSettingsMap.put(OpenAiServiceFields.USER, newSettings.user());
        }
        OpenAiEmbeddingsTaskSettings updatedSettings = (OpenAiEmbeddingsTaskSettings) initialSettings.updatedTaskSettings(
            Collections.unmodifiableMap(newSettingsMap)
        );
        if (newSettings.user() == null) {
            assertEquals(initialSettings.user(), updatedSettings.user());
        } else {
            assertEquals(newSettings.user(), updatedSettings.user());
        }
    }

    public void testFromMap_WithUser() {
        assertEquals(
            new OpenAiEmbeddingsTaskSettings("user"),
            OpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")), ConfigurationParseContext.REQUEST)
        );
    }

    public void testFromMap_UserIsEmptyString() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(OpenAiServiceFields.USER, "")),
                ConfigurationParseContext.REQUEST
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(Strings.format("Validation Failed: 1: [task_settings] Invalid value empty string. [user] must be a non-empty string;"))
        );
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of()), ConfigurationParseContext.PERSISTENT);
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")),
            ConfigurationParseContext.PERSISTENT
        );

        var overriddenTaskSettings = OpenAiEmbeddingsTaskSettings.of(taskSettings, OpenAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiServiceFields.USER, "user")),
            ConfigurationParseContext.PERSISTENT
        );

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user2")));

        var overriddenTaskSettings = OpenAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new OpenAiEmbeddingsTaskSettings("user2")));
    }

    @Override
    protected Writeable.Reader<OpenAiEmbeddingsTaskSettings> instanceReader() {
        return OpenAiEmbeddingsTaskSettings::new;
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings createTestInstance() {
        return createRandomWithUser();
    }

    @Override
    protected OpenAiEmbeddingsTaskSettings mutateInstance(OpenAiEmbeddingsTaskSettings instance) throws IOException {
        return randomValueOtherThan(instance, OpenAiEmbeddingsTaskSettingsTests::createRandomWithUser);
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable String user) {
        var map = new HashMap<String, Object>();

        if (user != null) {
            map.put(OpenAiServiceFields.USER, user);
        }

        return map;
    }
}

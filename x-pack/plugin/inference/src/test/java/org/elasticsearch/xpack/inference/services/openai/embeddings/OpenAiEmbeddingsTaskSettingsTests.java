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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class OpenAiEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<OpenAiEmbeddingsTaskSettings> {

    public static OpenAiEmbeddingsTaskSettings createRandomWithUser() {
        return new OpenAiEmbeddingsTaskSettings(randomAlphaOfLength(15), randomAlphaOfLength(15));
    }

    /**
     * The created settings can have the user set to null.
     */
    public static OpenAiEmbeddingsTaskSettings createRandom() {
        var user = randomBoolean() ? randomAlphaOfLength(15) : null;
        return new OpenAiEmbeddingsTaskSettings(randomAlphaOfLength(15), user);
    }

    public void testFromMap_MissingModel_ThrowException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> OpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.USER, "user")))
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] does not contain the required setting [%s];",
                    OpenAiEmbeddingsTaskSettings.MODEL
                )
            )
        );
    }

    public void testFromMap_CreatesWithModelAndUser() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model", OpenAiEmbeddingsTaskSettings.USER, "user"))
        );

        assertThat(taskSettings.model(), is("model"));
        assertThat(taskSettings.user(), is("user"));
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model")));

        assertThat(taskSettings.model(), is("model"));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model", OpenAiEmbeddingsTaskSettings.USER, "user"))
        );

        var overriddenTaskSettings = taskSettings.overrideWith(OpenAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS);
        assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model", OpenAiEmbeddingsTaskSettings.USER, "user"))
        );

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model2", OpenAiEmbeddingsTaskSettings.USER, "user2"))
        );

        var overriddenTaskSettings = taskSettings.overrideWith(requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new OpenAiEmbeddingsTaskSettings("model2", "user2")));
    }

    public void testOverrideWith_UsesOnlyNonNullModelSetting() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model", OpenAiEmbeddingsTaskSettings.USER, "user"))
        );

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model2"))
        );

        var overriddenTaskSettings = taskSettings.overrideWith(requestTaskSettings);
        assertThat(overriddenTaskSettings, is(new OpenAiEmbeddingsTaskSettings("model2", "user")));
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
        return createRandomWithUser();
    }

    public static Map<String, Object> getTaskSettingsMap(String model, @Nullable String user) {
        var map = new HashMap<String, Object>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, model));

        if (user != null) {
            map.put(OpenAiEmbeddingsTaskSettings.USER, user);
        }

        return map;
    }
}

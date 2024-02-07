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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

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
            () -> OpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.USER, "user")), true)
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Validation Failed: 1: [task_settings] does not contain the required setting [%s];",
                    OpenAiEmbeddingsTaskSettings.MODEL_ID
                )
            )
        );
    }

    public void testFromMap_CreatesWithModelAndUser() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model", OpenAiEmbeddingsTaskSettings.USER, "user")),
            false
        );

        MatcherAssert.assertThat(taskSettings.modelId(), is("model"));
        MatcherAssert.assertThat(taskSettings.user(), is("user"));
    }

    public void testFromMap_CreatesWithModelId() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL_ID, "model", OpenAiEmbeddingsTaskSettings.USER, "user")),
            false
        );

        MatcherAssert.assertThat(taskSettings.modelId(), is("model"));
        MatcherAssert.assertThat(taskSettings.user(), is("user"));
    }

    public void testFromMap_PrefersModelId_OverModel() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(
                Map.of(
                    OpenAiEmbeddingsTaskSettings.MODEL_ID,
                    "model",
                    OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD,
                    "old_model",
                    OpenAiEmbeddingsTaskSettings.USER,
                    "user"
                )
            ),
            false
        );

        MatcherAssert.assertThat(taskSettings.modelId(), is("model"));
        MatcherAssert.assertThat(taskSettings.user(), is("user"));
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model")),
            false
        );

        MatcherAssert.assertThat(taskSettings.modelId(), is("model"));
        assertNull(taskSettings.user());
    }

    public void testOverrideWith_KeepsOriginalValuesWithOverridesAreNull() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model", OpenAiEmbeddingsTaskSettings.USER, "user")),
            false
        );

        var overriddenTaskSettings = OpenAiEmbeddingsTaskSettings.of(taskSettings, OpenAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model", OpenAiEmbeddingsTaskSettings.USER, "user")),
            false
        );

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model2", OpenAiEmbeddingsTaskSettings.USER, "user2"))
        );

        var overriddenTaskSettings = OpenAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new OpenAiEmbeddingsTaskSettings("model2", "user2")));
    }

    public void testOverrideWith_UsesOverriddenSettings_UsesModel2_FromModelIdField() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model", OpenAiEmbeddingsTaskSettings.USER, "user")),
            false
        );

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(
                Map.of(
                    OpenAiEmbeddingsTaskSettings.MODEL_ID,
                    "model2",
                    OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD,
                    "model3",
                    OpenAiEmbeddingsTaskSettings.USER,
                    "user2"
                )
            )
        );

        var overriddenTaskSettings = OpenAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new OpenAiEmbeddingsTaskSettings("model2", "user2")));
    }

    public void testOverrideWith_UsesOnlyNonNullModelSetting() {
        var taskSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model", OpenAiEmbeddingsTaskSettings.USER, "user")),
            false
        );

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, "model2"))
        );

        var overriddenTaskSettings = OpenAiEmbeddingsTaskSettings.of(taskSettings, requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new OpenAiEmbeddingsTaskSettings("model2", "user")));
    }

    public void testXContent_WritesModelId() throws IOException {
        var entity = new OpenAiEmbeddingsTaskSettings("modelId", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"model_id":"modelId"}"""));
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
        var map = new HashMap<String, Object>(Map.of(OpenAiEmbeddingsTaskSettings.OLD_MODEL_ID_FIELD, model));

        if (user != null) {
            map.put(OpenAiEmbeddingsTaskSettings.USER, user);
        }

        return map;
    }
}

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
        var serviceSettings = OpenAiEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model", OpenAiEmbeddingsTaskSettings.USER, "user"))
        );

        assertThat(serviceSettings.model(), is("model"));
        assertThat(serviceSettings.user(), is("user"));
    }

    public void testFromMap_MissingUser_DoesNotThrowException() {
        var serviceSettings = OpenAiEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(OpenAiEmbeddingsTaskSettings.MODEL, "model")));

        assertThat(serviceSettings.model(), is("model"));
        assertNull(serviceSettings.user());
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
}

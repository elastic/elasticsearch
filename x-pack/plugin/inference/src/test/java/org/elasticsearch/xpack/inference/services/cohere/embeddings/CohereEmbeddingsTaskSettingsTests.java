/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<CohereEmbeddingsTaskSettings> {

    public static CohereEmbeddingsTaskSettings createRandom() {
        var inputType = randomBoolean() ? randomFrom(InputType.values()) : null;
        var truncation = randomBoolean() ? randomFrom(CohereTruncation.values()) : null;

        return new CohereEmbeddingsTaskSettings(inputType, truncation);
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            CohereEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new CohereEmbeddingsTaskSettings(null, null))
        );
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        MatcherAssert.assertThat(
            CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        CohereEmbeddingsTaskSettings.INPUT_TYPE,
                        InputType.INGEST.toString(),
                        CohereServiceFields.TRUNCATE,
                        CohereTruncation.END.toString()
                    )
                )
            ),
            is(new CohereEmbeddingsTaskSettings(InputType.INGEST, CohereTruncation.END))
        );
    }

    public void testFromMap_ReturnsFailure_WhenInputTypeIsInvalid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of(CohereEmbeddingsTaskSettings.INPUT_TYPE, "abc")))
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [task_settings] Invalid value [abc] received. [input_type] must be one of [ingest, search];")
        );
    }

    public void testOverrideWith_KeepsOriginalValuesWhenOverridesAreNull() {
        var taskSettings = CohereEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(CohereServiceSettings.MODEL, "model", CohereServiceFields.TRUNCATE, CohereTruncation.END.toString()))
        );

        var overriddenTaskSettings = taskSettings.overrideWith(CohereEmbeddingsTaskSettings.EMPTY_SETTINGS);
        MatcherAssert.assertThat(overriddenTaskSettings, is(taskSettings));
    }

    public void testOverrideWith_UsesOverriddenSettings() {
        var taskSettings = CohereEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(CohereServiceFields.TRUNCATE, CohereTruncation.END.toString()))
        );

        var requestTaskSettings = CohereEmbeddingsTaskSettings.fromMap(
            new HashMap<>(Map.of(CohereServiceFields.TRUNCATE, CohereTruncation.START.toString()))
        );

        var overriddenTaskSettings = taskSettings.overrideWith(requestTaskSettings);
        MatcherAssert.assertThat(overriddenTaskSettings, is(new CohereEmbeddingsTaskSettings(null, CohereTruncation.START)));
    }

    @Override
    protected Writeable.Reader<CohereEmbeddingsTaskSettings> instanceReader() {
        return CohereEmbeddingsTaskSettings::new;
    }

    @Override
    protected CohereEmbeddingsTaskSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected CohereEmbeddingsTaskSettings mutateInstance(CohereEmbeddingsTaskSettings instance) throws IOException {
        return null;
    }

    public static Map<String, Object> getTaskSettingsMapEmpty() {
        return new HashMap<>();
    }

    public static Map<String, Object> getTaskSettingsMap(@Nullable InputType inputType, @Nullable CohereTruncation truncation) {
        var map = new HashMap<String, Object>();

        if (inputType != null) {
            map.put(CohereEmbeddingsTaskSettings.INPUT_TYPE, inputType.toString());
        }

        if (truncation != null) {
            map.put(CohereServiceFields.TRUNCATE, truncation.toString());
        }

        return map;
    }
}

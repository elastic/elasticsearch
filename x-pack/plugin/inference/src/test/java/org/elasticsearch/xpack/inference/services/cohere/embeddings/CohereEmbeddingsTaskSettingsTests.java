/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsTaskSettingsTests extends AbstractWireSerializingTestCase<CohereEmbeddingsTaskSettings> {

    public static CohereEmbeddingsTaskSettings createRandom() {
        var model = randomBoolean() ? randomAlphaOfLength(15) : null;
        var inputType = randomBoolean() ? randomFrom(InputType.values()) : null;
        var embeddingTypes = randomBoolean() ? List.of(randomAlphaOfLength(6)) : null;
        var truncation = randomBoolean() ? randomFrom(CohereTruncation.values()) : null;

        return new CohereEmbeddingsTaskSettings(model, inputType, embeddingTypes, truncation);
    }

    public void testFromMap_CreatesEmptySettings_WhenAllFieldsAreNull() {
        MatcherAssert.assertThat(
            CohereEmbeddingsTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(new CohereEmbeddingsTaskSettings(null, null, null, null))
        );
    }

    public void testFromMap_CreatesSettings_WhenAllFieldsOfSettingsArePresent() {
        MatcherAssert.assertThat(
            CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        CohereServiceFields.MODEL,
                        "abc",
                        CohereEmbeddingsTaskSettings.INPUT_TYPE,
                        InputType.INGEST.toString().toLowerCase(Locale.ROOT),
                        CohereEmbeddingsTaskSettings.EMBEDDING_TYPES,
                        List.of("abc", "123"),
                        CohereServiceFields.TRUNCATE,
                        CohereTruncation.END.toString().toLowerCase(Locale.ROOT)
                    )
                )
            ),
            is(new CohereEmbeddingsTaskSettings("abc", InputType.INGEST, List.of("abc", "123"), CohereTruncation.END))
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

    public void testFromMap_ReturnsFailure_WhenEmbeddingTypesAreNotValid() {
        var exception = expectThrows(
            ValidationException.class,
            () -> CohereEmbeddingsTaskSettings.fromMap(
                new HashMap<>(Map.of(CohereEmbeddingsTaskSettings.EMBEDDING_TYPES, List.of("abc", 123)))
            )
        );

        MatcherAssert.assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: [task_settings] Invalid type [Integer]"
                    + " received for value [123]. [embedding_types] must be type [String];"
            )
        );
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

    public static Map<String, Object> getTaskSettingsMap() {
        return new HashMap<>(Collections.emptyMap());
    }
}

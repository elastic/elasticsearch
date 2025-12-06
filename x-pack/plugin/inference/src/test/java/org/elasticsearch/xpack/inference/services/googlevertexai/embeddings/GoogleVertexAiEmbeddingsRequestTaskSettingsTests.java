/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsRequestTaskSettingsTests extends ESTestCase {

    public void testFromMap_ReturnsEmptySettings_IfMapEmpty() {
        var requestTaskSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>());
        assertThat(requestTaskSettings, is(GoogleVertexAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_IfMapNull() {
        var requestTaskSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(null);
        assertThat(requestTaskSettings, is(GoogleVertexAiEmbeddingsRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_DoesNotThrowValidationException_IfAutoTruncateIsMissing() {
        var requestTaskSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(new HashMap<>(Map.of("unrelated", true)));
        assertThat(requestTaskSettings, is(new GoogleVertexAiEmbeddingsRequestTaskSettings(null, null)));
    }

    public void testFromMap_ExtractsAutoTruncate() {
        var autoTruncate = true;
        var requestTaskSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, autoTruncate))
        );
        assertThat(requestTaskSettings, is(new GoogleVertexAiEmbeddingsRequestTaskSettings(autoTruncate, null)));
    }

    public void testFromMap_ThrowsValidationException_IfAutoTruncateIsInvalidValue() {
        expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
                new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.AUTO_TRUNCATE, "invalid"))
            )
        );
    }

    public void testFromMap_ExtractsInputType() {
        var requestTaskSettings = GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, InputType.INGEST.toString()))
        );
        assertThat(requestTaskSettings, is(new GoogleVertexAiEmbeddingsRequestTaskSettings(null, InputType.INGEST)));
    }

    public void testFromMap_ThrowsValidationException_IfInputTypeIsInvalidValue() {
        expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
                new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, "abc"))
            )
        );
    }

    public void testFromMap_ThrowsValidationException_IfInputTypeIsUnspecified() {
        expectThrows(
            ValidationException.class,
            () -> GoogleVertexAiEmbeddingsRequestTaskSettings.fromMap(
                new HashMap<>(Map.of(GoogleVertexAiEmbeddingsTaskSettings.INPUT_TYPE, InputType.UNSPECIFIED.toString()))
            )
        );
    }
}

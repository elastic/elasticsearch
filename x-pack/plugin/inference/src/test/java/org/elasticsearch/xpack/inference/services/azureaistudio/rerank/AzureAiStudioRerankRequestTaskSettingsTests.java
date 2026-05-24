/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.RETURN_DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_N_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioRerankRequestTaskSettingsTests extends ESTestCase {
    private static final String INVALID_FIELD_TYPE_STRING = "invalid";
    private static final boolean RETURN_DOCUMENTS = true;
    private static final int TOP_N = 2;

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        assertThat(
            AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of())),
            is(AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        assertThat(
            AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model"))),
            is(AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS)
        );
    }

    public void testFromMap_ReturnsReturnDocuments() {
        assertThat(
            AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of(RETURN_DOCUMENTS_FIELD, RETURN_DOCUMENTS))),
            is(new AzureAiStudioRerankRequestTaskSettings(RETURN_DOCUMENTS, null))
        );
    }

    public void testFromMap_ReturnsTopN() {
        assertThat(
            AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of(TOP_N_FIELD, TOP_N))),
            is(new AzureAiStudioRerankRequestTaskSettings(null, TOP_N))
        );
    }

    public void testFromMap_ReturnDocumentsIsInvalidValue_ThrowsValidationException() {
        assertThrowsValidationExceptionIfStringValueProvidedFor(RETURN_DOCUMENTS_FIELD);
    }

    public void testFromMap_TopNIsInvalidValue_ThrowsValidationException() {
        assertThrowsValidationExceptionIfStringValueProvidedFor(TOP_N_FIELD);
    }

    private void assertThrowsValidationExceptionIfStringValueProvidedFor(String field) {
        final var thrownException = expectThrows(
            ValidationException.class,
            () -> AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of(field, INVALID_FIELD_TYPE_STRING)))
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "field ["
                        + field
                        + "] is not of the expected type. The value ["
                        + INVALID_FIELD_TYPE_STRING
                        + "] cannot be converted to a "
                )
            )
        );
    }
}

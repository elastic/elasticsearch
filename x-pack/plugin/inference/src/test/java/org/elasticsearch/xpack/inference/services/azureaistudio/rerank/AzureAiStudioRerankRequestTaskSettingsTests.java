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

    public void testFromMap_ReturnsEmptySettings_WhenTheMapIsEmpty() {
        final var settings = AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of()));
        assertThat(settings, is(AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsEmptySettings_WhenTheMapDoesNotContainTheFields() {
        final var settings = AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of("key", "model")));
        assertThat(settings, is(AzureAiStudioRerankRequestTaskSettings.EMPTY_SETTINGS));
    }

    public void testFromMap_ReturnsDoSample() {
        final var settings = AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of(RETURN_DOCUMENTS_FIELD, true)));
        assertThat(settings.returnDocuments(), is(true));
    }

    public void testFromMap_ReturnsTopN() {
        final var settings = AzureAiStudioRerankRequestTaskSettings.fromMap(new HashMap<>(Map.of(TOP_N_FIELD, 2)));
        assertThat(settings.topN(), is(2));
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

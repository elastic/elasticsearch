/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.hamcrest.Matchers;

import java.util.List;

public class InferenceInputsTests extends ESTestCase {
    public void testCastToSucceeds() {
        InferenceInputs inputs = new EmbeddingsInput(List.of(), InputTypeTests.randomWithNull(), false);
        assertThat(inputs.castTo(EmbeddingsInput.class), Matchers.instanceOf(EmbeddingsInput.class));

        var emptyRequest = new UnifiedCompletionRequest(List.of(), null, null, null, null, null, null, null);
        assertThat(new UnifiedChatInput(emptyRequest, false).castTo(UnifiedChatInput.class), Matchers.instanceOf(UnifiedChatInput.class));
        assertThat(
            new QueryAndDocsInputs("hello", List.of(), Boolean.TRUE, 33, false).castTo(QueryAndDocsInputs.class),
            Matchers.instanceOf(QueryAndDocsInputs.class)
        );
    }

    public void testCastToFails() {
        InferenceInputs inputs = new EmbeddingsInput(List.of(), null, false);
        var exception = expectThrows(IllegalArgumentException.class, () -> inputs.castTo(QueryAndDocsInputs.class));
        assertThat(
            exception.getMessage(),
            Matchers.containsString(
                Strings.format("Unable to convert inference inputs type: [%s] to [%s]", EmbeddingsInput.class, QueryAndDocsInputs.class)
            )
        );
    }
}

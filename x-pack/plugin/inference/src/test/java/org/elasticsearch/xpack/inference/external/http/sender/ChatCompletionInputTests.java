/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;

import java.util.List;

public class ChatCompletionInputTests extends InferenceObjectRamBytesUsedTest<ChatCompletionInput> {

    @Override
    public ChatCompletionInput objectToEstimate() {
        return new ChatCompletionInput(List.of("A string"));
    }

    @Override
    public List<ChatCompletionInput> objectsToEstimateWithLargerInput() {
        return List.of(
            // More elements each having the same size
            new ChatCompletionInput(List.of("A string", "A string")),
            // One larger string
            new ChatCompletionInput(List.of("A string".repeat(10)))
        );
    }
}

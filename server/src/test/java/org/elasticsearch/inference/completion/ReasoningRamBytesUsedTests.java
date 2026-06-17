/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;

import java.util.List;

/**
 * Separate to {@link ReasoningTests} as {@link ReasoningTests} already extends another class.
 */
public class ReasoningRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<Reasoning> {
    @Override
    public Reasoning objectToEstimate() {
        return new Reasoning(Reasoning.ReasoningEffort.HIGH, Reasoning.ReasoningSummary.AUTO, true, true);
    }

    @Override
    public boolean hasGrowingInputs() {
        return false;
    }

    @Override
    public List<Reasoning> objectsToEstimateWithLargerInput() {
        // Will be skipped as Reasoning does only have constant-size fields (booleans and Enums)
        return List.of();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.inference.DecisionQuestions;
import org.elasticsearch.inference.InferenceString;

import java.util.Objects;

/**
 * The inputs of a {@link org.elasticsearch.inference.TaskType#DECISION} request: the content to make decisions about and the questions
 * to answer, already resolved from the task settings of the inference endpoint and the request.
 */
public class DecisionInput extends InferenceInputs {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DecisionInput.class);

    private final InferenceString input;
    private final DecisionQuestions questions;

    public DecisionInput(InferenceString input, DecisionQuestions questions) {
        super(false);
        this.input = Objects.requireNonNull(input);
        this.questions = Objects.requireNonNull(questions);
    }

    public InferenceString getInput() {
        return input;
    }

    public DecisionQuestions getQuestions() {
        return questions;
    }

    @Override
    public boolean isSingleInput() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        // The questions are bounded by the request size and negligible compared to the input, which can be a base64 encoded image
        return SHALLOW_SIZE + input.ramBytesUsed();
    }
}

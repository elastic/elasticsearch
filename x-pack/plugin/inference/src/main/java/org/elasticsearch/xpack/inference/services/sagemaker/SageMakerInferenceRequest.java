/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;

import java.util.List;
import java.util.Objects;

public record SageMakerInferenceRequest(
    @Nullable String query,
    @Nullable Boolean returnDocuments,
    @Nullable Integer topN,
    List<String> input,
    boolean stream,
    InputType inputType
) {
    public SageMakerInferenceRequest {
        Objects.requireNonNull(input);
        Objects.requireNonNull(inputType);
    }
}

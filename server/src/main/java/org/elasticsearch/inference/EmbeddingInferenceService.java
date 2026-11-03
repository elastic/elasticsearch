/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

/**
 * Additional capabilities for inference services that support embedding inference.
 */
public interface EmbeddingInferenceService {

    /**
     * Whether the input must be sent in an {@link EmbeddingRequest} that contains no other inputs.
     * An isolated input may return more than one embedding.
     *
     * @param model the model used for inference
     * @param input the input that may require an isolated request
     * @return {@code true} if the input must be sent by itself
     */
    boolean requiresSingleInputEmbeddingRequest(Model model, InferenceString input);
}

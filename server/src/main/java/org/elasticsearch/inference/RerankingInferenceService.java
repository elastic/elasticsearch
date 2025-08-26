/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

public interface RerankingInferenceService {

    /**
     * The default window size for small reranking models (512 input tokens).
     */
    int CONSERVATIVE_DEFAULT_WINDOW_SIZE = 300;

    /**
     * The reranking model's max window or an approximation of
     * measured in the number of words.
     * @param modelId The model ID
     * @return Window size in words
     */
    int rerankerWindowSize(String modelId);
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.action.ActionListener;

import java.util.List;

/**
 * Provides NLP text inference results. Plugins can implement this interface to provide their own inference results.
 */
public interface InferenceProvider {
    /**
     * Returns nferenceResults for a given model ID and list of texts.
     *
     * @param modelId model identifier
     * @param texts texts to perform inference on
     * @param listener listener to be called when inference is complete
     */
    void textInference(String modelId, List<String> texts, ActionListener<List<InferenceResults>> listener);

    /**
     * Returns true if this inference provider can perform inference
     *
     * @return true if this inference provider can perform inference
     */
    boolean performsInference();

    class NoopInferenceProvider implements InferenceProvider {

        @Override
        public void textInference(String modelId, List<String> texts, ActionListener<List<InferenceResults>> listener) {
            throw new UnsupportedOperationException("No inference provider has been registered");
        }

        @Override
        public boolean performsInference() {
            return false;
        }
    }
}

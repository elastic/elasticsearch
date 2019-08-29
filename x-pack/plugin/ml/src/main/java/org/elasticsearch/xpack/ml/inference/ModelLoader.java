/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import java.util.Map;

public interface ModelLoader {
    /**
     *  Load the model with the given {@code modelId} and {@code config}
     *
     * @param modelId        The Id of the model to load
     * @param processorTag   The ingest processor tag
     * @param ignoreMissing  Should missing fields be ignored?
     * @param config         The ingest pipeline config
     * @return The loaded model
     */
    Model load(String modelId, String processorTag, boolean ignoreMissing, Map<String, Object> config) throws Exception;

    // parses the config out of the map

    /**
     * Inference processors must remove their configuration from the {@code config} map.
     * For the case when a model has already been loaded
     *
     * This method should be used when {@link #load(String, String, boolean, Map)} isn't.
     *
     * @param processorTag  The ingest processor tag
     * @param config        The ingest pipeline config
     */
    void consumeConfiguration(String processorTag, Map<String, Object> config);
}

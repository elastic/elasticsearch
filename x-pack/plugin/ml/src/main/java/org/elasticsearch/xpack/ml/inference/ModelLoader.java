/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import java.util.Map;

public interface ModelLoader {
    /**
     *  Load the model with the given {@code modelId} and {@code config}.
     *
     *  Model specific config should can read from {@code config} and should
     *  be removed from the map e.g with the use of {@link org.elasticsearch.ingest.ConfigurationUtils}
     *
     *
     * @param modelId        The Id of the model to load
     * @param processorTag   The ingest processor tag
     * @param ignoreMissing  Should missing fields be ignored?
     * @param config         The ingest pipeline config
     * @return The loaded model
     */
    Model load(String modelId, String processorTag, boolean ignoreMissing, Map<String, Object> config) throws Exception;

    /**
     * Inference processors must remove their configuration from the {@code config} map.
     * In the case when a model has already been loaded and {@link #load(String, String, boolean, Map)}
     * will not be called this use this method to strip the model specific config from the map.
     *
     * This method should only be used if {@link #load(String, String, boolean, Map)} isn't
     * called on the same {@code config} as {@link #load(String, String, boolean, Map)} will
     * also remove the processor's configuration from the map.
     *
     * @param processorTag  The ingest processor tag
     * @param config        The ingest pipeline config
     */
    void consumeConfiguration(String processorTag, Map<String, Object> config);
}

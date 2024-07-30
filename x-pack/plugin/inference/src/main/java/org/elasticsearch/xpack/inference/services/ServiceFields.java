/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

/**
 * Common strings and definitions shared by service implementations
 */
public final class ServiceFields {

    public static final String SIMILARITY = "similarity";
    public static final String DIMENSIONS = "dimensions";
    // Typically we use this to define the maximum tokens for the input text (text being sent to an integration)
    public static final String MAX_INPUT_TOKENS = "max_input_tokens";
    public static final String URL = "url";
    public static final String MODEL_ID = "model_id";
    /**
     * Represents the field elasticsearch uses to determine the embedding type (e.g. float, byte).
     * The value this field is normally set to would be one of the values in
     * {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType}
     */
    public static final String ELEMENT_TYPE = "element_type";

    private ServiceFields() {

    }
}

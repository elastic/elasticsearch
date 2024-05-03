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
    public static final String MAX_INPUT_TOKENS = "max_input_tokens";
    public static final String URL = "url";
    public static final String MODEL_ID = "model_id";

    private ServiceFields() {

    }
}

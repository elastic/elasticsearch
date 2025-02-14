/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

/**
 * Provides access to constants for core, ml, and inference plugins
 */
public class InferenceProcessorConstants {
    public static final String TYPE = "inference";
    public static final String TARGET_FIELD = "target_field";
    public static final String FIELD_MAP = "field_map";
    public static final String INFERENCE_CONFIG = "inference_config";

    private InferenceProcessorConstants() {}
}

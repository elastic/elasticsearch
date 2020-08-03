/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.persistence;

import org.elasticsearch.common.ParseField;

/**
 * Class containing the index constants so that the index version, name, and prefix are available to a wider audience.
 */
public final class InferenceIndexConstants {

    /**
     * version: 7.8.0:
     *  - adds inference_config definition to trained model config
     *
     */
    public static final String INDEX_VERSION = "000002";
    public static final String INDEX_NAME_PREFIX = ".ml-inference-";
    public static final String INDEX_PATTERN = INDEX_NAME_PREFIX + "*";
    public static final String LATEST_INDEX_NAME = INDEX_NAME_PREFIX + INDEX_VERSION;
    public static final ParseField DOC_TYPE = new ParseField("doc_type");

    private InferenceIndexConstants() {}
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request;

/**
 * Class containing field names used in Nvidia inference service requests.
 */
public final class NvidiaRequestFields {
    // Common field names
    public static final String MODEL_FIELD_NAME = "model";
    // Field name for embeddings requests
    public static final String INPUT_FIELD_NAME = "input";
    public static final String INPUT_TYPE_FIELD_NAME = "input_type";
    public static final String TRUNCATE_FIELD_NAME = "truncate";
    // Field names for rerank requests
    public static final String QUERY_FIELD_NAME = "query";
    public static final String PASSAGES_FIELD_NAME = "passages";
    public static final String TEXT_FIELD_NAME = "text";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private NvidiaRequestFields() {}
}

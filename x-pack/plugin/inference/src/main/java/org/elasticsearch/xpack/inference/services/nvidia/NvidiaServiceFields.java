/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

/**
 * Class containing field names used in Nvidia inference service.
 */
public final class NvidiaServiceFields {
    // Field names for embeddings task settings
    public static final String INPUT_TYPE_FIELD_NAME = "input_type";
    public static final String TRUNCATE_FIELD_NAME = "truncate";

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private NvidiaServiceFields() {}
}

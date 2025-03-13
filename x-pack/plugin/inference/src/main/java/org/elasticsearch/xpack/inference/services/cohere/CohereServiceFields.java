/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

public class CohereServiceFields {
    public static final String TRUNCATE = "truncate";

    /**
     * Taken from https://docs.cohere.com/reference/embed
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 96;
}

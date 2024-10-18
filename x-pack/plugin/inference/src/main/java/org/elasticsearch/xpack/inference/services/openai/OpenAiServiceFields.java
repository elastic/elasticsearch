/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

public class OpenAiServiceFields {

    public static final String USER = "user";

    public static final String ORGANIZATION = "organization_id";

    /**
     * Taken from https://platform.openai.com/docs/api-reference/embeddings/create
     */
    public static final int EMBEDDING_MAX_BATCH_SIZE = 2048;

}

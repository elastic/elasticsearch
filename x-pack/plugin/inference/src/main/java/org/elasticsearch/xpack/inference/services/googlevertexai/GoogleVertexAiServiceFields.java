/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

public class GoogleVertexAiServiceFields {

    public static final String LOCATION = "location";

    public static final String PROJECT_ID = "project_id";

    /**
     * In `us-central-1` the max input size is `250`, but in every other region it's `5` according
     * to these docs: https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings.
     *
     * Therefore, being conservative and setting it to `5`.
     */
    static final int EMBEDDING_MAX_BATCH_SIZE = 5;

}

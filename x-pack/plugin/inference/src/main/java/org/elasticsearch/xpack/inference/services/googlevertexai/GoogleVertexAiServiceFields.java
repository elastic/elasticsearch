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
    public static final String STREAMING_URL_SETTING_NAME = "streaming_url";
    public static final String PROVIDER_SETTING_NAME = "provider";
    public static final String MAX_BATCH_SIZE = "max_batch_size";

    /**
     * According to https://cloud.google.com/vertex-ai/docs/quotas#text-embedding-limits the limit is `250`.
     */
    public static final int EMBEDDING_MAX_BATCH_SIZE = 250;

}

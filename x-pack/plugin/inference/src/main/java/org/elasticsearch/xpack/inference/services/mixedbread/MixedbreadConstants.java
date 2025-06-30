/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

public class MixedbreadConstants {
    public static final String EMBEDDINGS_URI_PATH = "/v1/embeddings";
    public static final String RERANK_URI_PATH = "/v1/rerank";

    // common service settings fields
    public static final String API_KEY_FIELD = "api_key";

    // embeddings service and request settings
    public static final String INPUT_FIELD = "input";

    // rerank task settings fields
    public static final String QUERY_FIELD = "query";

    // embeddings task settings fields
    public static final String USER_FIELD = "user";

    // rerank task settings fields
    public static final String RETURN_DOCUMENTS_FIELD = "return_documents";
    public static final String TOP_K_FIELD = "top_k";

    private MixedbreadConstants() {}
}

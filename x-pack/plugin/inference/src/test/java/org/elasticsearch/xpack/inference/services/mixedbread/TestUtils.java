/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

public final class TestUtils {
    public static final String DEFAULT_EMBEDDINGS_URL = "https://api.mixedbread.com/v1/embeddings";
    public static final String DEFAULT_RERANK_URL = "https://api.mixedbread.com/v1/reranking";
    public static final String CUSTOM_URL = "https://custom.url.com/v1/task";
    public static final String MODEL_ID = "model_id_value";
    public static final String API_KEY = "secret";

    public static final Boolean RETURN_DOCUMENTS_TRUE = true;
    public static final Boolean RETURN_DOCUMENTS_FALSE = false;

    public static final int DIMENSIONS = 3;
    public static final Boolean NORMALIZED = Boolean.FALSE;

    public static final String PROMPT_INITIAL_VALUE = "prompt_initial_value";
    public static final Boolean NORMALIZED_INITIAL_VALUE = Boolean.FALSE;
    public static final String PROMPT_OVERRIDDEN_VALUE = "prompt_overridden_value";
    public static final Boolean NORMALIZED_OVERRIDDEN_VALUE = Boolean.TRUE;

    public static final Boolean DIMENSIONS_SET_BY_USER_TRUE = Boolean.TRUE;
}

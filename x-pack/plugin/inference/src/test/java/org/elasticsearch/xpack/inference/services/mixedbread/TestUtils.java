/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.common.model.Truncation;

public final class TestUtils {
    public static final String DEFAULT_EMBEDDINGS_URL = "https://api.mixedbread.com/v1/embeddings";
    public static final String DEFAULT_RERANK_URL = "https://api.mixedbread.com/v1/reranking";
    public static final String CUSTOM_URL = "https://custom.url.com/v1/task";
    public static final String MODEL_ID = "model_id_value";
    public static final String API_KEY = "secret";

    public static final Boolean RETURN_DOCUMENTS_TRUE = true;
    public static final Boolean RETURN_DOCUMENTS_FALSE = false;

    public static final InputType INPUT_TYPE_INITIAL_ELASTIC_VALUE = InputType.INGEST;
    public static final Truncation TRUNCATE_INITIAL_ELASTIC_VALUE = Truncation.START;
    public static final InputType INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE = InputType.SEARCH;
    public static final Truncation TRUNCATE_OVERRIDDEN_ELASTIC_VALUE = Truncation.END;
}

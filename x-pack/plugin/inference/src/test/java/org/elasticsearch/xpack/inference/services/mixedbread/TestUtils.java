/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.inference.SimilarityMeasure;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthOrNull;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeIntOrNull;
import static org.elasticsearch.test.ESTestCase.randomOptionalBoolean;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;

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
    public static final String ENCODING_VALUE = "float";

    public static final String PROMPT_INITIAL_VALUE = "prompt_initial_value";
    public static final Boolean NORMALIZED_INITIAL_VALUE = Boolean.FALSE;
    public static final String PROMPT_OVERRIDDEN_VALUE = "prompt_overridden_value";
    public static final Boolean NORMALIZED_OVERRIDDEN_VALUE = Boolean.TRUE;

    public static final Boolean DIMENSIONS_SET_BY_USER_TRUE = Boolean.TRUE;

    public static final String MODEL_ID_RANDOM = randomAlphaOfLength(10);
    public static final String API_KEY_RANDOM = randomAlphaOfLength(10);
    public static final String PROMPT_RANDOM = randomAlphaOfLengthOrNull(10);
    public static final Integer MAX_INPUT_TOKENS_RANDOM = randomFrom(randomIntBetween(128, 256), null);
    public static final Integer DIMENSIONS_RANDOM = randomNonNegativeIntOrNull();
    public static final boolean BOOLEAN_RANDOM = randomBoolean();
    public static final SimilarityMeasure SIMILARITY_RANDOM = randomFrom(randomSimilarityMeasure(), null);
}

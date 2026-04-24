/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import java.util.List;

public final class ContextualAiRerankTestFixtures {

    private ContextualAiRerankTestFixtures() {}

    public static final String TEST_INFERENCE_ENTITY_ID = "some_inference_entity_id";

    public static final boolean TEST_RETURN_DOCUMENTS = true;
    public static final boolean INITIAL_TEST_RETURN_DOCUMENTS = false;
    public static final boolean NEW_TEST_RETURN_DOCUMENTS = true;

    public static final int TEST_TOP_N = 5;
    public static final int INITIAL_TEST_TOP_N = 10;
    public static final int NEW_TEST_TOP_N = 15;

    public static final String TEST_INSTRUCTION = "some instruction";
    public static final String INITIAL_TEST_INSTRUCTION = "original instruction";
    public static final String NEW_TEST_INSTRUCTION = "new instruction";

    public static final String TEST_MODEL_ID = "some_model_id";
    public static final String INITIAL_TEST_MODEL_ID = "initial_model_id";
    public static final String NEW_TEST_MODEL_ID = "new_model_id";

    public static final String TEST_API_KEY = "some_api_key";

    public static final String DEFAULT_RERANK_URL = "https://api.contextual.ai/v1/rerank";

    public static final String TEST_FIRST_DOCUMENT = "some document";
    public static final String TEST_SECOND_DOCUMENT = "some other document";
    public static final List<String> TEST_DOCUMENTS = List.of(TEST_FIRST_DOCUMENT, TEST_SECOND_DOCUMENT);

    public static final String TEST_QUERY = "some query";

    public static final int TEST_RATE_LIMIT = 200;
    public static final int INITIAL_TEST_RATE_LIMIT = 300;
    public static final int NEW_TEST_RATE_LIMIT = 600;
    public static final int DEFAULT_RATE_LIMIT = 1000;

    public static final String INVALID_STRING_VALUE = "invalid";

}

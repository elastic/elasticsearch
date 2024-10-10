/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

public class AzureAiStudioConstants {
    public static final String EMBEDDINGS_URI_PATH = "/v1/embeddings";
    public static final String COMPLETIONS_URI_PATH = "/v1/chat/completions";

    // common service settings fields
    public static final String TARGET_FIELD = "target";
    public static final String ENDPOINT_TYPE_FIELD = "endpoint_type";
    public static final String PROVIDER_FIELD = "provider";
    public static final String API_KEY_FIELD = "api_key";

    // embeddings service and request settings
    public static final String INPUT_FIELD = "input";
    public static final String DIMENSIONS_FIELD = "dimensions";
    public static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";

    // embeddings task settings fields
    public static final String USER_FIELD = "user";

    // completion task settings fields
    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String TOP_P_FIELD = "top_p";
    public static final String DO_SAMPLE_FIELD = "do_sample";
    public static final String MAX_TOKENS_FIELD = "max_tokens";
    public static final String MAX_NEW_TOKENS_FIELD = "max_new_tokens";

    public static final Double MIN_TEMPERATURE_TOP_P = 0.0;
    public static final Double MAX_TEMPERATURE_TOP_P = 2.0;

    private AzureAiStudioConstants() {}
}

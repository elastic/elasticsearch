/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

public class AmazonBedrockConstants {
    public static final String ACCESS_KEY_FIELD = "access_key";
    public static final String SECRET_KEY_FIELD = "secret_key";
    public static final String REGION_FIELD = "region";
    public static final String MODEL_FIELD = "model";
    public static final String PROVIDER_FIELD = "provider";

    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String TOP_P_FIELD = "top_p";
    public static final String TOP_K_FIELD = "top_k";
    public static final String MAX_NEW_TOKENS_FIELD = "max_new_tokens";

    public static final String TRUNCATE_FIELD = "truncate";

    public static final Double MIN_TEMPERATURE_TOP_P_TOP_K_VALUE = 0.0;
    public static final Double MAX_TEMPERATURE_TOP_P_TOP_K_VALUE = 1.0;

    public static final int DEFAULT_MAX_CHUNK_SIZE = 2048;

}

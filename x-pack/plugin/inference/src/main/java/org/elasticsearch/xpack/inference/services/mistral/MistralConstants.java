/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

public class MistralConstants {
    public static final String MISTRAL_API_EMBEDDINGS_PATH = "https://api.mistral.ai/v1/embeddings";

    public static final String MISTRAL_API_KEY_FIELD = "api_key";
    public static final String DIMENSIONS_SET_BY_USER = "dimensions_set_by_user";

    public static final String MISTRAL_MODEL_FIELD = "model";
    public static final String MISTRAL_INPUT_FIELD = "input";
    public static final String MISTRAL_ENCODING_FORMAT_FIELD = "encoding_format";
}

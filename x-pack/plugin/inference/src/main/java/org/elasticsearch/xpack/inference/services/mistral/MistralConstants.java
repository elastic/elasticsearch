/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

public class MistralConstants {
    public static final String API_EMBEDDINGS_PATH = "https://api.mistral.ai/v1/embeddings";
    public static final String API_COMPLETIONS_PATH = "https://api.mistral.ai/v1/chat/completions";

    // note - there is no bounds information available from Mistral,
    // so we'll use a sane default here which is the same as Cohere's
    public static final int MAX_BATCH_SIZE = 96;

    public static final String API_KEY_FIELD = "api_key";
    public static final String MODEL_FIELD = "model";
    public static final String INPUT_FIELD = "input";
    public static final String ENCODING_FORMAT_FIELD = "encoding_format";
    public static final String MAX_TOKENS_FIELD = "max_tokens";
    public static final String DETAIL_FIELD = "detail";
    public static final String MSG_FIELD = "msg";
    public static final String MESSAGE_FIELD = "message";
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.xpack.inference.services.googlevertexai.response.GoogleVertexAiCompletionResponseEntity;

public class GoogleVertexAiChatCompletionResponseHandler extends GoogleVertexAiResponseHandler {

    public GoogleVertexAiChatCompletionResponseHandler(String requestType) {
        super(
            requestType,
            GoogleVertexAiCompletionResponseEntity::fromResponse,
            GoogleVertexAiUnifiedChatCompletionResponseHandler.GoogleVertexAiErrorResponse::fromResponse,
            true
        );
    }
}

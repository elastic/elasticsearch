/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponse;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseEntity;

public class AmazonBedrockChatCompletionResponseEntity implements AmazonBedrockResponseEntity {

    @Override
    public InferenceServiceResults getResults(AmazonBedrockRequest request, AmazonBedrockResponse response) {
        return response.accept(request);
    }
}

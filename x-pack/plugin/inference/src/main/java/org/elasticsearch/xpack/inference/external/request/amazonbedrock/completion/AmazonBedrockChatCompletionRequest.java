/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ConverseRequest;
import com.amazonaws.services.bedrockruntime.model.ConverseResult;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.external.amazonbedrock.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;

import java.io.IOException;

public class AmazonBedrockChatCompletionRequest extends AmazonBedrockRequest {
    public static final String USER_ROLE = "user";
    private final AmazonBedrockConverseRequestEntity requestEntity;
    private ConverseResult result;

    public AmazonBedrockChatCompletionRequest(
        AmazonBedrockChatCompletionModel model,
        AmazonBedrockConverseRequestEntity requestEntity,
        @Nullable TimeValue timeout
    ) {
        super(model, timeout);
        this.requestEntity = requestEntity;
    }

    public ConverseResult result() {
        return result;
    }

    @Override
    public void executeRequest(AmazonBedrockBaseClient client) {
        var converseRequest = getConverseRequest();

        try {
            result = SocketAccess.doPrivileged(() -> client.converse(converseRequest));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ConverseRequest getConverseRequest() {
        var converseRequest = new ConverseRequest().withModelId(amazonBedrockModel.model());
        converseRequest = requestEntity.addMessages(converseRequest);
        converseRequest = requestEntity.addInferenceConfig(converseRequest);
        converseRequest = requestEntity.addAdditionalModelFields(converseRequest);
        return converseRequest;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockChatCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class AmazonBedrockActionCreator implements AmazonBedrockActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;
    private final TimeValue timeout;

    public AmazonBedrockActionCreator(Sender sender, ServiceComponents serviceComponents, @Nullable TimeValue timeout) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
        this.timeout = timeout;
    }

    @Override
    public ExecutableAction create(AmazonBedrockEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        var overriddenModel = AmazonBedrockEmbeddingsModel.of(embeddingsModel, taskSettings);
        var requestManager = new AmazonBedrockEmbeddingsRequestManager(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool(),
            timeout
        );
        var errorMessage = constructFailedToSendRequestMessage("Amazon Bedrock embeddings");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }

    @Override
    public ExecutableAction create(AmazonBedrockChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        var overriddenModel = AmazonBedrockChatCompletionModel.of(completionModel, taskSettings);
        var requestManager = new AmazonBedrockChatCompletionRequestManager(overriddenModel, serviceComponents.threadPool(), timeout);
        var errorMessage = constructFailedToSendRequestMessage("Amazon Bedrock completion");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }
}

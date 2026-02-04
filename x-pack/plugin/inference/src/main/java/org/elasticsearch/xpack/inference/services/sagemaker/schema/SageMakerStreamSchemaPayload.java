/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema;

import software.amazon.awssdk.core.SdkBytes;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;

import java.util.EnumSet;

/**
 * Implemented for models that support streaming.
 * This is an extension of {@link SageMakerSchemaPayload} because Elastic expects Completion tasks to handle both streaming and
 * non-streaming, and all models currently support toggling streaming on/off.
 */
public interface SageMakerStreamSchemaPayload extends SageMakerSchemaPayload {
    /**
     * We currently only support streaming for Completion and Chat Completion, and if we are going to implement one then we should implement
     * the other, so this interface requires both streaming input and streaming unified input.
     * If we ever allowed streaming for more than just Completion, then we'd probably break up this class so that Unified Chat Completion
     * was its own interface.
     */
    @Override
    default EnumSet<TaskType> supportedTasks() {
        return EnumSet.of(TaskType.COMPLETION, TaskType.CHAT_COMPLETION);
    }

    /**
     * This API would only be called for Completion task types. {@link #requestBytes(SageMakerModel, SageMakerInferenceRequest)} would
     * handle the request translation for both streaming and non-streaming.
     */
    StreamingChatCompletionResults.Results streamResponseBody(SageMakerModel model, SdkBytes response) throws Exception;

    SdkBytes chatCompletionRequestBytes(SageMakerModel model, UnifiedCompletionRequest request) throws Exception;

    StreamingUnifiedChatCompletionResults.Results chatCompletionResponseBody(SageMakerModel model, SdkBytes response) throws Exception;
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;

import java.util.function.Supplier;

public interface ChatInputExecutor<I extends InferenceInputs> {
    void execute(
        AmazonBedrockChatCompletionModel model,
        I inputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompleted,
        TimeValue timeout,
        ActionListener<InferenceServiceResults> listener);
}

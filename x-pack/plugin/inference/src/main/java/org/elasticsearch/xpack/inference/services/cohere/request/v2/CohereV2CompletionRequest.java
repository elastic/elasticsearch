/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v2;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CohereV2CompletionRequest extends CohereRequest {
    private final List<String> input;

    public CohereV2CompletionRequest(List<String> input, CohereCompletionModel model, boolean stream) {
        super(CohereAccount.of(model), model.getInferenceEntityId(), Objects.requireNonNull(model.getServiceSettings().modelId()), stream);

        this.input = Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // we only allow one input for completion, so always get the first one
        builder.field(CohereUtils.MESSAGE_FIELD, input.getFirst());
        builder.field(CohereUtils.MODEL_FIELD, getModelId());
        builder.field(CohereUtils.STREAM_FIELD, isStreaming());
        builder.endObject();
        return builder;
    }

    @Override
    protected List<String> pathSegments() {
        return List.of(CohereUtils.VERSION_2, CohereUtils.CHAT_PATH);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v1;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModel;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

public class CohereV1CompletionRequest extends CohereRequest {
    private final List<String> input;

    public CohereV1CompletionRequest(List<String> input, CohereCompletionModel model, boolean stream) {
        super(
            CohereAccount.of(model, CohereV1CompletionRequest::buildDefaultUri),
            model.getInferenceEntityId(),
            model.getServiceSettings().modelId(),
            stream
        );

        this.input = Objects.requireNonNull(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // we only allow one input for completion, so always get the first one
        builder.field(CohereUtils.MESSAGE_FIELD, input.getFirst());
        if (getModelId() != null) {
            builder.field(CohereUtils.MODEL_FIELD, getModelId());
        }
        if (isStreaming()) {
            builder.field(CohereUtils.STREAM_FIELD, true);
        }
        builder.endObject();
        return builder;
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(CohereUtils.HOST)
            .setPathSegments(CohereUtils.VERSION_1, CohereUtils.CHAT_PATH)
            .build();
    }
}

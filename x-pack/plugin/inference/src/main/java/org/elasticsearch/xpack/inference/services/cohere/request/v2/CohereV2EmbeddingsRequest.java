/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v2;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

public class CohereV2EmbeddingsRequest extends CohereRequest {

    private final List<String> input;
    private final InputType inputType;
    private final CohereEmbeddingsTaskSettings taskSettings;
    private final CohereEmbeddingType embeddingType;

    public CohereV2EmbeddingsRequest(List<String> input, InputType inputType, CohereEmbeddingsModel embeddingsModel) {
        super(
            CohereAccount.of(embeddingsModel, CohereV2EmbeddingsRequest::buildDefaultUri),
            embeddingsModel.getInferenceEntityId(),
            Objects.requireNonNull(embeddingsModel.getServiceSettings().getCommonSettings().modelId()),
            false
        );

        this.input = Objects.requireNonNull(input);
        this.inputType = Objects.requireNonNull(inputType); // inputType is required in v2
        taskSettings = embeddingsModel.getTaskSettings();
        embeddingType = embeddingsModel.getServiceSettings().getEmbeddingType();
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(CohereUtils.HOST)
            .setPathSegments(CohereUtils.VERSION_2, CohereUtils.EMBEDDINGS_PATH)
            .build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CohereUtils.TEXTS_FIELD, input);
        builder.field(CohereUtils.MODEL_FIELD, getModelId());
        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(CohereUtils.INPUT_TYPE_FIELD, CohereUtils.inputTypeToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(CohereUtils.INPUT_TYPE_FIELD, CohereUtils.inputTypeToString(taskSettings.getInputType()));
        }
        builder.field(CohereUtils.EMBEDDING_TYPES_FIELD, List.of(embeddingType.toRequestString()));
        if (taskSettings.getTruncation() != null) {
            builder.field(CohereServiceFields.TRUNCATE, taskSettings.getTruncation());
        }
        builder.endObject();
        return builder;
    }
}

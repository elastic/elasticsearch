/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v1;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceFields;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingType;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereRequest;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CohereV1EmbeddingsRequest extends CohereRequest {

    private final List<String> input;
    private final InputType inputType;
    private final CohereEmbeddingsTaskSettings taskSettings;
    private final CohereEmbeddingType embeddingType;

    public CohereV1EmbeddingsRequest(List<String> input, InputType inputType, CohereEmbeddingsModel embeddingsModel) {
        super(
            CohereAccount.of(embeddingsModel),
            embeddingsModel.getInferenceEntityId(),
            embeddingsModel.getServiceSettings().getCommonSettings().modelId(),
            false
        );

        this.input = Objects.requireNonNull(input);
        this.inputType = inputType;
        taskSettings = embeddingsModel.getTaskSettings();
        embeddingType = embeddingsModel.getServiceSettings().getEmbeddingType();
    }

    @Override
    protected List<String> pathSegments() {
        return List.of(CohereUtils.VERSION_1, CohereUtils.EMBEDDINGS_PATH);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CohereUtils.TEXTS_FIELD, input);
        if (getModelId() != null) {
            builder.field(CohereServiceSettings.OLD_MODEL_ID_FIELD, getModelId());
        }

        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(CohereUtils.INPUT_TYPE_FIELD, CohereUtils.inputTypeToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(CohereUtils.INPUT_TYPE_FIELD, CohereUtils.inputTypeToString(taskSettings.getInputType()));
        }

        if (embeddingType != null) {
            builder.field(CohereUtils.EMBEDDING_TYPES_FIELD, List.of(embeddingType.toRequestString()));
        }

        if (taskSettings.getTruncation() != null) {
            builder.field(CohereServiceFields.TRUNCATE, taskSettings.getTruncation());
        }

        builder.endObject();
        return builder;
    }
}

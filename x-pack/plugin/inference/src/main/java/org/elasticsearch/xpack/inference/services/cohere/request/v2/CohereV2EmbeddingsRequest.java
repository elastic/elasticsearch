/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v2;

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
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils.CONTENT_FIELD;
import static org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils.CONTENT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils.IMAGE_URL_FIELD;
import static org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils.INPUTS_FIELD;
import static org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils.URL_FIELD;

public class CohereV2EmbeddingsRequest extends CohereRequest {

    private final List<String> input;
    private final InputType inputType;
    private final CohereEmbeddingsTaskSettings taskSettings;
    private final CohereEmbeddingType embeddingType;
    private final List<String> imageUrls;

    public CohereV2EmbeddingsRequest(
        List<String> input,
        InputType inputType,
        CohereEmbeddingsModel embeddingsModel,
        List<String> imageUrls
    ) {
        super(
            CohereAccount.of(embeddingsModel),
            embeddingsModel.getInferenceEntityId(),
            Objects.requireNonNull(embeddingsModel.getServiceSettings().getCommonSettings().modelId()),
            false
        );

        this.input = Objects.requireNonNull(input);
        this.inputType = Optional.ofNullable(inputType).orElse(InputType.SEARCH); // inputType is required in v2
        taskSettings = embeddingsModel.getTaskSettings();
        embeddingType = embeddingsModel.getServiceSettings().getEmbeddingType();
        this.imageUrls = imageUrls == null ? List.of() : imageUrls;
    }

    @Override
    protected List<String> pathSegments() {
        return List.of(CohereUtils.VERSION_2, CohereUtils.EMBEDDINGS_PATH);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(INPUTS_FIELD);
        for (String anInput : input) {
            addInput(builder, anInput, true);
        }
        for (String url : imageUrls) {
            addInput(builder, url, false);
        }
        builder.endArray();

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

    private static void addInput(XContentBuilder builder, String anInput, boolean isText) throws IOException {
        builder.startObject();
        builder.startArray(CONTENT_FIELD);
        if (isText) {
            addText(builder, anInput);
        } else {
            addImageUrl(builder, anInput);
        }
        builder.endArray();
        builder.endObject();
    }

    private static void addText(XContentBuilder builder, String anInput) throws IOException {
        builder.startObject();
        builder.field(CONTENT_TYPE_FIELD, TEXT_FIELD);
        builder.field(TEXT_FIELD, anInput);
        builder.endObject();
    }

    private static void addImageUrl(XContentBuilder builder, String url) throws IOException {
        builder.startObject();
        builder.field(CONTENT_TYPE_FIELD, IMAGE_URL_FIELD);
        builder.startObject(IMAGE_URL_FIELD);
        builder.field(URL_FIELD, url);
        builder.endObject();
        builder.endObject();
    }
}

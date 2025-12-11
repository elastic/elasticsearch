/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkerUtils;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

public record JinaAIEmbeddingsRequestEntity(List<String> input, InputType inputType, JinaAIEmbeddingsModel model)
    implements
        ToXContentObject {

    private static final String SEARCH_DOCUMENT = "retrieval.passage";
    private static final String SEARCH_QUERY = "retrieval.query";
    private static final String CLUSTERING = "separation";
    private static final String CLASSIFICATION = "classification";
    private static final String INPUT_FIELD = "input";
    private static final String MODEL_FIELD = "model";
    public static final String TASK_TYPE_FIELD = "task";
    public static final String LATE_CHUNKING = "late_chunking";
    static final String EMBEDDING_TYPE_FIELD = "embedding_type";
    static final String DIMENSIONS_FIELD = "dimensions";
    // Late chunking models have a token limit of 8000 or ~6000 words (using a rough 1 token:0.75 words ratio). We set the maximum word
    // count with a bit of extra room to 5500 words.
    static final int MAX_WORD_COUNT_FOR_LATE_CHUNKING = 5500;

    public JinaAIEmbeddingsRequestEntity {
        Objects.requireNonNull(input);
        Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INPUT_FIELD, input);
        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());

        builder.field(EMBEDDING_TYPE_FIELD, model.getServiceSettings().getEmbeddingType().toRequestString());

        // prefer the root level inputType over task settings input type
        JinaAIEmbeddingsTaskSettings taskSettings = model.getTaskSettings();
        if (InputType.isSpecified(inputType)) {
            builder.field(TASK_TYPE_FIELD, convertInputType(inputType));
        } else {
            if (InputType.isSpecified(taskSettings.getInputType())) {
                builder.field(TASK_TYPE_FIELD, convertInputType(taskSettings.getInputType()));
            }
        }

        if (taskSettings.getLateChunking() != null) {
            builder.field(LATE_CHUNKING, taskSettings.getLateChunking() && getInputWordCount() <= MAX_WORD_COUNT_FOR_LATE_CHUNKING);
        }

        if (model.getServiceSettings().dimensionsSetByUser() && model.getServiceSettings().dimensions() != null) {
            builder.field(DIMENSIONS_FIELD, model.getServiceSettings().dimensions());
        }

        builder.endObject();
        return builder;
    }

    // default for testing
    static String convertInputType(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> SEARCH_DOCUMENT;
            case SEARCH, INTERNAL_SEARCH -> SEARCH_QUERY;
            case CLASSIFICATION -> CLASSIFICATION;
            case CLUSTERING -> CLUSTERING;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }

    private int getInputWordCount() {
        int wordCount = 0;
        for (var text : input) {
            wordCount += ChunkerUtils.countWords(text);
        }

        return wordCount;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.chunking.ChunkerUtils;
import org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsModel;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.INTERNAL_SEARCH;
import static org.elasticsearch.inference.InputType.SEARCH;
import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

/**
 * Class representing the request body for a Jina embedding request. Depending on whether the {@link JinaAIEmbeddingsModel} passed into the
 * constructor supports multimodal inputs, and what the inputs are, the input field may take several different forms.
 * <br>
 * For text-only models:
 * <pre><code>
 *     "input": ["text input"]
 * </code></pre>
 * or
 * <pre><code>
 *     "input": ["first text input", "second text input", "third text input"]
 * </code></pre>
 * For multimodal models using text, image, audio or video inputs:
 * <pre><code>
 *     "input": [
 *       {"text":"text input"},
 *       {"image":"data:image/jpeg;base64,..."},
 *       {"audio":"data:audio/mpeg;base64,..."},
 *       {"video":"data:video/mp4;base64,..."}
 *     ]
 * </code></pre>
 * For multimodal models using multiple inputs to generate a single embedding vector (the below inputs would result in two vectors being
 * generated):
 * <pre><code>
 *     "input": [
 *       {
 *         "content": [
 *           {"text":"text input"},
 *           {"video":"data:video/mp4;base64,..."}
 *         ]
 *       },
 *       {
 *         "image":"data:image/jpeg;base64,..."
 *       }
 *     ]
 * </code></pre>
 * For multimodal models using a single PDF input:
 * <pre><code>
 *     "input": {
 *       "pdf":"data:application/pdf;base64,..."
 *     }
 * </code></pre>
 * @param input the inputs to be used to generate embeddings
 * @param inputType the {@link InputType} to use, which will be translated to Jina's "task" field, may be null
 * @param model the {@link JinaAIEmbeddingsModel} whose settings will be used when generating the request
 */
public record JinaAIEmbeddingsRequestEntity(List<InferenceStringGroup> input, @Nullable InputType inputType, JinaAIEmbeddingsModel model)
    implements
        ToXContentObject {

    public static final String INPUT_FIELD = "input";
    public static final String INPUT_TEXT_FIELD = "text";
    public static final String MODEL_FIELD = "model";
    private static final String RETRIEVAL_PASSAGE = "retrieval.passage";
    private static final String RETRIEVAL_QUERY = "retrieval.query";
    private static final String SEPARATION = "separation";
    private static final String CLASSIFICATION = "classification";
    private static final String INPUT_CONTENT_FIELD = "content";
    private static final String INPUT_IMAGE_FIELD = "image";
    private static final String INPUT_AUDIO_FIELD = "audio";
    private static final String INPUT_VIDEO_FIELD = "video";
    private static final String INPUT_PDF_FIELD = "pdf";
    public static final String TASK_TYPE_FIELD = "task";
    public static final String LATE_CHUNKING = "late_chunking";
    public static final String EMBEDDING_TYPE_FIELD = "embedding_type";
    public static final String JINA_CLIP_V_2_MODEL_NAME = "jina-clip-v2";

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
        writeInputs(builder);
        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());

        builder.field(EMBEDDING_TYPE_FIELD, model.getServiceSettings().getEmbeddingType().toRequestString());

        // Prefer the root level inputType over task settings input type.
        var taskSettings = model.getTaskSettings();
        InputType inputTypeToUse = null;
        if (InputType.isSpecified(inputType)) {
            inputTypeToUse = inputType;
        } else {
            var taskSettingsInputType = taskSettings.getInputType();
            if (InputType.isSpecified(taskSettingsInputType)) {
                inputTypeToUse = taskSettingsInputType;
            }
        }

        // Do not specify the "task" field if the provided input type is null or not supported by the model
        if (shouldWriteInputType(model, inputTypeToUse)) {
            builder.field(TASK_TYPE_FIELD, convertInputType(inputTypeToUse));
        }

        if (taskSettings.getLateChunking() != null) {
            builder.field(
                LATE_CHUNKING,
                // Late chunking is not supported for image inputs
                taskSettings.getLateChunking()
                    && InferenceStringGroup.containsNonTextEntry(input) == false
                    && getInputWordCount() <= MAX_WORD_COUNT_FOR_LATE_CHUNKING
            );
        }

        if (model.getServiceSettings().dimensionsSetByUser() && model.getServiceSettings().dimensions() != null) {
            builder.field(DIMENSIONS_FIELD, model.getServiceSettings().dimensions());
        }

        builder.endObject();
        return builder;
    }

    private void writeInputs(XContentBuilder builder) throws IOException {
        builder.field(INPUT_FIELD);
        if (input.size() == 1 && input.getFirst().containsPdfEntry()) {
            // PDF input must be written as a single entry, not an array
            // See https://api.jina.ai/scalar#tag/search-foundation-models/POST/v1/embeddings
            writeInferenceStringGroup(builder, input.getFirst());
        } else {
            builder.startArray();
            for (var inferenceStringGroup : input) {
                if (model.getServiceSettings().isMultimodal()) {
                    writeInferenceStringGroup(builder, inferenceStringGroup);
                } else {
                    builder.value(inferenceStringGroup.textValue());
                }
            }
            builder.endArray();
        }
    }

    private static void writeInferenceStringGroup(XContentBuilder builder, InferenceStringGroup inferenceStringGroup) throws IOException {
        boolean multipleContentObjects = inferenceStringGroup.containsMultipleInferenceStrings();
        if (multipleContentObjects) {
            builder.startObject();
            builder.startArray(INPUT_CONTENT_FIELD);
        }
        for (var inferenceString : inferenceStringGroup.inferenceStrings()) {
            writeInferenceString(builder, inferenceString);
        }
        if (multipleContentObjects) {
            builder.endArray();
            builder.endObject();
        }
    }

    // default for testing
    static String convertInputType(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> RETRIEVAL_PASSAGE;
            case SEARCH, INTERNAL_SEARCH -> RETRIEVAL_QUERY;
            case CLASSIFICATION -> CLASSIFICATION;
            case CLUSTERING -> SEPARATION;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }

    private int getInputWordCount() {
        int wordCount = 0;
        for (var inferenceStringGroup : input) {
            wordCount += ChunkerUtils.countWords(inferenceStringGroup.textValue());
        }

        return wordCount;
    }

    private static boolean shouldWriteInputType(JinaAIEmbeddingsModel model, @Nullable InputType inputType) {
        if (inputType == null) {
            return false;
        }
        if (JINA_CLIP_V_2_MODEL_NAME.equalsIgnoreCase(model.getServiceSettings().modelId())) {
            // jina-clip-v2 only accepts "retrieval.query" for the "task" field
            return SEARCH.equals(inputType) || INTERNAL_SEARCH.equals(inputType);
        }
        return true;
    }

    private static void writeInferenceString(XContentBuilder builder, InferenceString inferenceString) throws IOException {
        var fieldName = switch (inferenceString.dataType()) {
            case TEXT -> INPUT_TEXT_FIELD;
            case IMAGE -> INPUT_IMAGE_FIELD;
            case AUDIO -> INPUT_AUDIO_FIELD;
            case VIDEO -> INPUT_VIDEO_FIELD;
            case PDF -> INPUT_PDF_FIELD;
        };
        builder.startObject();
        builder.field(fieldName, inferenceString.value());
        builder.endObject();
    }
}

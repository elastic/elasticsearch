/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.inference.InputType.invalidInputTypeMessage;

/**
 * Request entity for VoyageAI multimodal embeddings API.
 * Supports text, image_base64, image_url, video_base64, and video_url content types.
 *
 * VoyageAI multimodal API format:
 * <pre>
 * {
 *   "inputs": [
 *     {
 *       "content": [
 *         {"type": "text", "text": "text content"},
 *         {"type": "image_base64", "image_base64": "base64 encoded image data"}
 *       ]
 *     }
 *   ],
 *   "model": "voyage-multimodal-3"
 * }
 * </pre>
 */
public record VoyageAIMultimodalEmbeddingsRequestEntity(
    List<InferenceStringGroup> inputs,
    InputType inputType,
    VoyageAIMultimodalEmbeddingsServiceSettings serviceSettings,
    VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
    String model
) implements ToXContentObject {

    // VoyageAI content type constants
    private static final String TYPE_TEXT = "text";
    private static final String TYPE_IMAGE_BASE64 = "image_base64";
    private static final String TYPE_IMAGE_URL = "image_url";
    private static final String TYPE_VIDEO_BASE64 = "video_base64";
    private static final String TYPE_VIDEO_URL = "video_url";

    // Input type values
    private static final String DOCUMENT = "document";
    private static final String QUERY = "query";

    // JSON field names
    private static final String INPUTS_FIELD = "inputs";
    private static final String CONTENT_FIELD = "content";
    private static final String TYPE_FIELD = "type";
    private static final String MODEL_FIELD = "model";
    public static final String INPUT_TYPE_FIELD = "input_type";
    public static final String TRUNCATION_FIELD = "truncation";

    public VoyageAIMultimodalEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        // Build multimodal inputs structure: inputs[{content: [{type: "text", text: "..."}]}]
        builder.startArray(INPUTS_FIELD);
        for (InferenceStringGroup group : inputs) {
            builder.startObject();
            builder.startArray(CONTENT_FIELD);
            for (InferenceString inferenceString : group.inferenceStrings()) {
                writeInferenceString(builder, inferenceString);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();

        builder.field(MODEL_FIELD, model);

        // prefer the root level inputType over task settings input type
        if (InputType.isSpecified(inputType)) {
            builder.field(INPUT_TYPE_FIELD, convertToString(inputType));
        } else if (InputType.isSpecified(taskSettings.getInputType())) {
            builder.field(INPUT_TYPE_FIELD, convertToString(taskSettings.getInputType()));
        }

        if (taskSettings.getTruncation() != null) {
            builder.field(TRUNCATION_FIELD, taskSettings.getTruncation());
        }

        // Note: multimodal embeddings API does NOT support output_dimension or output_dtype

        builder.endObject();
        return builder;
    }

    /**
     * Writes an InferenceString to the XContentBuilder in VoyageAI multimodal format.
     * Maps Elasticsearch InferenceString types to VoyageAI content types:
     * - TEXT/TEXT → {"type": "text", "text": "..."}
     * - IMAGE/BASE64 → {"type": "image_base64", "image_base64": "..."}
     *
     * Note: VIDEO types are not yet supported in Elasticsearch's InferenceString,
     * but the format would be:
     * - VIDEO/BASE64 → {"type": "video_base64", "video_base64": "..."}
     * - VIDEO/URL → {"type": "video_url", "video_url": "..."}
     */
    private void writeInferenceString(XContentBuilder builder, InferenceString inferenceString) throws IOException {
        builder.startObject();

        String voyageType = mapToVoyageType(inferenceString);
        builder.field(TYPE_FIELD, voyageType);
        builder.field(voyageType, normalizeDataUri(inferenceString.value()));

        builder.endObject();
    }

    /**
     * Normalizes data URIs for VoyageAI compatibility.
     * VoyageAI requires "image/jpeg" but some sources use "image/jpg".
     */
    private String normalizeDataUri(String value) {
        if (value != null && value.startsWith("data:image/jpg;")) {
            return value.replaceFirst("data:image/jpg;", "data:image/jpeg;");
        }
        return value;
    }

    /**
     * Maps an InferenceString to the corresponding VoyageAI content type.
     */
    private String mapToVoyageType(InferenceString inferenceString) {
        return switch (inferenceString.dataType()) {
            case TEXT -> TYPE_TEXT;
            case IMAGE -> switch (inferenceString.dataFormat()) {
                case BASE64 -> TYPE_IMAGE_BASE64;
                case TEXT -> TYPE_IMAGE_URL;  // TEXT format for IMAGE type means URL
            };
        };
    }

    public static String convertToString(InputType inputType) {
        return switch (inputType) {
            case null -> null;
            case INGEST, INTERNAL_INGEST -> DOCUMENT;
            case SEARCH, INTERNAL_SEARCH -> QUERY;
            default -> {
                assert false : invalidInputTypeMessage(inputType);
                yield null;
            }
        };
    }
}

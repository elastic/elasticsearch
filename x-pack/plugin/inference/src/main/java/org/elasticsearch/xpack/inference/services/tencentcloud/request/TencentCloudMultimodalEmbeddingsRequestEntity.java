/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Request body for TencentCloud {@code POST /v1/multimodal-embeddings} in separate (non-fusion) mode.
 * <pre>
 *   { "model": "WeMM-Embedding-2B", "texts": [...], "image_data": [...], "video_url": [...], "prompt_name": "SearchQuery" }
 * </pre>
 * Each input is placed in the array of its modality and format; arrays without inputs are omitted. The API returns one embedding list
 * per modality, so the position of an input within its modality identifies its embedding in the response.
 *
 * @param inputs the text, image and video inputs in request order
 * @param modelId the model to call
 * @param promptName the text prefix to apply, only sent when the request contains texts
 */
public record TencentCloudMultimodalEmbeddingsRequestEntity(List<InferenceString> inputs, String modelId, @Nullable String promptName)
    implements
        ToXContentObject {

    public static final String MODEL_FIELD = "model";
    public static final String TEXTS_FIELD = "texts";
    public static final String IMAGE_DATA_FIELD = "image_data";
    public static final String IMAGE_URL_FIELD = "image_url";
    public static final String VIDEO_DATA_FIELD = "video_data";
    public static final String VIDEO_URL_FIELD = "video_url";
    public static final String PROMPT_NAME_FIELD = "prompt_name";

    private static final List<String> INPUT_FIELDS = List.of(
        TEXTS_FIELD,
        IMAGE_DATA_FIELD,
        IMAGE_URL_FIELD,
        VIDEO_DATA_FIELD,
        VIDEO_URL_FIELD
    );

    public TencentCloudMultimodalEmbeddingsRequestEntity {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(modelId);
    }

    /**
     * Returns the request field that carries the given input.
     */
    public static String inputField(InferenceString input) {
        var isUrl = input.dataFormat() == DataFormat.URL;
        return switch (input.dataType()) {
            case TEXT -> TEXTS_FIELD;
            case IMAGE -> isUrl ? IMAGE_URL_FIELD : IMAGE_DATA_FIELD;
            case VIDEO -> isUrl ? VIDEO_URL_FIELD : VIDEO_DATA_FIELD;
            default -> throw new IllegalArgumentException("Unsupported input type [" + input.dataType() + "]");
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_FIELD, modelId);
        for (var field : INPUT_FIELDS) {
            var values = inputs.stream().filter(input -> inputField(input).equals(field)).map(InferenceString::value).toList();
            if (values.isEmpty() == false) {
                builder.field(field, values);
            }
        }
        if (promptName != null && inputs.stream().anyMatch(InferenceString::isText)) {
            builder.field(PROMPT_NAME_FIELD, promptName);
        }
        builder.endObject();
        return builder;
    }
}

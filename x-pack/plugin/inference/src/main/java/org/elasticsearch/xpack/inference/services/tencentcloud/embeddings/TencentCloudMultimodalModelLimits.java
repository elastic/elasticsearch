/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceStringGroup;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Per-request limits of the TencentCloud multimodal embeddings API for a model, and the batching of inputs that respects them.
 *
 * @param maxTexts the maximum number of texts in one request
 * @param maxImages the maximum number of images in one request
 * @param maxVideos the maximum number of videos in one request
 * @param supportsPromptName whether the model applies {@code prompt_name}
 */
public record TencentCloudMultimodalModelLimits(int maxTexts, int maxImages, int maxVideos, boolean supportsPromptName) {

    public static final String EMBEDDING_GEMMA_2 = "EmbeddingGemma-2";

    private static final int MAX_IMAGES = 8;
    private static final int MAX_VIDEOS = 4;

    // The text limits keep the response below the API's 10MB cap for the model's dimensions.
    private static final Map<String, TencentCloudMultimodalModelLimits> KNOWN_MODELS = Map.of(
        "WeMM-Embedding-2B",
        new TencentCloudMultimodalModelLimits(120, MAX_IMAGES, MAX_VIDEOS, false),
        "WeMM-Embedding-9B",
        new TencentCloudMultimodalModelLimits(60, MAX_IMAGES, MAX_VIDEOS, false),
        EMBEDDING_GEMMA_2,
        new TencentCloudMultimodalModelLimits(200, MAX_IMAGES, MAX_VIDEOS, true)
    );

    private static final TencentCloudMultimodalModelLimits DEFAULT_LIMITS = new TencentCloudMultimodalModelLimits(
        60,
        MAX_IMAGES,
        MAX_VIDEOS,
        false
    );

    public static TencentCloudMultimodalModelLimits forModel(String modelId) {
        return KNOWN_MODELS.getOrDefault(modelId, DEFAULT_LIMITS);
    }

    /**
     * Splits the inputs into consecutive batches that each fit in one API request: every batch stays within the per-modality limits,
     * and does not mix base64 and URL inputs of the same modality, which the API rejects. Each group must hold a single text, image
     * or video.
     *
     * @param inputs the inputs in request order
     * @return the batches, which concatenated in order equal {@code inputs}
     */
    public List<List<InferenceStringGroup>> partition(List<InferenceStringGroup> inputs) {
        List<List<InferenceStringGroup>> batches = new ArrayList<>();
        List<InferenceStringGroup> batch = new ArrayList<>();
        Map<DataType, Integer> counts = new EnumMap<>(DataType.class);
        Map<DataType, DataFormat> formats = new EnumMap<>(DataType.class);

        for (var input : inputs) {
            var value = input.value();
            var type = value.dataType();
            var count = counts.getOrDefault(type, 0);
            var format = formats.get(type);
            if (count >= limit(type) || (format != null && format != value.dataFormat())) {
                batches.add(batch);
                batch = new ArrayList<>();
                counts.clear();
                formats.clear();
                count = 0;
            }
            batch.add(input);
            counts.put(type, count + 1);
            formats.put(type, value.dataFormat());
        }
        if (batch.isEmpty() == false) {
            batches.add(batch);
        }
        return batches;
    }

    private int limit(DataType type) {
        return switch (type) {
            case TEXT -> maxTexts;
            case IMAGE -> maxImages;
            case VIDEO -> maxVideos;
            default -> throw new IllegalArgumentException("Unsupported input type [" + type + "]");
        };
    }
}

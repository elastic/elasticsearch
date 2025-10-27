/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.inference.InputType;

public final class NvidiaUtils {
    public static final String HOST = "integrate.api.nvidia.com";
    public static final String VERSION_1 = "v1";

    public static final String EMBEDDINGS_PATH = "embeddings";

    public static final String CHAT_PATH = "chat";
    public static final String COMPLETIONS_PATH = "completions";

    public static final String RERANK_HOST = "ai.api.nvidia.com";
    public static final String RETRIEVAL_PATH = "retrieval";
    public static final String NVIDIA_PATH = "nvidia";
    public static final String RERANKING_PATH = "reranking";

    public static final String PASSAGE = "passage";
    public static final String QUERY = "query";

    public static String inputTypeToString(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> PASSAGE;
            case SEARCH, INTERNAL_SEARCH -> QUERY;
            case null, default -> null;
        };
    }

    public static final TransportVersion ML_INFERENCE_NVIDIA_ADDED = TransportVersion.fromName("ml_inference_nvidia_added");

    public static boolean supportsNvidia(TransportVersion version) {
        return version.supports(ML_INFERENCE_NVIDIA_ADDED);
    }

    private NvidiaUtils() {}

}

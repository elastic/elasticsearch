/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

/**
 * Utility class for Nvidia inference services.
 */
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

    private static final String PASSAGE = "passage";
    private static final String QUERY = "query";

    /**
     * Converts an {@link InputType} to its corresponding string representation for Nvidia services.
     *
     * @param inputType the InputType to convert
     * @return the string representation of the {@link InputType}
     */
    public static String inputTypeToString(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> PASSAGE;
            case SEARCH, INTERNAL_SEARCH -> QUERY;
            case null, default -> throw new IllegalArgumentException(
                Strings.format("Unrecognized input_type [%s], must be one of %s", inputType, NvidiaService.VALID_INPUT_TYPE_VALUES)
            );
        };
    }

    public static final TransportVersion ML_INFERENCE_NVIDIA_ADDED = TransportVersion.fromName("ml_inference_nvidia_added");

    /**
     * Check if the given transport version supports Nvidia inference services.
     *
     * @param version the transport version to check
     * @return true if the version supports Nvidia inference services, false otherwise
     */
    public static boolean supportsNvidia(TransportVersion version) {
        return version.supports(ML_INFERENCE_NVIDIA_ADDED);
    }

    /**
     * Determines if the given HTTP result indicates that the content is too large.
     *
     * @param result the HTTP result to check
     * @param contentTooLargeMessage a substring to search for in the error message indicating content is too large
     * @return true if the content is too large, false otherwise
     */
    public static boolean isContentTooLarge(HttpResult result, String contentTooLargeMessage) {
        int statusCode = result.response().getStatusLine().getStatusCode();
        if (statusCode == 413) {
            return true;
        }
        if (statusCode == 400) {
            var errorResponse = ErrorResponse.fromResponse(result);
            return errorResponse != null
                && errorResponse.getErrorMessage() != null
                && errorResponse.getErrorMessage().contains(contentTooLargeMessage);
        }
        return false;
    }

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private NvidiaUtils() {}

}

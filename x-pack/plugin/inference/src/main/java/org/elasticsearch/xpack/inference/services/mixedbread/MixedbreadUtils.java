/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

/**
 * Utility class for Mixedbread related version checks.
 */
public final class MixedbreadUtils {
    public static final String HOST = "api.mixedbread.com";
    public static final String VERSION_1 = "v1";

    public static final String EMBEDDINGS_PATH = "embeddings";

    public static final String RERANK_PATH = "reranking";

    // common service settings fields
    public static final String MODEL_FIELD = "model";
    public static final String INPUT_FIELD = "input";

    // rerank fields
    public static final String QUERY_FIELD = "query";
    public static final String DOCUMENTS_FIELD = "documents";
    public static final String RETURN_DOCUMENTS_FIELD = "return_input";
    public static final String TOP_K_FIELD = "top_k";

    // embeddings fields
    public static final String DIMENSIONS_FIELD = "dimensions";
    public static final String PROMPT_FIELD = "prompt";
    public static final String NORMALIZED_FIELD = "normalized";

    public static final String EMBEDDINGS_ERROR_PREFIX = "Embeddings rerank";
    public static final String RERANK_ERROR_PREFIX = "Mixedbread rerank";
    public static final String EMBEDDINGS_REQUEST_TYPE = "mixedbread text embedding";
    public static final String RERANK_REQUEST_TYPE = "mixedbread rerank";

    /**
     * TransportVersion indicating when Mixedbread features were added.
     */
    public static final TransportVersion INFERENCE_MIXEDBREAD_ADDED = TransportVersion.fromName("inference_mixedbread_added");

    /**
     * Checks if the given TransportVersion supports Mixedbread features.
     *
     * @param version the TransportVersion to check
     * @return true if Mixedbread features are supported, false otherwise
     */
    public static boolean supportsMixedbread(TransportVersion version) {
        return version.supports(INFERENCE_MIXEDBREAD_ADDED);
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
     * Private constructor to prevent instantiation.
     */
    private MixedbreadUtils() {}

}

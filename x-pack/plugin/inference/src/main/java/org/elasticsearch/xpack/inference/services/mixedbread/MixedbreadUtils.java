/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.TransportVersion;

/**
 * Utility class for Mixedbread related version checks.
 */
public final class MixedbreadUtils {
    public static final String HOST = "api.mixedbread.com";
    public static final String VERSION_1 = "v1";
    public static final String RERANK_PATH = "reranking";
    public static URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https").setHost(MixedbreadUtils.HOST);

    // common service settings fields
    public static final String MODEL_FIELD = "model";

    public static final String INPUT_FIELD = "input";

    // rerank task settings fields
    public static final String QUERY_FIELD = "query";

    public static final String DOCUMENTS_FIELD = "documents";

    // rerank task settings fields
    public static final String RETURN_DOCUMENTS_FIELD = "return_input";
    public static final String TOP_K_FIELD = "top_k";

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
     * Private constructor to prevent instantiation.
     */
    private MixedbreadUtils() {}

}

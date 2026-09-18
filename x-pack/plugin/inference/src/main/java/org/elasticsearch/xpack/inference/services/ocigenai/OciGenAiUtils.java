/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Constants and URL helpers for the OCI Generative AI inference API.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/">OCI Generative AI Inference API</a>
 */
public final class OciGenAiUtils {

    public static final TransportVersion ML_INFERENCE_OCI_GENAI_ADDED = TransportVersion.fromName("ml_inference_oci_genai_added");

    /** Host of the regional public OCI Generative AI inference endpoint; the placeholder is the region identifier. */
    public static final String HOST_TEMPLATE = "inference.generativeai.%s.oci.oraclecloud.com";
    public static final String API_VERSION = "20231130";
    public static final String ACTIONS = "actions";
    public static final String EMBED_TEXT = "embedText";
    public static final String CHAT = "chat";
    public static final String RERANK_TEXT = "rerankText";

    /**
     * Builds the URL of an OCI Generative AI inference action.
     *
     * @param baseUri an optional base URL overriding the public regional endpoint (for example a private endpoint or a different
     *                OCI realm). The action path is appended to it.
     * @param region  the OCI region identifier, used to derive the public endpoint host when no base URL is provided
     * @param action  the action name, one of {@link #EMBED_TEXT}, {@link #CHAT} or {@link #RERANK_TEXT}
     */
    public static URI buildUri(@Nullable URI baseUri, @Nullable String region, String action) {
        try {
            if (baseUri != null) {
                var basePath = baseUri.getRawPath();
                if (basePath == null || basePath.isEmpty() || basePath.equals("/")) {
                    basePath = "";
                } else if (basePath.endsWith("/")) {
                    basePath = basePath.substring(0, basePath.length() - 1);
                }
                return new URIBuilder(baseUri).setPath(basePath + "/" + API_VERSION + "/" + ACTIONS + "/" + action).build();
            }

            return new URIBuilder().setScheme("https")
                .setHost(Strings.format(HOST_TEMPLATE, region))
                .setPathSegments(API_VERSION, ACTIONS, action)
                .build();
        } catch (URISyntaxException e) {
            // using bad request here so that potentially sensitive URL information does not get logged
            throw new ElasticsearchStatusException("Failed to construct OCI Generative AI URL", RestStatus.BAD_REQUEST, e);
        }
    }

    private OciGenAiUtils() {}
}

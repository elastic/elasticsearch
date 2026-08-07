/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public final class TencentCloudUtils {

    public static final String SCHEME = "https";
    public static final String DEFAULT_REGION = "bj";
    private static final String HOST_SUFFIX = ".aisearch.tencentelasticsearch.com";
    public static final String VERSION_1 = "v1";
    public static final String EMBEDDINGS_PATH = "embeddings";
    public static final String CHAT_COMPLETIONS_PATH_1 = "chat";
    public static final String CHAT_COMPLETIONS_PATH_2 = "completions";
    public static final String RERANK_PATH = "rerank";

    private TencentCloudUtils() {}

    public static String buildHost(String region) {
        return region + HOST_SUFFIX;
    }

    public static URI buildUri(String region, String... segments) {
        var builder = new URIBuilder().setScheme(SCHEME).setHost(buildHost(region)).setPathSegments(List.of(segments));
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to build TencentCloud URI for region [" + region + "]", e);
        }
    }

    public static void decorateWithAuthHeader(HttpPost request, SecureString apiKey) {
        request.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        request.setHeader(createAuthBearerHeader(apiKey));
    }
}

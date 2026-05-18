/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;

public class IbmWatsonxRerankModelTests extends ESTestCase {
    public static IbmWatsonxRerankModel createModel(
        String modelId,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        String url,
        String authHeaderValue
    ) {
        return createModel(modelId, projectId, uri, apiVersion, apiKey, url, authHeaderValue, null, null, null);
    }

    public static IbmWatsonxRerankModel createModel(
        String modelId,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        String url,
        String authHeaderValue,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        @Nullable Integer truncateInputTokens
    ) {
        return new IbmWatsonxRerankModel(
            "id",
            TaskType.RERANK,
            "service",
            url,
            new IbmWatsonxRerankServiceSettings(uri, apiVersion, modelId, projectId, null),
            new IbmWatsonxRerankTaskSettings(topN, returnDocuments, truncateInputTokens),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            (httpPost, model) -> httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeaderValue)
        );
    }
}

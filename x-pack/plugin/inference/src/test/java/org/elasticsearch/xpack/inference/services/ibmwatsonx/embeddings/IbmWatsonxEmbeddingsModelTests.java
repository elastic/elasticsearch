/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;

public class IbmWatsonxEmbeddingsModelTests extends ESTestCase {
    public static IbmWatsonxEmbeddingsModel createModel(
        String model,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        String url
    ) {
        return createModel(model, projectId, uri, apiVersion, apiKey, null, null, SimilarityMeasure.DOT_PRODUCT, url, "foo");
    }

    public static IbmWatsonxEmbeddingsModel createModel(
        String url,
        String modelId,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        Integer dimensions,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new IbmWatsonxEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new IbmWatsonxEmbeddingsServiceSettings(modelId, projectId, uri, apiVersion, null, dimensions, similarityMeasure, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            (httpPost, model) -> httpPost.setHeader(HttpHeaders.AUTHORIZATION, "foo")
        );
    }

    public static IbmWatsonxEmbeddingsModel createModel(
        String modelId,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable String url,
        String authHeaderValue
    ) {
        return new IbmWatsonxEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new IbmWatsonxEmbeddingsServiceSettings(modelId, projectId, uri, apiVersion, tokenLimit, dimensions, similarityMeasure, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            (httpPost, model) -> httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeaderValue)
        );
    }
}

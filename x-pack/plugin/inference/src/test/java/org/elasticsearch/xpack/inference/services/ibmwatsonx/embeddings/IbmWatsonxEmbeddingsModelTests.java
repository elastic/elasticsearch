/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class IbmWatsonxEmbeddingsModelTests extends ESTestCase {
    public static IbmWatsonxEmbeddingsModel createModel(
        String model,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        String url
    ) {
        return new IbmWatsonxEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new IbmWatsonxEmbeddingsServiceSettings(model, projectId, uri, apiVersion, null, null, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static IbmWatsonxEmbeddingsModel createModel(
        String url,
        String model,
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
            new IbmWatsonxEmbeddingsServiceSettings(model, projectId, uri, apiVersion, null, dimensions, similarityMeasure, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static IbmWatsonxEmbeddingsModel createModel(
        String model,
        String projectId,
        URI uri,
        String apiVersion,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions
    ) {
        return new IbmWatsonxEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new IbmWatsonxEmbeddingsServiceSettings(
                model,
                projectId,
                uri,
                apiVersion,
                tokenLimit,
                dimensions,
                SimilarityMeasure.DOT_PRODUCT,
                null
            ),
            EmptyTaskSettings.INSTANCE,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testThrowsError_WhenInputTypeSpecified() throws URISyntaxException {
        var model = createModel("url", "projectId", new URI("http"), "api_version", "api_key", "url");

        var thrownException = expectThrows(ValidationException.class, () -> IbmWatsonxEmbeddingsModel.of(model, Map.of(), InputType.SEARCH));
        assertThat(
            thrownException.getMessage(),
            CoreMatchers.is("Validation Failed: 1: Invalid value [search] received. [input_type] is not allowed;")
        );
    }

    public void testAcceptsInternalInputType() throws URISyntaxException {
        var model = createModel("url", "projectId", new URI("http"), "api_version", "api_key", "url");
        var overriddenModel = IbmWatsonxEmbeddingsModel.of(model, Map.of(), InputType.INTERNAL_SEARCH);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public void testAcceptsNullInputType() throws URISyntaxException {
        var model = createModel("url", "projectId", new URI("http"), "api_version", "api_key", "url");
        var overriddenModel = IbmWatsonxEmbeddingsModel.of(model, Map.of(), null);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }
}

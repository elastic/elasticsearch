/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.is;

public class GoogleVertexAiEmbeddingsModelTests extends ESTestCase {

    public void testBuildUri() throws URISyntaxException {
        var location = "location";
        var projectId = "project";
        var modelId = "model";

        URI uri = GoogleVertexAiEmbeddingsModel.buildUri(location, projectId, modelId);

        assertThat(
            uri,
            is(
                new URI(
                    Strings.format(
                        "https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict",
                        location,
                        projectId,
                        location,
                        modelId
                    )
                )
            )
        );
    }

    public static GoogleVertexAiEmbeddingsModel createModel(
        String location,
        String projectId,
        String modelId,
        String uri,
        String serviceAccountJson
    ) {
        return new GoogleVertexAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            uri,
            new GoogleVertexAiEmbeddingsServiceSettings(location, projectId, modelId, false, null, null, null, null),
            new GoogleVertexAiEmbeddingsTaskSettings(Boolean.FALSE),
            new GoogleVertexAiSecretSettings(new SecureString(serviceAccountJson.toCharArray()))
        );
    }

    public static GoogleVertexAiEmbeddingsModel createModel(String modelId, @Nullable Boolean autoTruncate) {
        return new GoogleVertexAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleVertexAiEmbeddingsServiceSettings(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                modelId,
                false,
                null,
                null,
                SimilarityMeasure.DOT_PRODUCT,
                null
            ),
            new GoogleVertexAiEmbeddingsTaskSettings(autoTruncate),
            null,
            new GoogleVertexAiSecretSettings(new SecureString(randomAlphaOfLength(8).toCharArray()))
        );
    }
}

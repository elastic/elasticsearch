/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.is;

public class GoogleVertexAiRerankModelTests extends ESTestCase {

    public void testBuildUri() throws URISyntaxException {
        var projectId = "project";

        URI uri = GoogleVertexAiRerankModel.buildUri(projectId);

        assertThat(
            uri,
            is(
                new URI(
                    Strings.format(
                        "https://discoveryengine.googleapis.com/v1/projects/%s/locations/global/rankingConfigs/default_ranking_config:rank",
                        projectId
                    )
                )
            )
        );
    }

    public static GoogleVertexAiRerankModel createModel(@Nullable String modelId, @Nullable Integer topN) {
        return new GoogleVertexAiRerankModel(
            "id",
            TaskType.RERANK,
            "service",
            new GoogleVertexAiRerankServiceSettings(randomAlphaOfLength(10), modelId, null),
            new GoogleVertexAiRerankTaskSettings(topN),
            new GoogleVertexAiSecretSettings(randomSecureStringOfLength(8))
        );
    }

    public static GoogleVertexAiRerankModel createModel(String url, @Nullable String modelId, @Nullable Integer topN) {
        return new GoogleVertexAiRerankModel(
            "id",
            TaskType.RERANK,
            "service",
            url,
            new GoogleVertexAiRerankServiceSettings(randomAlphaOfLength(10), modelId, null),
            new GoogleVertexAiRerankTaskSettings(topN),
            new GoogleVertexAiSecretSettings(randomSecureStringOfLength(8))
        );
    }

}

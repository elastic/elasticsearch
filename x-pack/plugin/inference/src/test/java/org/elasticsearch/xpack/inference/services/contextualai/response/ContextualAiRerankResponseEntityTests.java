/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequest;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_QUERY;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ContextualAiRerankResponseEntityTests extends ESTestCase {

    public void testFromResponse_SortsByRelevanceScoreDescending() throws IOException {
        String responseJson = """
            {
                "results": [
                    {"index": 0, "relevance_score": 0.1},
                    {"index": 1, "relevance_score": 0.9},
                    {"index": 2, "relevance_score": 0.5}
                ]
            }
            """;

        var request = createRequest(null);
        RankedDocsResults parsed = ContextualAiRerankResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsed.getRankedDocs(),
            is(
                List.of(
                    new RankedDocsResults.RankedDoc(1, 0.9F, null),
                    new RankedDocsResults.RankedDoc(2, 0.5F, null),
                    new RankedDocsResults.RankedDoc(0, 0.1F, null)
                )
            )
        );
    }

    public void testFromResponse_TopNLimitsResultsAfterSort() throws IOException {
        String responseJson = """
            {
                "results": [
                    {"index": 0, "relevance_score": 0.1},
                    {"index": 1, "relevance_score": 0.9},
                    {"index": 2, "relevance_score": 0.5}
                ]
            }
            """;

        int topN = 2;
        var request = createRequest(topN);
        RankedDocsResults parsed = ContextualAiRerankResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsed.getRankedDocs(),
            is(List.of(new RankedDocsResults.RankedDoc(1, 0.9F, null), new RankedDocsResults.RankedDoc(2, 0.5F, null)))
        );
    }

    private static ContextualAiRerankRequest createRequest(Integer requestTopN) {
        var model = new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(TEST_MODEL_ID, new RateLimitSettings(1000L))
            ),
            new ContextualAiRerankTaskSettings(null, null, null),
            new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
        );
        return new ContextualAiRerankRequest(TEST_QUERY, TEST_DOCUMENTS, requestTopN, null, model);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.alibabacloudsearch;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AlibabaCloudSearchEmbeddingsResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForASingleItem() throws IOException, URISyntaxException {
        String responseJson = """
            {
                "request_id": "B4AB89C8-B135-xxxx-A6F8-2BAB801A2CE4",
                "latency": 38,
                "usage": {
                    "token_count": 3072
                },
                "result": {
                    "embeddings": [
                        {
                            "index": 0,
                            "embedding": [
                                -0.02868066355586052,
                                0.022033605724573135
                            ]
                        }
                    ]
                }
            }
            """;

        AlibabaCloudSearchRequest request = mock(AlibabaCloudSearchRequest.class);
        URI uri = new URI("mock_uri");
        when(request.getURI()).thenReturn(uri);

        InferenceTextEmbeddingFloatResults parsedResults = AlibabaCloudSearchEmbeddingsResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(
                        new float[] { -0.02868066355586052f, 0.022033605724573135f }
                    )
                )
            )
        );
    }
}

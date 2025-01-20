/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.alibabacloudsearch;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
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

public class AlibabaCloudSearchSparseResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForASingleItem() throws IOException, URISyntaxException {
        String responseJson = """
            {
              "request_id": "DDC4306F-xxxx-xxxx-xxxx-92C5CEA756A0",
              "latency": 25,
              "usage": {
                "token_count": 11
              },
              "result": {
                "sparse_embeddings": [
                  {
                    "index": 0,
                    "embedding": [
                      {
                        "token_id": 6,
                        "weight": 0.1014404296875
                      },
                      {
                        "token_id": 163040,
                        "weight": 0.2841796875
                      },
                      {
                        "token_id": 354,
                        "weight": 0.1431884765625
                      }
                    ]
                  }
                ]
              }
            }
            """;

        AlibabaCloudSearchRequest request = mock(AlibabaCloudSearchRequest.class);
        URI uri = new URI("mock_uri");
        when(request.getURI()).thenReturn(uri);

        SparseEmbeddingResults parsedResults = AlibabaCloudSearchSparseResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new SparseEmbeddingResults.Embedding(
                        List.of(
                            new WeightedToken("6", 0.1014404296875f),
                            new WeightedToken("163040", 0.2841796875f),
                            new WeightedToken("354", 0.1431884765625f)
                        ),
                        false
                    )
                )
            )
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AlibabaCloudSearchCompletionResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResponseEntityForText() throws IOException, URISyntaxException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-****-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "result": {
                "text":"result"
              }
              "usage": {
                  "output_tokens": 6320,
                  "input_tokens": 35,
                  "total_tokens": 6355,
              }
            }
            """;

        AlibabaCloudSearchRequest request = mock(AlibabaCloudSearchRequest.class);
        URI uri = new URI("mock_uri");
        when(request.getURI()).thenReturn(uri);

        ChatCompletionResults chatCompletionResults = AlibabaCloudSearchCompletionResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
    }
}

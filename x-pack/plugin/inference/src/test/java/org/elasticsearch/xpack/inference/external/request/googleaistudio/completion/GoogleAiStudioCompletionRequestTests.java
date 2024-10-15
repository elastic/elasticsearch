/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googleaistudio.completion;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioCompletionRequest;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class GoogleAiStudioCompletionRequestTests extends ESTestCase {

    public void testCreateRequest() throws IOException {
        var apiKey = "api_key";
        var input = "input";

        var request = new GoogleAiStudioCompletionRequest(listOf(input), GoogleAiStudioCompletionModelTests.createModel("model", apiKey));

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), endsWith(Strings.format("%s=%s", "key", apiKey)));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(
            requestMap,
            is(
                Map.of(
                    "contents",
                    List.of(Map.of("role", "user", "parts", List.of(Map.of("text", input)))),
                    "generationConfig",
                    Map.of("candidateCount", 1)
                )
            )
        );
    }

    public void testTruncate_ReturnsSameInstance() {
        var request = new GoogleAiStudioCompletionRequest(
            listOf("input"),
            GoogleAiStudioCompletionModelTests.createModel("model", "api key")
        );
        var truncatedRequest = request.truncate();

        assertThat(truncatedRequest, sameInstance(request));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = new GoogleAiStudioCompletionRequest(
            listOf("input"),
            GoogleAiStudioCompletionModelTests.createModel("model", "api key")
        );

        assertNull(request.getTruncationInfo());
    }

    private static DocumentsOnlyInput listOf(String... input) {
        return new DocumentsOnlyInput(List.of(input));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HuggingFaceElserResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesTextExpansionResults() throws IOException {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    [
                      ".",
                      0.133155956864357
                    ],
                    [
                      "the",
                      0.6747211217880249
                    ]
                  ]
                ]
              }
            ]
            """;
        TextExpansionResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        Map<String, Float> tokenWeightMap = parsedResults.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        assertThat(tokenWeightMap.get("."), is(0.13315596f));
        assertThat(tokenWeightMap.get("the"), is(0.67472112f));
    }
}

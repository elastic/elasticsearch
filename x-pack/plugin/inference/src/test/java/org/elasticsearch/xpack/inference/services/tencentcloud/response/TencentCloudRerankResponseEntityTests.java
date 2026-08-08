/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TencentCloudRerankResponseEntityTests extends ESTestCase {

    public void testFromResponse_ParsesResultsWithStringDocument() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "results": [
                {"index": 0, "relevance_score": 0.9856, "document": "Artificial intelligence is a branch of computer science"},
                {"index": 2, "relevance_score": 0.8234, "document": "Machine learning is the core technology of AI"},
                {"index": 1, "relevance_score": 0.0123, "document": "The weather is nice today"}
              ],
              "model": "bge-reranker-v2-m3",
              "usage": {"total_tokens": 45}
            }
            """;

        InferenceServiceResults parsed = TencentCloudRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsed, instanceOf(RankedDocsResults.class));
        List<RankedDocsResults.RankedDoc> docs = ((RankedDocsResults) parsed).getRankedDocs();
        assertThat(docs.size(), is(3));
        assertThat(docs.get(0).index(), is(0));
        assertEquals(0.9856f, docs.get(0).relevanceScore(), 0.0001f);
        assertThat(docs.get(0).text(), is("Artificial intelligence is a branch of computer science"));
        assertThat(docs.get(2).index(), is(1));
    }

    public void testFromResponse_HandlesResultsWithoutDocument() throws IOException {
        String responseJson = """
            {
              "object": "list",
              "results": [
                {"index": 1, "relevance_score": 0.98},
                {"index": 0, "relevance_score": 0.10}
              ]
            }
            """;

        InferenceServiceResults parsed = TencentCloudRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        List<RankedDocsResults.RankedDoc> docs = ((RankedDocsResults) parsed).getRankedDocs();
        assertThat(docs.size(), is(2));
        assertThat(docs.get(0).index(), is(1));
        assertNull(docs.get(0).text());
    }

    public void testFromResponse_HandlesObjectDocument() throws IOException {
        String responseJson = """
            {
              "results": [
                {"index": 0, "relevance_score": 0.7, "document": {"text": "hello world"}}
              ]
            }
            """;

        InferenceServiceResults parsed = TencentCloudRerankResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        RankedDocsResults.RankedDoc doc = ((RankedDocsResults) parsed).getRankedDocs().get(0);
        assertThat(doc.text(), is("hello world"));
    }
}

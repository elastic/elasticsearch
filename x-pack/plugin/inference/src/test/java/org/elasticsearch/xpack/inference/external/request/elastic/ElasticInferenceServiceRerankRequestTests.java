/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.elastic.rerank.ElasticInferenceServiceRerankRequest;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModelTests;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceRerankRequestTests extends ESTestCase {

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var url = "http://eis-gateway.com";
        var query = "query";
        var documents = List.of("document 1", "document 2", "document 3");
        var modelId = "my-model-id";
        var topN = 3;

        var request = createRequest(url, modelId, query, documents, topN);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var traceParent = request.getTraceContext().traceParent();
        var traceState = request.getTraceContext().traceState();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    public void testTruncate_DoesNotTruncate() throws IOException {
        var url = "http://eis-gateway.com";
        var query = "query";
        var documents = List.of("document 1", "document 2", "document 3");
        var modelId = "my-model-id";
        var topN = 3;

        var request = createRequest(url, modelId, query, documents, topN);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(4));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("documents"), is(documents));
        assertThat(requestMap.get("top_n"), is(topN));
    }

    private ElasticInferenceServiceRerankRequest createRequest(
        String url,
        String modelId,
        String query,
        List<String> documents,
        Integer topN
    ) {
        var rerankModel = ElasticInferenceServiceRerankModelTests.createModel(url, modelId);

        return new ElasticInferenceServiceRerankRequest(
            query,
            documents,
            topN,
            rerankModel,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata()
        );
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ocigenai.embeddings.OciGenAiEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.ENDPOINT_ID_VALUE;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OciGenAiEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_OnDemand_WithRequestInputType() throws IOException {
        var model = OciGenAiEmbeddingsModelTests.createModel(null, "cohere.embed-v4.0", null, null, InputType.SEARCH, Truncation.END);
        var request = createRequest(model, List.of("abc"), InputType.INGEST);

        var httpPost = getHttpPost(request);

        assertThat(httpPost.getURI(), is(model.uri()));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(OciGenAiTestUtils.AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(5));
        assertThat(requestMap.get("inputs"), is(List.of("abc")));
        assertThat(requestMap.get("compartmentId"), is(COMPARTMENT_ID));
        assertThat(requestMap.get("servingMode"), is(Map.of("servingType", "ON_DEMAND", "modelId", "cohere.embed-v4.0")));
        // the request input type wins over the task settings input type
        assertThat(requestMap.get("inputType"), is("SEARCH_DOCUMENT"));
        assertThat(requestMap.get("truncate"), is("END"));
    }

    public void testCreateRequest_FallsBackToTaskSettingsInputType_AndWritesOutputDimensions() throws IOException {
        var model = OciGenAiEmbeddingsModelTests.createModel(null, "cohere.embed-v4.0", 512, null, InputType.CLUSTERING, null);
        var request = createRequest(model, List.of("abc"), InputType.UNSPECIFIED);

        var requestMap = entityAsMap(getHttpPost(request).getEntity().getContent());

        assertThat(requestMap.get("inputType"), is("CLUSTERING"));
        assertThat(requestMap.get("outputDimensions"), is(512));
        assertFalse(requestMap.containsKey("truncate"));
    }

    public void testCreateRequest_DoesNotWriteDimensionsDiscoveredFromTheModel() throws IOException {
        var model = OciGenAiEmbeddingsModelTests.createModel(null, "cohere.embed-v4.0", 1536, false, null, null, null, null, null);
        var request = createRequest(model, List.of("abc"), null);

        var requestMap = entityAsMap(getHttpPost(request).getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertFalse(requestMap.containsKey("outputDimensions"));
        assertFalse(requestMap.containsKey("inputType"));
    }

    public void testCreateRequest_Dedicated() throws IOException {
        var model = OciGenAiEmbeddingsModelTests.createModel(
            null,
            "cohere.embed-v4.0",
            null,
            false,
            null,
            null,
            null,
            ENDPOINT_ID_VALUE,
            null
        );
        var request = createRequest(model, List.of("abc"), InputType.INTERNAL_SEARCH);

        var requestMap = entityAsMap(getHttpPost(request).getEntity().getContent());

        assertThat(requestMap.get("servingMode"), is(Map.of("servingType", "DEDICATED", "endpointId", ENDPOINT_ID_VALUE)));
        assertThat(requestMap.get("inputType"), is("SEARCH_QUERY"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var model = OciGenAiEmbeddingsModelTests.createModel(null, "cohere.embed-v4.0", null, null, null, null);
        var request = createRequest(model, List.of("abcd"), null);

        var truncatedRequest = request.truncate();
        var requestMap = entityAsMap(getHttpPost(truncatedRequest).getEntity().getContent());

        assertThat(requestMap.get("inputs"), is(List.of("ab")));
        assertFalse(request.getTruncationInfo()[0]);
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    private static HttpPost getHttpPost(org.elasticsearch.xpack.inference.external.request.OutboundRequest request) {
        var httpRequest = RequestTests.getHttpRequestSync(request);
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        return (HttpPost) httpRequest.httpRequestBase();
    }

    public static OciGenAiEmbeddingsRequest createRequest(OciGenAiEmbeddingsModel model, List<String> inputs, InputType inputType) {
        return new OciGenAiEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(inputs, new boolean[inputs.size()]),
            inputType,
            model
        );
    }
}

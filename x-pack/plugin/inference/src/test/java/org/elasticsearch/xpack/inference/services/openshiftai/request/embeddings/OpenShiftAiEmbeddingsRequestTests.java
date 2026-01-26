/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OpenShiftAiEmbeddingsRequestTests extends ESTestCase {

    private static final String INPUT_FIELD_NAME = "input";
    private static final String MODEL_FIELD_NAME = "model";

    private static final String MODEL_VALUE = "some_model";
    private static final String INPUT_VALUE = "ABCD";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final int DIMENSIONS_VALUE = 384;

    public void testCreateRequest_NoDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_Success(null, false, null);
    }

    public void testCreateRequest_NoDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_Success(null, true, null);
    }

    public void testCreateRequest_WithDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_Success(DIMENSIONS_VALUE, false, null);
    }

    public void testCreateRequest_WithDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_Success(DIMENSIONS_VALUE, true, DIMENSIONS_VALUE);
    }

    private void testCreateRequest_Success(Integer dimensions, boolean dimensionsSetByUser, Integer expectedDimensions) throws IOException {
        var request = createRequest(dimensions, dimensionsSetByUser);
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        assertThat(requestMap.get(ServiceFields.DIMENSIONS), is(expectedDimensions));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

    public void testCreateRequest_NoModel_Success() throws IOException {
        var request = createRequest(null, false, null);
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(nullValue()));
        assertThat(requestMap.get(ServiceFields.DIMENSIONS), is(nullValue()));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));

    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest(null, false);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_VALUE.substring(0, INPUT_VALUE.length() / 2))));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));

    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest(null, false);
        assertThat(request.getTruncationInfo()[0], is(false));

        var truncatedRequest = request.truncate();
        assertThat(truncatedRequest.getTruncationInfo()[0], is(true));
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(URL_VALUE));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        return httpPost;
    }

    private static OpenShiftAiEmbeddingsRequest createRequest(Integer dimensions, Boolean dimensionsSetByUser) {
        return createRequest(dimensions, dimensionsSetByUser, MODEL_VALUE);
    }

    private static OpenShiftAiEmbeddingsRequest createRequest(Integer dimensions, Boolean dimensionsSetByUser, String modelId) {
        var embeddingsModel = OpenShiftAiEmbeddingsModelTests.createModel(
            URL_VALUE,
            API_KEY_VALUE,
            modelId,
            null,
            dimensionsSetByUser,
            dimensions,
            null
        );
        return new OpenShiftAiEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(INPUT_VALUE), new boolean[] { false }),
            embeddingsModel
        );
    }

}

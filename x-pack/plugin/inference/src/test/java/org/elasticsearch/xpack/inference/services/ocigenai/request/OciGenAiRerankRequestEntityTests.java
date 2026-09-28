/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.ocigenai.rerank.OciGenAiRerankModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

public class OciGenAiRerankRequestEntityTests extends ESTestCase {

    public void testToXContent_WritesAllFields() throws IOException {
        var model = OciGenAiRerankModelTests.createModel(null, "cohere.rerank-v3.5", 2, true);
        var entity = new OciGenAiRerankRequestEntity("query", List.of("doc one", "doc two"), model.getServiceSettings(), 2, true);

        var requestMap = entityAsMap(Strings.toString(entity));

        assertThat(requestMap, aMapWithSize(6));
        assertThat(requestMap.get("input"), is("query"));
        assertThat(requestMap.get("documents"), is(List.of("doc one", "doc two")));
        assertThat(requestMap.get("compartmentId"), is(COMPARTMENT_ID));
        assertThat(requestMap.get("servingMode"), is(Map.of("servingType", "ON_DEMAND", "modelId", "cohere.rerank-v3.5")));
        assertThat(requestMap.get("topN"), is(2));
        assertThat(requestMap.get("isEcho"), is(true));
    }

    public void testRequest_PrefersRequestLevelTopNAndReturnDocumentsOverTaskSettings() throws IOException {
        var model = OciGenAiRerankModelTests.createModel(null, "cohere.rerank-v3.5", 2, true);
        var request = new OciGenAiRerankRequest("query", List.of("doc"), 7, false, model);

        var httpPost = (HttpPost) RequestTests.getHttpRequestSync(request).httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get("topN"), is(7));
        assertThat(requestMap.get("isEcho"), is(false));
    }

    public void testRequest_FallsBackToTaskSettings() throws IOException {
        var model = OciGenAiRerankModelTests.createModel(null, "cohere.rerank-v3.5", 2, null);
        var request = new OciGenAiRerankRequest("query", List.of("doc"), null, null, model);

        var httpPost = (HttpPost) RequestTests.getHttpRequestSync(request).httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get("topN"), is(2));
        assertFalse(requestMap.containsKey("isEcho"));
    }
}

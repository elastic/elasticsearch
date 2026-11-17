/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests;

import static org.hamcrest.Matchers.is;

public class VoyageAIRequestTests extends ESTestCase {

    public void testDecorateWithHeaders() {
        var request = SimpleRequestBuilder.post("http://www.abc.com").build();
        var model = VoyageAIEmbeddingsModelTests.createModel("abc", "key", null, "model_id");

        VoyageAIRequestUtils.decorateWithHeaders(request, model);

        assertThat(request.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer key"));
        assertThat(request.getFirstHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE));
    }

}

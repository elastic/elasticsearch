/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestInferenceActionTests extends RestActionTestCase {
    @Before
    public void setUpAction() {
        controller().registerHandler(new RestInferenceAction());
    }

    public void test() {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(InferenceAction.Request.class));

            var request = (InferenceAction.Request) actionRequest;
            assertThat(request.getInferenceTimeout(), is(InferenceAction.Request.DEFAULT_TIMEOUT));

            executeCalled.set(true);
            return new InferenceAction.Response(
                new TextEmbeddingByteResults(List.of(new TextEmbeddingByteResults.Embedding(List.of((byte) -1))))
            );
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_inference/test")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }
}

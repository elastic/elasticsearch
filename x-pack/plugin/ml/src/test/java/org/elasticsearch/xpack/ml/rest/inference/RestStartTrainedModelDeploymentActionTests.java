/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestStartTrainedModelDeploymentActionTests extends RestActionTestCase {

    public void testCacheDisabled() {
        final boolean disableInferenceProcessCache = true;
        controller().registerHandler(new RestStartTrainedModelDeploymentAction(disableInferenceProcessCache));
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(StartTrainedModelDeploymentAction.Request.class));

            var request = (StartTrainedModelDeploymentAction.Request) actionRequest;
            assertThat(request.getCacheSize(), is(ByteSizeValue.ZERO));

            executeCalled.set(true);
            return createResponse();
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_ml/trained_models/test_id/deployment/_start")
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testCacheEnabled() {
        final boolean disableInferenceProcessCache = false;
        controller().registerHandler(new RestStartTrainedModelDeploymentAction(disableInferenceProcessCache));
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(StartTrainedModelDeploymentAction.Request.class));

            var request = (StartTrainedModelDeploymentAction.Request) actionRequest;
            assertNull(request.getCacheSize());

            executeCalled.set(true);
            return createResponse();
        }));

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_ml/trained_models/test_id/deployment/_start")
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testExceptionFromDifferentParamsInQueryAndBody() throws IOException {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        controller().registerHandler(new RestStartTrainedModelDeploymentAction(false));
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(StartTrainedModelDeploymentAction.Request.class));
            executeCalled.set(true);
            return createResponse();
        }));

        Map<String, String> paramsMap = new HashMap<>(1);
        paramsMap.put("cache_size", "1mb");
        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_ml/trained_models/test_id/deployment/_start")
            .withParams(paramsMap)
            .withContent(
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("cache_size", "2mb").endObject()),
                XContentType.JSON
            )
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(null)); // the duplicate parameter should cause an exception, but the exception isn't
                                                        // visible here, so we just check that the request failed
    }

    private static CreateTrainedModelAssignmentAction.Response createResponse() {
        return new CreateTrainedModelAssignmentAction.Response(TrainedModelAssignmentTests.randomInstance());
    }
}

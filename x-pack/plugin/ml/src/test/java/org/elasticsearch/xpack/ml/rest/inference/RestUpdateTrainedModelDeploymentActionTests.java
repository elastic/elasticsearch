/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;

import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestUpdateTrainedModelDeploymentActionTests extends RestActionTestCase {
    public void testNumberOfAllocationInParam() {
        controller().registerHandler(new RestUpdateTrainedModelDeploymentAction());
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(UpdateTrainedModelDeploymentAction.Request.class));

            var request = (UpdateTrainedModelDeploymentAction.Request) actionRequest;
            assertEquals(request.getNumberOfAllocations().intValue(), 5);

            executeCalled.set(true);
            return mock(CreateTrainedModelAssignmentAction.Response.class);
        }));
        var params = new HashMap<String, String>();
        params.put("number_of_allocations", "5");

        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_ml/trained_models/test_id/deployment/_update")
            .withParams(params)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testNumberOfAllocationInBody() {
        controller().registerHandler(new RestUpdateTrainedModelDeploymentAction());
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(UpdateTrainedModelDeploymentAction.Request.class));

            var request = (UpdateTrainedModelDeploymentAction.Request) actionRequest;
            assertEquals(request.getNumberOfAllocations().intValue(), 6);

            executeCalled.set(true);
            return mock(CreateTrainedModelAssignmentAction.Response.class);
        }));

        final String content = """
            {"number_of_allocations": 6}
            """;
        RestRequest inferenceRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("_ml/trained_models/test_id/deployment/_update")
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        dispatchRequest(inferenceRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentTests;

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

    private static CreateTrainedModelAssignmentAction.Response createResponse() {
        return new CreateTrainedModelAssignmentAction.Response(TrainedModelAssignmentTests.randomInstance());
    }
}

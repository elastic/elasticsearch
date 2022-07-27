/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;

public class RestPutPipelineActionTests extends RestActionTestCase {

    private RestPutPipelineAction action;

    @Before
    public void setUpAction() {
        action = new RestPutPipelineAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(AcknowledgedResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(AcknowledgedResponse.class));
    }

    public void testInvalidIfVersionValue() {
        Map<String, String> params = new HashMap<>();
        final String invalidValue = randomAlphaOfLength(5);
        params.put("if_version", invalidValue);
        params.put("id", "my_pipeline");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_ingest/pipeline/my_pipeline")
            .withContent(new BytesArray("{\"processors\":{}}"), XContentType.JSON)
            .withParams(params)
            .build();

        Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertEquals("invalid value [" + invalidValue + "] specified for [if_version]. must be an integer value", ex.getMessage());
    }

    public void testMissingIfVersionValue() {
        Map<String, String> params = new HashMap<>();
        params.put("id", "my_pipeline");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_ingest/pipeline/my_pipeline")
            .withContent(new BytesArray("{\"processors\":{}}"), XContentType.JSON)
            .withParams(params)
            .build();

        var verifier = new BiFunction<ActionType<AcknowledgedResponse>, ActionRequest, AcknowledgedResponse>() {
            boolean wasInvoked = false;

            @Override
            public AcknowledgedResponse apply(ActionType<AcknowledgedResponse> actionType, ActionRequest actionRequest) {
                wasInvoked = true;
                assertThat(actionRequest.getClass(), equalTo(PutPipelineRequest.class));
                PutPipelineRequest req = (PutPipelineRequest) actionRequest;
                assertThat(req.getId(), equalTo("my_pipeline"));
                return null;
            }

            public boolean wasInvoked() {
                return wasInvoked;
            }
        };
        verifyingClient.setExecuteVerifier(verifier);

        dispatchRequest(request);
        assertThat(verifier.wasInvoked(), equalTo(true));
    }

    public void testNumericIfVersionValue() {
        Map<String, String> params = new HashMap<>();
        final int numericValue = randomInt();
        params.put("id", "my_pipeline");
        params.put("if_version", Integer.toString(numericValue));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_ingest/pipeline/my_pipeline")
            .withContent(new BytesArray("{\"processors\":{}}"), XContentType.JSON)
            .withParams(params)
            .build();

        var verifier = new BiFunction<ActionType<AcknowledgedResponse>, ActionRequest, AcknowledgedResponse>() {
            boolean wasInvoked = false;

            @Override
            public AcknowledgedResponse apply(ActionType<AcknowledgedResponse> actionType, ActionRequest actionRequest) {
                wasInvoked = true;
                assertThat(actionRequest.getClass(), equalTo(PutPipelineRequest.class));
                PutPipelineRequest req = (PutPipelineRequest) actionRequest;
                assertThat(req.getId(), equalTo("my_pipeline"));
                assertThat(req.getVersion(), equalTo(numericValue));
                return null;
            }

            public boolean wasInvoked() {
                return wasInvoked;
            }
        };
        verifyingClient.setExecuteVerifier(verifier);

        dispatchRequest(request);
        assertThat(verifier.wasInvoked(), equalTo(true));
    }
}

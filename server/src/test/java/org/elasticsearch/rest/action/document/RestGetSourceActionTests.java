/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.document.RestGetSourceAction.RestGetSourceResponseListener;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestGetSourceActionTests extends RestActionTestCase {

    private static RestRequest request = new FakeRestRequest();
    private static FakeRestChannel channel = new FakeRestChannel(request, true, 0);
    private static RestGetSourceResponseListener listener = new RestGetSourceResponseListener(channel, request);
    private final List<String> compatibleMediaType = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetSourceAction());
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(GetRequest.class));
            return Mockito.mock(GetResponse.class);
        });
    }

    @AfterClass
    public static void cleanupReferences() {
        request = null;
        channel = null;
        listener = null;
    }

    public void testRestGetSourceAction() throws Exception {
        final BytesReference source = new BytesArray("{\"foo\": \"bar\"}");
        final GetResponse response =
            new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, true, source, emptyMap(), null));

        final RestResponse restResponse = listener.buildResponse(response);

        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));//dropping charset as it was not on a request
        assertThat(restResponse.content(), equalTo(new BytesArray("{\"foo\": \"bar\"}")));
    }

    public void testRestGetSourceActionWithMissingDocument() {
        final GetResponse response =
            new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, false, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Document not found [index1]/[1]"));
    }

    public void testRestGetSourceActionWithMissingDocumentSource() {
        final GetResponse response =
            new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, true, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Source not found [index1]/[1]"));
    }

    /**
     * test deprecation is logged if type is used in path
     */
    public void testTypeInPath() {
        for (RestRequest.Method method : Arrays.asList(RestRequest.Method.GET, RestRequest.Method.HEAD)) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withHeaders(Map.of("Accept", compatibleMediaType))
                .withMethod(method)
                .withPath("/some_index/some_type/id/_source")
                .build();
            dispatchRequest(request);
            assertWarnings(RestGetSourceAction.TYPES_DEPRECATION_MESSAGE);
        }
    }

    /**
     * test deprecation is logged if type is used as parameter
     */
    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");
        for (RestRequest.Method method : Arrays.asList(RestRequest.Method.GET, RestRequest.Method.HEAD)) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withHeaders(Map.of("Accept", compatibleMediaType))
                .withMethod(method)
                .withPath("/some_index/_source/id")
                .withParams(params)
                .build();
            dispatchRequest(request);
            assertWarnings(RestGetSourceAction.TYPES_DEPRECATION_MESSAGE);
        }
    }

}

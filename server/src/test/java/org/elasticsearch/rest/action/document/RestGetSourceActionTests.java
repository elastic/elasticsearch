/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.document.RestGetSourceAction.RestGetSourceResponseListener;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;

public class RestGetSourceActionTests extends RestActionTestCase {

    private static RestRequest request = new FakeRestRequest();
    private static FakeRestChannel channel = new FakeRestChannel(request, true, 0);
    private static RestGetSourceResponseListener listener = new RestGetSourceResponseListener(channel, request);

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetSourceAction());
    }

    @AfterClass
    public static void cleanupReferences() {
        request = null;
        channel = null;
        listener = null;
    }

    /**
     * test deprecation is logged if type is used in path
     */
    public void testTypeInPath() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (action, r) -> new GetResponse(new GetResult("index", "_doc", "id", 0, 1, 0, true, new BytesArray("{}"), null, null))
        );
        for (Method method : Arrays.asList(Method.GET, Method.HEAD)) {
            // Ensure we have a fresh context for each request so we don't get duplicate headers
            try (ThreadContext.StoredContext ignore = verifyingClient.threadPool().getThreadContext().stashContext()) {
                RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(method)
                    .withPath("/some_index/some_type/id/_source")
                    .build();

                dispatchRequest(request);
                assertWarnings(RestGetSourceAction.TYPES_DEPRECATION_MESSAGE);
            }
        }
    }

    /**
     * test deprecation is logged if type is used as parameter
     */
    public void testTypeParameter() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (action, r) -> new GetResponse(new GetResult("index", "_doc", "id", 0, 1, 0, true, new BytesArray("{}"), null, null))
        );
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");
        for (Method method : Arrays.asList(Method.GET, Method.HEAD)) {
            // Ensure we have a fresh context for each request so we don't get duplicate headers
            try (ThreadContext.StoredContext ignore = verifyingClient.threadPool().getThreadContext().stashContext()) {
                RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(method)
                    .withPath("/some_index/_source/id")
                    .withParams(params)
                    .build();
                dispatchRequest(request);
                assertWarnings(RestGetSourceAction.TYPES_DEPRECATION_MESSAGE);
            }
        }
    }

    public void testRestGetSourceAction() throws Exception {
        final BytesReference source = new BytesArray("{\"foo\": \"bar\"}");
        final GetResponse response = new GetResponse(
            new GetResult("index1", "_doc", "1", UNASSIGNED_SEQ_NO, 0, -1, true, source, emptyMap(), null)
        );

        final RestResponse restResponse = listener.buildResponse(response);

        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json; charset=UTF-8"));
        assertThat(restResponse.content(), equalTo(new BytesArray("{\"foo\": \"bar\"}")));
    }

    public void testRestGetSourceActionWithMissingDocument() {
        final GetResponse response = new GetResponse(
            new GetResult("index1", "_doc", "1", UNASSIGNED_SEQ_NO, 0, -1, false, null, emptyMap(), null)
        );

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Document not found [index1]/[_doc]/[1]"));
    }

    public void testRestGetSourceActionWithMissingDocumentSource() {
        final GetResponse response = new GetResponse(
            new GetResult("index1", "_doc", "1", UNASSIGNED_SEQ_NO, 0, -1, true, null, emptyMap(), null)
        );

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Source not found [index1]/[_doc]/[1]"));
    }
}

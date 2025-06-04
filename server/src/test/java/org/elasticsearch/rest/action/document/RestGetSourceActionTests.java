/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.exception.ResourceNotFoundException;
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

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public final class RestGetSourceActionTests extends RestActionTestCase {

    private static RestRequest request = new FakeRestRequest();
    private static FakeRestChannel channel = new FakeRestChannel(request, true, 0);
    private static RestGetSourceResponseListener listener = new RestGetSourceResponseListener(channel, request);

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
        final GetResponse response = new GetResponse(
            new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, true, source, emptyMap(), null)
        );

        final RestResponse restResponse = listener.buildResponse(response);

        assertThat(restResponse.status(), equalTo(OK));
        assertThat(restResponse.contentType(), equalTo("application/json"));// dropping charset as it was not on a request
        assertThat(restResponse.content(), equalTo(new BytesArray("{\"foo\": \"bar\"}")));
    }

    public void testRestGetSourceActionWithMissingDocument() {
        final GetResponse response = new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, false, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Document not found [index1]/[1]"));
    }

    public void testRestGetSourceActionWithMissingDocumentSource() {
        final GetResponse response = new GetResponse(new GetResult("index1", "1", UNASSIGNED_SEQ_NO, 0, -1, true, null, emptyMap(), null));

        final ResourceNotFoundException exception = expectThrows(ResourceNotFoundException.class, () -> listener.buildResponse(response));

        assertThat(exception.getMessage(), equalTo("Source not found [index1]/[1]"));
    }
}

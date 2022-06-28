/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

public class RestGetActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetAction());
    }

    public void testTypeInPathWithGet() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (arg1, arg2) -> new GetResponse(new GetResult("index", "_doc", "id", 0, 1, 0, true, new BytesArray("{}"), null, null))
        );

        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath(
            "/some_index/some_type/some_id"
        );
        dispatchRequest(deprecatedRequest.withMethod(Method.GET).build());
        assertWarnings(RestGetAction.TYPES_DEPRECATION_MESSAGE);

        FakeRestRequest.Builder validRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/some_index/_doc/some_id");
        dispatchRequest(validRequest.withMethod(Method.GET).build());
    }

    public void testTypeInPathWithHead() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (arg1, arg2) -> new GetResponse(new GetResult("index", "_doc", "id", 0, 1, 0, true, new BytesArray("{}"), null, null))
        );

        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath(
            "/some_index/some_type/some_id"
        );
        dispatchRequest(deprecatedRequest.withMethod(Method.HEAD).build());
        assertWarnings(RestGetAction.TYPES_DEPRECATION_MESSAGE);

        FakeRestRequest.Builder validRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/some_index/_doc/some_id");
        dispatchRequest(validRequest.withMethod(Method.HEAD).build());
    }
}

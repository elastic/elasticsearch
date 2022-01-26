/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;

public class RestUpdateActionTests extends RestActionTestCase {

    private RestUpdateAction action;

    @Before
    public void setUpAction() {
        action = new RestUpdateAction();
        controller().registerHandler(action);
    }

    public void testTypeInPath() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier(
            (arg1, arg2) -> new UpdateResponse(new ShardId("index", "uuid", 0), "_doc", "id", 0, 1, 1, UpdateResponse.Result.UPDATED)
        );

        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.POST)
            .withPath("/some_index/some_type/some_id/_update")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestUpdateAction.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.POST)
            .withPath("/some_index/_update/some_id")
            .build();
        dispatchRequest(validRequest);
    }

    public void testUpdateDocVersion() {
        Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("version", Long.toString(randomNonNegativeLong()));
            params.put("version_type", randomFrom(VersionType.values()).name());
        } else if (randomBoolean()) {
            params.put("version", Long.toString(randomNonNegativeLong()));
        } else {
            params.put("version_type", randomFrom(VersionType.values()).name());
        }
        String content = "{\n" + "    \"doc\" : {\n" + "        \"name\" : \"new_name\"\n" + "    }\n" + "}";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("test/_update/1")
            .withParams(params)
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> action.prepareRequest(updateRequest, mock(NodeClient.class))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "internal versioning can not be used for optimistic concurrency control. "
                    + "Please use `if_seq_no` and `if_primary_term` instead"
            )
        );
    }
}

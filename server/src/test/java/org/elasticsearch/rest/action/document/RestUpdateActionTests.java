/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;

public final class RestUpdateActionTests extends RestActionTestCase {
    private RestUpdateAction action;

    @Before
    public void setUpAction() {
        action = new RestUpdateAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(UpdateResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(UpdateResponse.class));
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
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                }
            }""";
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;

import static org.mockito.Mockito.mock;

public class RestFieldCapabilitiesActionTests extends ESTestCase {

    private RestFieldCapabilitiesAction action;

    @Before
    public void setUpAction() {
        action = new RestFieldCapabilitiesAction();
    }

    public void testRequestBodyAndParamsBothInput() throws IOException {
        String content = "{ \"fields\": [\"title\"] }";
        HashMap<String, String> paramsMap = new HashMap<>();
        paramsMap.put("fields", "title");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_field_caps")
            .withParams(paramsMap)
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        try {
            action.prepareRequest(request, mock(NodeClient.class));
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertEquals(
                e.getMessage(),
                "can't specify a request body and [fields]"
                    + " request parameter, either specify a request body or the"
                    + " [fields] request parameter"
            );
        }

    }
}

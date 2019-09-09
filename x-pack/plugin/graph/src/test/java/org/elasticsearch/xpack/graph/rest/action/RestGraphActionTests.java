/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.graph.rest.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

public class RestGraphActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        new RestGraphAction(controller());
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_graph/explore")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        dispatchRequest(request);
        assertWarnings(RestGraphAction.TYPES_DEPRECATION_MESSAGE);
    }

}

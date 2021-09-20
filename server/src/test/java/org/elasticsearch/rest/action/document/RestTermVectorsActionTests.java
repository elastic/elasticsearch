/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestTermVectorsActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestTermVectorsAction());
        //todo how to workaround this? we get AssertionError without this
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(TermVectorsResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(TermVectorsResponse.class));
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.POST)
            .withPath("/some_index/some_type/some_id/_termvectors")
            .build();

        dispatchRequest(request);
        assertWarnings(RestTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject()
            .field("_type", "some_type")
            .field("_id", 1)
            .endObject();

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_termvectors/some_id")
            .withContent(BytesReference.bytes(content), null)
            .build();

        dispatchRequest(request);
        assertWarnings(RestTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }
}

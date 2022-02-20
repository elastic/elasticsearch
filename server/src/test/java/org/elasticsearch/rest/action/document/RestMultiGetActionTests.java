/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class RestMultiGetActionTests extends RestActionTestCase {
    XContentType VND_TYPE = randomVendorType();
    List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(VND_TYPE, RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestMultiGetAction(Settings.EMPTY));
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(MultiGetRequest.class));
            return Mockito.mock(MultiGetResponse.class);
        });
    }

    public void testTypeInPath() {
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("some_index/some_type/_mget").build();
        dispatchRequest(deprecatedRequest);
        assertCriticalWarnings(RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() throws Exception {
        XContentBuilder content = XContentFactory.contentBuilder(VND_TYPE)
            .startObject()
            .startArray("docs")
            .startObject()
            .field("_index", "some_index")
            .field("_type", "_doc")
            .field("_id", "2")
            .endObject()
            .startObject()
            .field("_index", "test")
            .field("_id", "2")
            .endObject()
            .endArray()
            .endObject();

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("_mget")
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withContent(BytesReference.bytes(content), null)
            .build();
        dispatchRequest(request);
        assertCriticalWarnings(RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);
    }

}

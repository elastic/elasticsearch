/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.mockito.Mockito.mock;

public class RestCreateIndexActionTests extends RestActionTestCase {
    private RestCreateIndexAction action;

    @Before
    public void setupAction() {
        action = new RestCreateIndexAction();
        controller().registerHandler(action);
    }

    public void testIncludeTypeName() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, randomFrom("true", "false"));
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/some_index")
            .withParams(params)
            .build();

        action.prepareRequest(deprecatedRequest, mock(NodeClient.class));
        assertWarnings(RestCreateIndexAction.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/some_index")
            .build();
        action.prepareRequest(validRequest, mock(NodeClient.class));
    }

    public void testPrepareTypelessRequest() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, content.contentType()).v2();
        boolean includeTypeName = false;
        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap, includeTypeName);

        XContentBuilder expectedContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();
        Map<String, Object> expectedContentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(expectedContent),
            true,
            expectedContent.contentType()
        ).v2();

        assertEquals(expectedContentAsMap, source);
    }

    public void testPrepareTypedRequest() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("type")
            .startObject("properties")
            .startObject("field1")
            .field("type", "keyword")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, content.contentType()).v2();
        boolean includeTypeName = true;
        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap, includeTypeName);

        assertEquals(contentAsMap, source);
    }

    public void testMalformedMappings() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .field("mappings", "some string")
            .startObject("aliases")
            .startObject("read_alias")
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, content.contentType()).v2();

        boolean includeTypeName = false;
        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap, includeTypeName);
        assertEquals(contentAsMap, source);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestCreateIndexActionTests extends ESTestCase {

    public void testPrepareTypelessRequest() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject()
            .startObject("mappings")
                .startObject("properties")
                    .startObject("field1").field("type", "keyword").endObject()
                    .startObject("field2").field("type", "text").endObject()
                .endObject()
            .endObject()
            .startObject("aliases")
                .startObject("read_alias").endObject()
            .endObject()
        .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(content), true, content.contentType()).v2();
        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap);

        XContentBuilder expectedContent = XContentFactory.jsonBuilder().startObject()
            .startObject("mappings")
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("field1").field("type", "keyword").endObject()
                        .startObject("field2").field("type", "text").endObject()
                    .endObject()
                .endObject()
            .endObject()
            .startObject("aliases")
                .startObject("read_alias").endObject()
            .endObject()
        .endObject();
        Map<String, Object> expectedContentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(expectedContent), true, expectedContent.contentType()).v2();

        assertEquals(expectedContentAsMap, source);
    }

    public void testMalformedMappings() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject()
            .field("mappings", "some string")
            .startObject("aliases")
                .startObject("read_alias").endObject()
            .endObject()
        .endObject();

        Map<String, Object> contentAsMap = XContentHelper.convertToMap(
            BytesReference.bytes(content), true, content.contentType()).v2();

        Map<String, Object> source = RestCreateIndexAction.prepareMappings(contentAsMap);
        assertEquals(contentAsMap, source);
    }

    public void testIncludeTypeName() throws IOException {
        RestCreateIndexAction action = new RestCreateIndexAction();
        List<String> compatibleMediaType = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, randomFrom("true", "false"));
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Accept", compatibleMediaType))
            .withMethod(RestRequest.Method.PUT)
            .withPath("/some_index")
            .withParams(params)
            .build();

        action.prepareRequest(deprecatedRequest, mock(NodeClient.class));
        assertWarnings(RestCreateIndexAction.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.PUT)
            .withPath("/some_index")
            .build();
        action.prepareRequest(validRequest, mock(NodeClient.class));
    }

    public void testTypeInMapping() throws IOException {
        RestCreateIndexAction action = new RestCreateIndexAction();

        List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

        String content = "{\n"
            + "  \"mappings\": {\n"
            + "    \"some_type\": {\n"
            + "      \"properties\": {\n"
            + "        \"field1\": {\n"
            + "          \"type\": \"text\"\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        Map<String, String> params = new HashMap<>();
        params.put(RestCreateIndexAction.INCLUDE_TYPE_NAME_PARAMETER, "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withPath("/some_index")
            .withParams(params)
            .withContent(new BytesArray(content), null)
            .build();

        CreateIndexRequest createIndexRequest = action.prepareRequestV7(request);
        // some_type is replaced with _doc
        assertThat(createIndexRequest.mappings(), equalTo("{\"_doc\":{\"properties\":{\"field1\":{\"type\":\"text\"}}}}"));
        assertWarnings(RestCreateIndexAction.TYPES_DEPRECATION_MESSAGE);
    }
}

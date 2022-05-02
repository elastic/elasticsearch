/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.mockito.Mockito.mock;

public class RestPutIndexTemplateActionTests extends ESTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    private RestPutIndexTemplateAction action;

    @Before
    public void setUpAction() {
        action = new RestPutIndexTemplateAction();
    }

    public void testIncludeTypeName() throws IOException {
        XContentBuilder typedContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("mappings")
            .startObject("my_doc")
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

        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        )
            .withMethod(RestRequest.Method.PUT)
            .withParams(params)
            .withPath("/_template/_some_template")
            .withContent(BytesReference.bytes(typedContent), null)
            .build();
        action.prepareRequest(request, mock(NodeClient.class));
        assertCriticalWarnings(RestPutIndexTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }
}

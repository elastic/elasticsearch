/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.mockito.Mockito.mock;

public class RestPutIndexTemplateActionTests extends RestActionTestCase {
    private RestPutIndexTemplateAction action;

    @Before
    public void setUpAction() {
        action = new RestPutIndexTemplateAction(controller());
    }

    public void testIncludeTypeName() throws IOException {
        XContentBuilder typedContent = XContentFactory.jsonBuilder().startObject()
                .startObject("mappings")
                    .startObject("my_doc")
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

        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, "true");
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.PUT)
                .withParams(params)
                .withPath("/_template/_some_template")
                .withContent(BytesReference.bytes(typedContent), XContentType.JSON)
                .build();
        action.prepareRequest(request, mock(NodeClient.class));
        assertWarnings(RestPutIndexTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }
}

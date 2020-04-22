/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.compat.FakeCompatRestRequestBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RestCreateIndexActionV7Tests extends RestActionTestCase {

    RestCreateIndexActionV7 restHandler = new RestCreateIndexActionV7();

    @Before
    public void setUpAction() {
        controller().registerHandler(restHandler);
    }

    public void testTypeInMapping() throws IOException {
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
        params.put(RestCreateIndexActionV7.INCLUDE_TYPE_NAME_PARAMETER, "true");
        RestRequest request = new FakeCompatRestRequestBuilder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/some_index")
            .withParams(params)
            .withContent(new BytesArray(content), null)
            .build();

        CreateIndexRequest createIndexRequest = restHandler.prepareV7Request(request);
        // some_type is replaced with _doc
        assertThat(createIndexRequest.mappings(), equalTo("{\"_doc\":{\"properties\":{\"field1\":{\"type\":\"text\"}}}}"));
    }
}

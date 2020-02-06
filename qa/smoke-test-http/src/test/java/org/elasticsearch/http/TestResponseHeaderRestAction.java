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
package org.elasticsearch.http;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

public class TestResponseHeaderRestAction extends BaseRestHandler {

    public TestResponseHeaderRestAction(RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, "/_protected", this);
    }

    @Override
    public String getName() {
        return "test_response_header_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if ("password".equals(request.header("Secret"))) {
            RestResponse response = new BytesRestResponse(RestStatus.OK, "Access granted");
            response.addHeader("Secret", "granted");
            return channel -> channel.sendResponse(response);
        } else {
            RestResponse response = new BytesRestResponse(RestStatus.UNAUTHORIZED, "Access denied");
            response.addHeader("Secret", "required");
            return channel -> channel.sendResponse(response);
        }
    }
}

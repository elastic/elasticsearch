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

package org.elasticsearch.script.mustache;

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchTemplateActionV7 extends RestSearchTemplateAction {

    @Override
    public List<Route> routes() {
        return List.of(
            // Deprecated typed endpoints.
            new Route(GET, "/{index}/{type}/_search/template"),
            new Route(POST, "/{index}/{type}/_search/template")
        );
    }

    @Override
    public String compatibleWithVersion() {
        return String.valueOf(Version.V_7_0_0.major);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        request.param("type");
        return super.prepareRequest(request, client);
    }
}

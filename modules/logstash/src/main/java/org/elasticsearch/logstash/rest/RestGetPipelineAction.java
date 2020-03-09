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

package org.elasticsearch.logstash.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.logstash.action.GetPipelineAction;
import org.elasticsearch.logstash.action.GetPipelineRequest;
import org.elasticsearch.logstash.action.GetPipelineResponse;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

public class RestGetPipelineAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "logstash_get_pipeline";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(Method.GET, "/_logstash/pipeline"), new Route(Method.GET, "/_logstash/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final List<String> ids = List.of(request.paramAsStringArray("id", Strings.EMPTY_ARRAY));
        return restChannel -> client.execute(
            GetPipelineAction.INSTANCE,
            new GetPipelineRequest(ids),
            new RestToXContentListener<>(restChannel) {
                @Override
                protected RestStatus getStatus(GetPipelineResponse response) {
                    if (response.pipelines().isEmpty() && ids.isEmpty() == false) {
                        return RestStatus.NOT_FOUND;
                    }
                    return RestStatus.OK;
                }
            }
        );
    }
}

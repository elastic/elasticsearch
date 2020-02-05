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

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestFlushAction extends BaseRestHandler {

    public RestFlushAction(RestController controller) {
        controller.registerHandler(POST, "/_flush", this);
        controller.registerHandler(POST, "/{index}/_flush", this);

        controller.registerHandler(GET, "/_flush", this);
        controller.registerHandler(GET, "/{index}/_flush", this);
    }

    @Override
    public String getName() {
        return "flush_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        FlushRequest flushRequest = new FlushRequest(Strings.splitStringByCommaToArray(request.param("index")));
        flushRequest.indicesOptions(IndicesOptions.fromRequest(request, flushRequest.indicesOptions()));
        flushRequest.force(request.paramAsBoolean("force", flushRequest.force()));
        flushRequest.waitIfOngoing(request.paramAsBoolean("wait_if_ongoing", flushRequest.waitIfOngoing()));
        return channel -> client.admin().indices().flush(flushRequest, new RestToXContentListener<>(channel));
    }
}

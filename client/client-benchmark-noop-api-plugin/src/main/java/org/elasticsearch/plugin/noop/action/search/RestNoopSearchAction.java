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
package org.elasticsearch.plugin.noop.action.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestNoopSearchAction extends BaseRestHandler {
    public RestNoopSearchAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_noop_search", this);
        controller.registerHandler(POST, "/_noop_search", this);
        controller.registerHandler(GET, "/{index}/_noop_search", this);
        controller.registerHandler(POST, "/{index}/_noop_search", this);
        controller.registerHandler(GET, "/{index}/{type}/_noop_search", this);
        controller.registerHandler(POST, "/{index}/{type}/_noop_search", this);
    }

    @Override
    public String getName() {
        return "noop_search_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        return channel -> client.execute(NoopSearchAction.INSTANCE, searchRequest, new RestStatusToXContentListener<>(channel));
    }
}

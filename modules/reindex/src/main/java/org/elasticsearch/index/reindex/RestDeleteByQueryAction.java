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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestDeleteByQueryAction extends AbstractBulkByQueryRestHandler<DeleteByQueryRequest, DeleteByQueryAction> {
    public RestDeleteByQueryAction(Settings settings, RestController controller) {
        super(settings, DeleteByQueryAction.INSTANCE);
        controller.registerHandler(POST, "/{index}/_delete_by_query", this);
        controller.registerHandler(POST, "/{index}/{type}/_delete_by_query", this);
    }

    @Override
    public String getName() {
        return "delete_by_query_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return doPrepareRequest(request, client, false, false);
    }

    @Override
    protected DeleteByQueryRequest buildRequest(RestRequest request) throws IOException {
        /*
         * Passing the search request through DeleteByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parseInternalRequest can override them.
         */
        DeleteByQueryRequest internal = new DeleteByQueryRequest(new SearchRequest());

        Map<String, Consumer<Object>> consumers = new HashMap<>();
        consumers.put("conflicts", o -> internal.setConflicts((String) o));

        parseInternalRequest(internal, request, consumers);

        return internal;
    }
}

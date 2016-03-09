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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.reindex.RestReindexAction.parseCommon;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpdateByQueryAction extends
        AbstractBaseReindexRestHandler<UpdateByQueryRequest, BulkIndexByScrollResponse, TransportUpdateByQueryAction> {

    @Inject
    public RestUpdateByQueryAction(Settings settings, RestController controller, Client client, ClusterService clusterService,
            IndicesQueriesRegistry indicesQueriesRegistry, AggregatorParsers aggParsers, Suggesters suggesters,
            TransportUpdateByQueryAction action) {
        super(settings, controller, client, clusterService, indicesQueriesRegistry, aggParsers, suggesters, action);
        controller.registerHandler(POST, "/{index}/_update_by_query", this);
        controller.registerHandler(POST, "/{index}/{type}/_update_by_query", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        /*
         * Passing the search request through UpdateByQueryRequest first allows
         * it to set its own defaults which differ from SearchRequest's
         * defaults. Then the parse can override them.
         */
        UpdateByQueryRequest internalRequest = new UpdateByQueryRequest(new SearchRequest());

        // Intercept the size parameter and use it for our own size
        internalRequest.setSize(request.paramAsInt("size", internalRequest.getSize()));
        request.params().remove("size");

        /*
         * We can't send parseSearchRequest REST content that it doesn't support
         * so we will have to remove the content that is valid in addition to
         * what it supports from the content first. This is a temporary hack and
         * should get better when SearchRequest has full ObjectParser support
         * then we can delegate and stuff.
         */
        Tuple<XContentType, Map<String, Object>> body = null;
        boolean bodyModified = false;
        if (RestActions.hasBodyContent(request)) {
            body = XContentHelper.convertToMap(RestActions.getRestContent(request), false);
            String conflicts = (String) body.v2().remove("conflicts");
            if (conflicts != null) {
                internalRequest.setConflicts(conflicts);
                bodyModified = true;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> script = (Map<String, Object>) body.v2().remove("script");
            if (script != null) {
                internalRequest.setScript(Script.parse(script, false, parseFieldMatcher));
                bodyModified = true;
            }

            // Intercept size in the request body
            Integer size = (Integer) body.v2().remove("size");
            if (size != null) {
                internalRequest.setSize(size);
                bodyModified = true;
            }
        }
        
        // If the user specified "scroll_size" which is actually the search requets's "size"
        if (request.hasParam("scroll_size")) {
            if (body == null) {
                body = new Tuple<XContentType, Map<String, Object>>(XContentType.JSON, new HashMap<String, Object>());
            }
            body.v2().put("size", request.paramAsInt("scroll_size", -1 /* We don't use the default. */));
            bodyModified = true;
        }

        // Let the requester set search timeout. It is probably only going to be useful for testing but who knows.
        if (request.hasParam("search_timeout")) {
            body.v2().put("timeout", request.paramAsTime("search_timeout", null));
            bodyModified = true;
        }

        BytesReference bodyContent = null;
        if (bodyModified) {
            XContentBuilder builder = XContentFactory.contentBuilder(body.v1());
            builder.map(body.v2());
            bodyContent = builder.bytes();
        }

        RestSearchAction.parseSearchRequest(internalRequest.getSearchRequest(), request, parseFieldMatcher, bodyContent);
        
        String conflicts = request.param("conflicts");
        if (conflicts != null) {
            internalRequest.setConflicts(conflicts);
        }
        parseCommon(internalRequest, request);
        execute(request, internalRequest, channel);
    }
}
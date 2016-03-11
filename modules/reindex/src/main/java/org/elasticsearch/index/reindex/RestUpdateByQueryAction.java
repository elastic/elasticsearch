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

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.SIZE_ALL_MATCHES;
import static org.elasticsearch.index.reindex.RestReindexAction.parseCommon;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.util.Map;

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

public class RestUpdateByQueryAction extends
        AbstractBaseReindexRestHandler<UpdateByQueryRequest, BulkIndexByScrollResponse, TransportUpdateByQueryAction> {
    @Inject
    public RestUpdateByQueryAction(Settings settings, RestController controller, Client client,
            IndicesQueriesRegistry indicesQueriesRegistry, AggregatorParsers aggParsers, Suggesters suggesters,
            ClusterService clusterService, TransportUpdateByQueryAction action) {
        super(settings, client, indicesQueriesRegistry, aggParsers, suggesters, clusterService, action);
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
        int scrollSize = internalRequest.getSearchRequest().source().size();
        internalRequest.getSearchRequest().source().size(SIZE_ALL_MATCHES);
        /*
         * We can't send parseSearchRequest REST content that it doesn't support
         * so we will have to remove the content that is valid in addition to
         * what it supports from the content first. This is a temporary hack and
         * should get better when SearchRequest has full ObjectParser support
         * then we can delegate and stuff.
         */
        BytesReference bodyContent = null;
        if (RestActions.hasBodyContent(request)) {
            bodyContent = RestActions.getRestContent(request);
            Tuple<XContentType, Map<String, Object>> body = XContentHelper.convertToMap(bodyContent, false);
            boolean modified = false;
            String conflicts = (String) body.v2().remove("conflicts");
            if (conflicts != null) {
                internalRequest.setConflicts(conflicts);
                modified = true;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> script = (Map<String, Object>) body.v2().remove("script");
            if (script != null) {
                internalRequest.setScript(Script.parse(script, false, parseFieldMatcher));
                modified = true;
            }
            if (modified) {
                XContentBuilder builder = XContentFactory.contentBuilder(body.v1());
                builder.map(body.v2());
                bodyContent = builder.bytes();
            }
        }
        RestSearchAction.parseSearchRequest(internalRequest.getSearchRequest(), indicesQueriesRegistry, request,
                parseFieldMatcher, aggParsers, suggesters, bodyContent);

        String conflicts = request.param("conflicts");
        if (conflicts != null) {
            internalRequest.setConflicts(conflicts);
        }
        parseCommon(internalRequest, request);

        internalRequest.setSize(internalRequest.getSearchRequest().source().size());
        internalRequest.setPipeline(request.param("pipeline"));
        internalRequest.getSearchRequest().source().size(request.paramAsInt("scroll_size", scrollSize));
        // Let the requester set search timeout. It is probably only going to be useful for testing but who knows.
        if (request.hasParam("search_timeout")) {
            internalRequest.getSearchRequest().source().timeout(request.paramAsTime("search_timeout", null));
        }

        execute(request, internalRequest, channel);
    }
}

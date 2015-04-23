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

package org.elasticsearch.rest.action.suggest;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.search.suggest.Suggest;

/**
 *
 */
public class RestSuggestAction extends BaseRestHandler {

    @Inject
    public RestSuggestAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/_suggest", this);
        controller.registerHandler(GET, "/_suggest", this);
        controller.registerHandler(POST, "/{index}/_suggest", this);
        controller.registerHandler(GET, "/{index}/_suggest", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        SuggestRequest suggestRequest = new SuggestRequest(Strings.splitStringByCommaToArray(request.param("index")));
        suggestRequest.indicesOptions(IndicesOptions.fromRequest(request, suggestRequest.indicesOptions()));
        suggestRequest.listenerThreaded(false);
        if (RestActions.hasBodyContent(request)) {
            suggestRequest.suggest(RestActions.getRestContent(request));
        } else {
            throw new ElasticsearchIllegalArgumentException("no content or source provided to execute suggestion");
        }
        suggestRequest.routing(request.param("routing"));
        suggestRequest.preference(request.param("preference"));

        client.suggest(suggestRequest, new RestBuilderListener<SuggestResponse>(channel) {
            @Override
            public RestResponse buildResponse(SuggestResponse response, XContentBuilder builder) throws Exception {
                RestStatus restStatus = RestStatus.status(response.getSuccessfulShards(), response.getTotalShards(), response.getShardFailures());
                builder.startObject();
                buildBroadcastShardsHeader(builder, response);
                Suggest suggest = response.getSuggest();
                if (suggest != null) {
                    suggest.toXContent(builder, request);
                }
                builder.endObject();
                return new BytesRestResponse(restStatus, builder);
            }
        });
    }
}

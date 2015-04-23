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

package org.elasticsearch.rest.action.exists;

import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.action.exists.ExistsRequest.DEFAULT_MIN_SCORE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Action for /_search/exists endpoint
 */
public class RestExistsAction extends BaseRestHandler {

    public RestExistsAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final ExistsRequest existsRequest = new ExistsRequest(Strings.splitStringByCommaToArray(request.param("index")));
        existsRequest.indicesOptions(IndicesOptions.fromRequest(request, existsRequest.indicesOptions()));
        existsRequest.listenerThreaded(false);
        if (RestActions.hasBodyContent(request)) {
            existsRequest.source(RestActions.getRestContent(request));
        } else {
            QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
            if (querySourceBuilder != null) {
                existsRequest.source(querySourceBuilder);
            }
        }
        existsRequest.routing(request.param("routing"));
        existsRequest.minScore(request.paramAsFloat("min_score", DEFAULT_MIN_SCORE));
        existsRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        existsRequest.preference(request.param("preference"));

        client.exists(existsRequest, new RestBuilderListener<ExistsResponse>(channel) {
            @Override
            public RestResponse buildResponse(ExistsResponse response, XContentBuilder builder) throws Exception {
                RestStatus status = response.exists() ? OK : NOT_FOUND;
                builder.startObject();
                builder.field("exists", response.exists());
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }
}

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

package org.elasticsearch.rest.action.count;

import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.action.count.CountRequest.DEFAULT_MIN_SCORE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 *
 */
public class RestCountAction extends BaseRestHandler {

    @Inject
    public RestCountAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(POST, "/_count", this);
        controller.registerHandler(GET, "/_count", this);
        controller.registerHandler(POST, "/{index}/_count", this);
        controller.registerHandler(GET, "/{index}/_count", this);
        controller.registerHandler(POST, "/{index}/{type}/_count", this);
        controller.registerHandler(GET, "/{index}/{type}/_count", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        CountRequest countRequest = new CountRequest(Strings.splitStringByCommaToArray(request.param("index")));
        countRequest.indicesOptions(IndicesOptions.fromRequest(request, countRequest.indicesOptions()));
        countRequest.listenerThreaded(false);
        if (request.hasContent()) {
            countRequest.source(request.content(), request.contentUnsafe());
        } else {
            String source = request.param("source");
            if (source != null) {
                countRequest.source(source);
            } else {
                QuerySourceBuilder querySourceBuilder = RestActions.parseQuerySource(request);
                if (querySourceBuilder != null) {
                    countRequest.source(querySourceBuilder);
                }
            }
        }
        countRequest.routing(request.param("routing"));
        countRequest.minScore(request.paramAsFloat("min_score", DEFAULT_MIN_SCORE));
        countRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
        countRequest.preference(request.param("preference"));

        client.count(countRequest, new RestBuilderListener<CountResponse>(channel) {
            @Override
            public RestResponse buildResponse(CountResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field("count", response.getCount());

                buildBroadcastShardsHeader(builder, response);

                builder.endObject();
                return new BytesRestResponse(response.status(), builder);
            }
        });
    }
}

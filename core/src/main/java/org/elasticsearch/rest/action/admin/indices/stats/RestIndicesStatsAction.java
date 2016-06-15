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

package org.elasticsearch.rest.action.admin.indices.stats;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
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
import org.elasticsearch.rest.action.support.RestBuilderListener;

import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestActions.buildBroadcastShardsHeader;

/**
 */
public class RestIndicesStatsAction extends BaseRestHandler {

    @Inject
    public RestIndicesStatsAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, "/_stats", this);
        controller.registerHandler(GET, "/_stats/{metric}", this);
        controller.registerHandler(GET, "/_stats/{metric}/{indexMetric}", this);
        controller.registerHandler(GET, "/{index}/_stats", this);
        controller.registerHandler(GET, "/{index}/_stats/{metric}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, indicesStatsRequest.indicesOptions()));
        indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

        Set<String> metrics = Strings.splitStringByCommaToSet(request.param("metric", "_all"));
        // short cut, if no metrics have been specified in URI
        if (metrics.size() == 1 && metrics.contains("_all")) {
            indicesStatsRequest.all();
        } else {
            indicesStatsRequest.clear();
            indicesStatsRequest.docs(metrics.contains("docs"));
            indicesStatsRequest.store(metrics.contains("store"));
            indicesStatsRequest.indexing(metrics.contains("indexing"));
            indicesStatsRequest.search(metrics.contains("search") || metrics.contains("suggest"));
            indicesStatsRequest.get(metrics.contains("get"));
            indicesStatsRequest.merge(metrics.contains("merge"));
            indicesStatsRequest.refresh(metrics.contains("refresh"));
            indicesStatsRequest.flush(metrics.contains("flush"));
            indicesStatsRequest.warmer(metrics.contains("warmer"));
            indicesStatsRequest.queryCache(metrics.contains("query_cache"));
            indicesStatsRequest.segments(metrics.contains("segments"));
            indicesStatsRequest.fieldData(metrics.contains("fielddata"));
            indicesStatsRequest.completion(metrics.contains("completion"));
            indicesStatsRequest.requestCache(metrics.contains("request_cache"));
            indicesStatsRequest.recovery(metrics.contains("recovery"));
            indicesStatsRequest.translog(metrics.contains("translog"));
        }

        if (request.hasParam("groups")) {
            indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("groups")));
        }

        if (request.hasParam("types")) {
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));
        }

        if (indicesStatsRequest.completion() && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            indicesStatsRequest.completionFields(request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", Strings.EMPTY_ARRAY)));
        }

        if (indicesStatsRequest.fieldData() && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            indicesStatsRequest.fieldDataFields(request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", Strings.EMPTY_ARRAY)));
        }

        if (indicesStatsRequest.segments() && request.hasParam("include_segment_file_sizes")) {
            indicesStatsRequest.includeSegmentFileSizes(true);
        }

        client.admin().indices().stats(indicesStatsRequest, new RestBuilderListener<IndicesStatsResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndicesStatsResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                buildBroadcastShardsHeader(builder, request, response);
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}

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

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestIndicesStatsAction extends BaseRestHandler {

    public RestIndicesStatsAction(RestController controller) {
        controller.registerHandler(GET, "/_stats", this);
        controller.registerHandler(GET, "/_stats/{metric}", this);
        controller.registerHandler(GET, "/{index}/_stats", this);
        controller.registerHandler(GET, "/{index}/_stats/{metric}", this);
    }

    @Override
    public String getName() {
        return "indices_stats_action";
    }

    static final Map<String, Consumer<IndicesStatsRequest>> METRICS;

    static {
        Map<String, Consumer<IndicesStatsRequest>> metrics = new HashMap<>();
        for (Flag flag : CommonStatsFlags.Flag.values()) {
            metrics.put(flag.getRestName(), m -> m.flags().set(flag, true));
        }
        METRICS = Collections.unmodifiableMap(metrics);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        boolean forbidClosedIndices = request.paramAsBoolean("forbid_closed_indices", true);
        IndicesOptions defaultIndicesOption = forbidClosedIndices ? indicesStatsRequest.indicesOptions()
            : IndicesOptions.strictExpandOpen();
        assert indicesStatsRequest.indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed() : "IndicesStats default indices " +
            "options changed";
        indicesStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, defaultIndicesOption));
        indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));

        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));
        // short cut, if no metrics have been specified in URI
        if (metrics.size() == 1 && metrics.contains("_all")) {
            indicesStatsRequest.all();
        } else if (metrics.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")));
        } else {
            indicesStatsRequest.clear();
            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
            final Set<String> invalidMetrics = new TreeSet<>();
            for (final String metric : metrics) {
                final Consumer<IndicesStatsRequest> consumer = METRICS.get(metric);
                if (consumer != null) {
                    consumer.accept(indicesStatsRequest);
                } else {
                    invalidMetrics.add(metric);
                }
            }

            if (!invalidMetrics.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
            }
        }

        if (request.hasParam("groups")) {
            indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("groups")));
        }

        if (request.hasParam("types")) {
            indicesStatsRequest.types(Strings.splitStringByCommaToArray(request.param("types")));
        }

        if (indicesStatsRequest.completion() && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            indicesStatsRequest.completionFields(
                    request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", Strings.EMPTY_ARRAY)));
        }

        if (indicesStatsRequest.fieldData() && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            indicesStatsRequest.fieldDataFields(
                    request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", Strings.EMPTY_ARRAY)));
        }

        if (indicesStatsRequest.segments()) {
            indicesStatsRequest.includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
            indicesStatsRequest.includeUnloadedSegments(request.paramAsBoolean("include_unloaded_segments", false));
        }

        return channel -> client.admin().indices().stats(indicesStatsRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private static final Set<String> RESPONSE_PARAMS = Collections.singleton("level");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

}

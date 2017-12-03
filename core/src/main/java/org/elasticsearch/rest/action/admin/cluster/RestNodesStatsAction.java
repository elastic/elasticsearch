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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestNodesStatsAction extends BaseRestHandler {
    public RestNodesStatsAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_nodes/stats", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats", this);

        controller.registerHandler(GET, "/_nodes/stats/{metric}", this);
        controller.registerHandler(GET, "/_nodes/{nodeId}/stats/{metric}", this);

        controller.registerHandler(GET, "/_nodes/stats/{metric}/{index_metric}", this);

        controller.registerHandler(GET, "/_nodes/{nodeId}/stats/{metric}/{index_metric}", this);
    }

    static final Map<String, Consumer<NodesStatsRequest>> METRICS;

    static {
        final Map<String, Consumer<NodesStatsRequest>> metrics = new HashMap<>();
        metrics.put("os", r -> r.os(true));
        metrics.put("jvm", r -> r.jvm(true));
        metrics.put("thread_pool", r -> r.threadPool(true));
        metrics.put("fs", r -> r.fs(true));
        metrics.put("transport", r -> r.transport(true));
        metrics.put("http", r -> r.http(true));
        metrics.put("indices", r -> r.indices(true));
        metrics.put("process", r -> r.process(true));
        metrics.put("breaker", r -> r.breaker(true));
        metrics.put("script", r -> r.script(true));
        metrics.put("discovery", r -> r.discovery(true));
        metrics.put("ingest", r -> r.ingest(true));
        METRICS = Collections.unmodifiableMap(metrics);
    }

    static final Map<String, Consumer<CommonStatsFlags>> FLAGS;

    static {
        final Map<String, Consumer<CommonStatsFlags>> flags = new HashMap<>();
        for (final Flag flag : CommonStatsFlags.Flag.values()) {
            flags.put(flag.getRestName(), f -> f.set(flag, true));
        }
        FLAGS = Collections.unmodifiableMap(flags);
    }

    @Override
    public String getName() {
        return "nodes_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metrics = Strings.splitStringByCommaToSet(request.param("metric", "_all"));

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        nodesStatsRequest.timeout(request.param("timeout"));

        if (metrics.size() == 1 && metrics.contains("_all")) {
            if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains index metrics [%s] but all stats requested",
                        request.path(),
                        request.param("index_metric")));
            }
            nodesStatsRequest.all();
            nodesStatsRequest.indices(CommonStatsFlags.ALL);
        } else if (metrics.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")));
        } else {
            nodesStatsRequest.clear();

            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
            final Set<String> invalidMetrics = new TreeSet<>();
            for (final String metric : metrics) {
                final Consumer<NodesStatsRequest> handler = METRICS.get(metric);
                if (handler != null) {
                    handler.accept(nodesStatsRequest);
                } else {
                    invalidMetrics.add(metric);
                }
            }

            if (!invalidMetrics.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
            }

            // check for index specific metrics
            if (metrics.contains("indices")) {
                Set<String> indexMetrics = Strings.splitStringByCommaToSet(request.param("index_metric", "_all"));
                if (indexMetrics.size() == 1 && indexMetrics.contains("_all")) {
                    nodesStatsRequest.indices(CommonStatsFlags.ALL);
                } else {
                    CommonStatsFlags flags = new CommonStatsFlags();
                    flags.clear();
                    // use a sorted set so the unrecognized parameters appear in a reliable sorted order
                    final Set<String> invalidIndexMetrics = new TreeSet<>();
                    for (final String indexMetric : indexMetrics) {
                        final Consumer<CommonStatsFlags> handler = FLAGS.get(indexMetric);
                        if (handler != null) {
                            handler.accept(flags);
                        } else {
                            invalidIndexMetrics.add(indexMetric);
                        }
                    }

                    if (!invalidIndexMetrics.isEmpty()) {
                        throw new IllegalArgumentException(unrecognized(request, invalidIndexMetrics, FLAGS.keySet(), "index metric"));
                    }

                    nodesStatsRequest.indices(flags);
                }
            } else if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains index metrics [%s] but indices stats not requested",
                        request.path(),
                        request.param("index_metric")));
            }
        }

        if (nodesStatsRequest.indices().isSet(Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            nodesStatsRequest.indices().fieldDataFields(
                    request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            nodesStatsRequest.indices().completionDataFields(
                    request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Search) && (request.hasParam("groups"))) {
            nodesStatsRequest.indices().groups(request.paramAsStringArray("groups", null));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Indexing) && (request.hasParam("types"))) {
            nodesStatsRequest.indices().types(request.paramAsStringArray("types", null));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Segments)) {
            nodesStatsRequest.indices().includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
        }

        return channel -> client.admin().cluster().nodesStats(nodesStatsRequest, new NodesResponseRestListener<>(channel));
    }

    private final Set<String> RESPONSE_PARAMS = Collections.singleton("level");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}

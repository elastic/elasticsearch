/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestNodesStatsAction extends BaseRestHandler {

    private static final Set<String> SUPPORTED_CAPABILITIES = Set.of("dense_vector_off_heap_stats");

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_nodes/stats"),
            new Route(GET, "/_nodes/{nodeId}/stats"),
            new Route(GET, "/_nodes/stats/{metric}"),
            new Route(GET, "/_nodes/{nodeId}/stats/{metric}"),
            new Route(GET, "/_nodes/stats/{metric}/{index_metric}"),
            new Route(GET, "/_nodes/{nodeId}/stats/{metric}/{index_metric}")
        );
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
    public Set<String> supportedCapabilities() {
        return SUPPORTED_CAPABILITIES;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metricNames = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        nodesStatsRequest.setTimeout(getTimeout(request));
        // level parameter validation
        nodesStatsRequest.setIncludeShardsStats(NodeStatsLevel.of(request, NodeStatsLevel.NODE) != NodeStatsLevel.NODE);

        if (metricNames.size() == 1 && metricNames.contains("_all")) {
            if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains index metrics [%s] but all stats requested",
                        request.path(),
                        request.param("index_metric")
                    )
                );
            }
            nodesStatsRequest.all();
            nodesStatsRequest.indices(CommonStatsFlags.ALL);
        } else if (metricNames.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")
                )
            );
        } else {
            nodesStatsRequest.clear();

            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
            final Set<String> invalidMetrics = new TreeSet<>();
            for (final String metricName : metricNames) {
                if (Metric.isValid(metricName)) {
                    nodesStatsRequest.addMetric(Metric.get(metricName));
                } else {
                    if (metricName.equals("indices")) continue; // indices metric has different implications, see below
                    invalidMetrics.add(metricName);
                }
            }

            if (invalidMetrics.isEmpty() == false) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, Metric.ALL_NAMES, "metric"));
            }

            // check for index specific metrics
            if (metricNames.contains("indices")) {
                nodesStatsRequest.indices(true);
                Set<String> indexMetrics = Strings.tokenizeByCommaToSet(request.param("index_metric", "_all"));
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

                    if (invalidIndexMetrics.isEmpty() == false) {
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
                        request.param("index_metric")
                    )
                );
            }
        }

        if (nodesStatsRequest.indices().isSet(Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            nodesStatsRequest.indices()
                .fieldDataFields(request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            nodesStatsRequest.indices()
                .completionDataFields(request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Search) && (request.hasParam("groups"))) {
            nodesStatsRequest.indices().groups(request.paramAsStringArray("groups", null));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Segments)) {
            nodesStatsRequest.indices().includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
            nodesStatsRequest.indices().includeUnloadedSegments(request.paramAsBoolean("include_unloaded_segments", false));
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .nodesStats(nodesStatsRequest, new RestRefCountedChunkedToXContentListener<>(channel));
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

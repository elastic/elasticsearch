/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.NodeStatsLevel;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
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

@ServerlessScope(Scope.INTERNAL)
public class RestNodesStatsAction extends BaseRestHandler {
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestNodesStatsAction.class);
    private static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in nodes stats requests is deprecated.";

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

    static final Map<String, Consumer<NodesStatsRequest>> METRICS;

    static {
        Map<String, Consumer<NodesStatsRequest>> map = new HashMap<>();
        for (NodesStatsRequestParameters.Metric metric : NodesStatsRequestParameters.Metric.values()) {
            map.put(metric.metricName(), request -> request.addMetric(metric.metricName()));
        }
        map.put("indices", request -> request.indices(true));
        METRICS = Collections.unmodifiableMap(map);
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
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("types")) {
            deprecationLogger.compatibleCritical("nodes_stats_types", TYPES_DEPRECATION_MESSAGE);
            request.param("types");
        }

        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        nodesStatsRequest.timeout(request.param("timeout"));
        // level parameter validation
        nodesStatsRequest.setIncludeShardsStats(NodeStatsLevel.of(request, NodeStatsLevel.NODE) != NodeStatsLevel.NODE);

        if (metrics.size() == 1 && metrics.contains("_all")) {
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
        } else if (metrics.contains("_all")) {
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
            for (final String metric : metrics) {
                final Consumer<NodesStatsRequest> handler = METRICS.get(metric);
                if (handler != null) {
                    handler.accept(nodesStatsRequest);
                } else {
                    invalidMetrics.add(metric);
                }
            }

            if (invalidMetrics.isEmpty() == false) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
            }

            // check for index specific metrics
            if (metrics.contains("indices")) {
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

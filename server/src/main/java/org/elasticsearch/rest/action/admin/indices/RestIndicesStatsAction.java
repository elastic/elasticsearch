/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.rest.action.document.RestMultiTermVectorsAction;

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
public class RestIndicesStatsAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestMultiTermVectorsAction.class);
    private static final String TYPES_DEPRECATION_MESSAGE = "[types removal] "
        + "Specifying types in indices stats requests is deprecated.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_stats"),
            new Route(GET, "/_stats/{metric}"),
            new Route(GET, "/{index}/_stats"),
            new Route(GET, "/{index}/_stats/{metric}")
        );
    }

    @Override
    public String getName() {
        return "indices_stats_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
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
        if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("types")) {
            deprecationLogger.compatibleCritical("indices_stats_types", TYPES_DEPRECATION_MESSAGE);
            request.param("types");
        }

        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        boolean forbidClosedIndices = request.paramAsBoolean("forbid_closed_indices", true);
        IndicesOptions defaultIndicesOption = forbidClosedIndices
            ? indicesStatsRequest.indicesOptions()
            : IndicesOptions.strictExpandOpen();
        assert indicesStatsRequest.indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed()
            : "IndicesStats default indices options changed";
        indicesStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, defaultIndicesOption));
        indicesStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        // level parameter validation
        ClusterStatsLevel.of(request, ClusterStatsLevel.INDICES);

        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));
        // short cut, if no metrics have been specified in URI
        if (metrics.size() == 1 && metrics.contains("_all")) {
            indicesStatsRequest.all();
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

            if (invalidMetrics.isEmpty() == false) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
            }
        }

        if (request.hasParam("groups")) {
            indicesStatsRequest.groups(Strings.splitStringByCommaToArray(request.param("groups")));
        }

        if (indicesStatsRequest.completion() && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            indicesStatsRequest.completionFields(
                request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", Strings.EMPTY_ARRAY))
            );
        }

        if (indicesStatsRequest.fieldData() && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            indicesStatsRequest.fieldDataFields(
                request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", Strings.EMPTY_ARRAY))
            );
        }

        if (indicesStatsRequest.segments()) {
            indicesStatsRequest.includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
            indicesStatsRequest.includeUnloadedSegments(request.paramAsBoolean("include_unloaded_segments", false));
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .stats(indicesStatsRequest, new RestChunkedToXContentListener<>(channel));
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

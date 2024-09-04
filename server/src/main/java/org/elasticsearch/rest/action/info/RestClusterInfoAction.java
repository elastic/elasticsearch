/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.info;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric.HTTP;
import static org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric.INGEST;
import static org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric.SCRIPT;
import static org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric.THREAD_POOL;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

@ServerlessScope(Scope.PUBLIC)
public class RestClusterInfoAction extends BaseRestHandler {

    static final Map<Metric, Function<NodesStatsResponse, ChunkedToXContent>> RESPONSE_MAPPER = Map.of(
        HTTP,
        nodesStatsResponse -> nodesStatsResponse.getNodes().stream().map(NodeStats::getHttp).reduce(HttpStats.IDENTITY, HttpStats::merge),
        //
        INGEST,
        nodesStatsResponse -> nodesStatsResponse.getNodes()
            .stream()
            .map(NodeStats::getIngestStats)
            .reduce(IngestStats.IDENTITY, IngestStats::merge),
        //
        THREAD_POOL,
        nodesStatsResponse -> nodesStatsResponse.getNodes()
            .stream()
            .map(NodeStats::getThreadPool)
            .reduce(ThreadPoolStats.IDENTITY, ThreadPoolStats::merge),
        //
        SCRIPT,
        nodesStatsResponse -> nodesStatsResponse.getNodes()
            .stream()
            .map(NodeStats::getScriptStats)
            .reduce(ScriptStats.IDENTITY, ScriptStats::merge)
    );
    static final Set<Metric> AVAILABLE_TARGETS = RESPONSE_MAPPER.keySet();
    static final Set<String> AVAILABLE_TARGET_NAMES = AVAILABLE_TARGETS.stream().map(Metric::metricName).collect(toUnmodifiableSet());

    @Override
    public String getName() {
        return "cluster_info_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_info/{target}"));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        var nodesStatsRequest = new NodesStatsRequest().clear();
        nodesStatsRequest.setIncludeShardsStats(false);
        var targets = Strings.tokenizeByCommaToSet(request.param("target"));

        if (targets.size() == 1 && targets.contains("_all")) {
            targets.clear();
            AVAILABLE_TARGETS.forEach(m -> {
                nodesStatsRequest.addMetric(m);
                targets.add(m.metricName());
            });

        } else if (targets.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "request [%s] contains _all and individual target [%s]", request.path(), request.param("target"))
            );

        } else {
            var invalidTargetParams = targets.stream()
                .filter(Predicate.not(AVAILABLE_TARGET_NAMES::contains))
                .collect(Collectors.toCollection(TreeSet::new));

            if (invalidTargetParams.isEmpty() == false) {
                throw new IllegalArgumentException(unrecognized(request, invalidTargetParams, AVAILABLE_TARGET_NAMES, "target"));
            }

            targets.forEach(metricName -> nodesStatsRequest.addMetric(Metric.get(metricName)));
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .nodesStats(nodesStatsRequest, new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(NodesStatsResponse response) throws Exception {
                    var chunkedResponses = targets.stream()
                        .map(Metric::get)
                        .map(RESPONSE_MAPPER::get)
                        .map(mapper -> mapper.apply(response))
                        .iterator();

                    return RestResponse.chunked(
                        RestStatus.OK,
                        ChunkedRestResponseBodyPart.fromXContent(
                            outerParams -> Iterators.concat(
                                ChunkedToXContentHelper.startObject(),
                                Iterators.single((builder, params) -> builder.field("cluster_name", response.getClusterName().value())),
                                Iterators.flatMap(chunkedResponses, chunk -> chunk.toXContentChunked(outerParams)),
                                ChunkedToXContentHelper.endObject()
                            ),
                            EMPTY_PARAMS,
                            channel
                        ),
                        null
                    );
                }
            });
    }
}

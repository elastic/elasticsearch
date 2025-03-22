/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.util.set.Sets.addToCopy;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestClusterStateAction extends BaseRestHandler {

    private static final Set<String> RESPONSE_PARAMS = addToCopy(Settings.FORMAT_PARAMS, "metric");

    private final SettingsFilter settingsFilter;

    private final ThreadPool threadPool;

    public RestClusterStateAction(SettingsFilter settingsFilter, ThreadPool threadPool) {
        this.settingsFilter = settingsFilter;
        this.threadPool = threadPool;
    }

    @Override
    public String getName() {
        return "cluster_state_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_cluster/state"),
            new Route(GET, "/_cluster/state/{metric}"),
            new Route(GET, "/_cluster/state/{metric}/{indices}")
        );
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest(getMasterNodeTimeout(request));
        clusterStateRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterStateRequest.indicesOptions()));
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        if (request.hasParam("wait_for_metadata_version")) {
            clusterStateRequest.waitForMetadataVersion(request.paramAsLong("wait_for_metadata_version", 0));
        }
        clusterStateRequest.waitForTimeout(request.paramAsTime("wait_for_timeout", ClusterStateRequest.DEFAULT_WAIT_FOR_NODE_TIMEOUT));

        final String[] indices = Strings.splitStringByCommaToArray(request.param("indices", "_all"));
        boolean isAllIndicesOnly = indices.length == 1 && "_all".equals(indices[0]);
        if (isAllIndicesOnly == false) {
            clusterStateRequest.indices(indices);
        }

        if (request.hasParam("metric")) {
            EnumSet<ClusterState.Metric> metrics = ClusterState.Metric.parseString(request.param("metric"), true);
            // do not ask for what we do not need.
            clusterStateRequest.nodes(metrics.contains(ClusterState.Metric.NODES) || metrics.contains(ClusterState.Metric.MASTER_NODE));
            /*
             * there is no distinction in Java api between routing_table and routing_nodes, it's the same info set over the wire, one single
             * flag to ask for it
             */
            clusterStateRequest.routingTable(
                metrics.contains(ClusterState.Metric.ROUTING_TABLE) || metrics.contains(ClusterState.Metric.ROUTING_NODES)
            );
            clusterStateRequest.metadata(metrics.contains(ClusterState.Metric.METADATA));
            clusterStateRequest.blocks(metrics.contains(ClusterState.Metric.BLOCKS));
            clusterStateRequest.customs(metrics.contains(ClusterState.Metric.CUSTOMS));
        }
        settingsFilter.addFilterSettingParams(request);

        // We'll probably want to review this approach of adding the XContent parameter.
        @FixForMultiProject
        final Map<String, String> params;
        if (request.paramAsBoolean("multi_project", false)) {
            params = Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API, "multi-project", "true");
        } else {
            params = Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API);
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            ClusterStateAction.INSTANCE,
            clusterStateRequest,
            new RestChunkedToXContentListener<RestClusterStateResponse>(channel, new ToXContent.DelegatingMapParams(params, request)).map(
                response -> new RestClusterStateResponse(clusterStateRequest, response, threadPool.relativeTimeInMillisSupplier())
            )
        );
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    static final class Fields {
        static final String WAIT_FOR_TIMED_OUT = "wait_for_timed_out";
        static final String CLUSTER_NAME = "cluster_name";
    }

    private static class RestClusterStateResponse implements ChunkedToXContent {

        private final ClusterStateRequest request;
        private final ClusterStateResponse response;
        private final LongSupplier currentTimeMillisSupplier;
        private final long startTimeMillis;

        RestClusterStateResponse(ClusterStateRequest request, ClusterStateResponse response, LongSupplier currentTimeMillisSupplier) {
            this.request = request;
            this.response = response;
            this.currentTimeMillisSupplier = currentTimeMillisSupplier;
            this.startTimeMillis = currentTimeMillisSupplier.getAsLong();
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            if (request.local() == false
                && request.masterNodeTimeout().millis() >= 0
                && currentTimeMillisSupplier.getAsLong() - startTimeMillis > request.masterNodeTimeout().millis()) {
                throw new ElasticsearchTimeoutException("Timed out getting cluster state");
            }

            final ClusterState responseState = response.getState();

            return Iterators.concat(Iterators.single((builder, params) -> {
                builder.startObject();
                if (request.waitForMetadataVersion() != null) {
                    builder.field(Fields.WAIT_FOR_TIMED_OUT, response.isWaitForTimedOut());
                }
                builder.field(Fields.CLUSTER_NAME, response.getClusterName().value());
                return builder;
            }),
                responseState == null ? Collections.emptyIterator() : responseState.toXContentChunked(outerParams),
                ChunkedToXContentHelper.endObject()
            );
        }
    }

}

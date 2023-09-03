/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.geoip.DatabaseNodeService;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction.NodeRequest;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction.NodeResponse;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction.Request;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction.Response;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class GeoIpDownloaderStatsTransportAction extends TransportNodesAction<Request, Response, NodeRequest, NodeResponse> {

    private final TransportService transportService;
    private final DatabaseNodeService registry;
    private final GeoIpDownloaderTaskExecutor geoIpDownloaderTaskExecutor;

    @Inject
    public GeoIpDownloaderStatsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        DatabaseNodeService registry,
        GeoIpDownloaderTaskExecutor geoIpDownloaderTaskExecutor
    ) {
        super(
            GeoIpDownloaderStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.transportService = transportService;
        this.registry = registry;
        this.geoIpDownloaderTaskExecutor = geoIpDownloaderTaskExecutor;
    }

    @Override
    protected Response newResponse(Request request, List<NodeResponse> nodeResponses, List<FailedNodeException> failures) {
        return new Response(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeResponse(in);
    }

    @Override
    protected NodeResponse nodeOperation(NodeRequest request, Task task) {
        GeoIpDownloader geoIpTask = geoIpDownloaderTaskExecutor.getCurrentTask();
        GeoIpDownloaderStats stats = geoIpTask == null || geoIpTask.getStatus() == null ? null : geoIpTask.getStatus();
        return new NodeResponse(
            transportService.getLocalNode(),
            stats,
            registry.getAvailableDatabases(),
            registry.getFilesInTemp(),
            registry.getConfigDatabases()
        );
    }
}

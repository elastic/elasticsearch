/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.IngestGeoIpFeatures.GET_DATABASE_CONFIGURATION_ACTION_MULTI_NODE;

public class TransportGetDatabaseConfigurationAction extends TransportNodesAction<
    GetDatabaseConfigurationAction.Request,
    GetDatabaseConfigurationAction.Response,
    GetDatabaseConfigurationAction.NodeRequest,
    GetDatabaseConfigurationAction.NodeResponse,
    List<DatabaseConfigurationMetadata>> {

    private final FeatureService featureService;

    @Inject
    public TransportGetDatabaseConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        FeatureService featureService
    ) {
        super(
            GetDatabaseConfigurationAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            GetDatabaseConfigurationAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.featureService = featureService;
    }

    @Override
    protected void doExecute(
        Task task,
        GetDatabaseConfigurationAction.Request request,
        ActionListener<GetDatabaseConfigurationAction.Response> listener
    ) {
        if (featureService.clusterHasFeature(clusterService.state(), GET_DATABASE_CONFIGURATION_ACTION_MULTI_NODE) == false) {
            /*
             * TransportGetDatabaseConfigurationAction used to be a TransportMasterNodeAction, and not all nodes in the cluster have been
             * updated. So we don't want to send node requests to the other nodes because they will blow up. Instead, we just return
             * the information that we used to return from the master node (it doesn't make any difference that this might not be the master
             * node, because we're only reading the cluster state).
             */
            newResponseAsync(task, request, createActionContext(task, request), List.of(), List.of(), listener);
        } else {
            super.doExecute(task, request, listener);
        }
    }

    protected List<DatabaseConfigurationMetadata> createActionContext(Task task, GetDatabaseConfigurationAction.Request request) {
        final Set<String> ids;
        if (request.getDatabaseIds().length == 0) {
            // if we did not ask for a specific name, then return all databases
            ids = Set.of("*");
        } else {
            ids = new LinkedHashSet<>(Arrays.asList(request.getDatabaseIds()));
        }

        if (ids.size() > 1 && ids.stream().anyMatch(Regex::isSimpleMatchPattern)) {
            throw new IllegalArgumentException(
                "wildcard only supports a single value, please use comma-separated values or a single wildcard value"
            );
        }

        final IngestGeoIpMetadata geoIpMeta = clusterService.state().metadata().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);
        List<DatabaseConfigurationMetadata> results = new ArrayList<>();

        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, DatabaseConfigurationMetadata> entry : geoIpMeta.getDatabases().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        results.add(entry.getValue());
                    }
                }
            } else {
                DatabaseConfigurationMetadata meta = geoIpMeta.getDatabases().get(id);
                if (meta == null) {
                    throw new ResourceNotFoundException("database configuration not found: {}", id);
                } else {
                    results.add(meta);
                }
            }
        }
        return results;
    }

    protected void newResponseAsync(
        Task task,
        GetDatabaseConfigurationAction.Request request,
        List<DatabaseConfigurationMetadata> results,
        List<GetDatabaseConfigurationAction.NodeResponse> responses,
        List<FailedNodeException> failures,
        ActionListener<GetDatabaseConfigurationAction.Response> listener
    ) {
        ActionListener.run(
            listener,
            l -> ActionListener.respondAndRelease(
                l,
                new GetDatabaseConfigurationAction.Response(results, clusterService.getClusterName(), responses, failures)
            )
        );
    }

    @Override
    protected GetDatabaseConfigurationAction.Response newResponse(
        GetDatabaseConfigurationAction.Request request,
        List<GetDatabaseConfigurationAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        throw new UnsupportedOperationException("Use newResponseAsync instead");
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeRequest newNodeRequest(GetDatabaseConfigurationAction.Request request) {
        return new GetDatabaseConfigurationAction.NodeRequest(request.getDatabaseIds());
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetDatabaseConfigurationAction.NodeResponse(in);
    }

    @Override
    protected GetDatabaseConfigurationAction.NodeResponse nodeOperation(GetDatabaseConfigurationAction.NodeRequest request, Task task) {
        return new GetDatabaseConfigurationAction.NodeResponse(transportService.getLocalNode(), List.of());
    }

}

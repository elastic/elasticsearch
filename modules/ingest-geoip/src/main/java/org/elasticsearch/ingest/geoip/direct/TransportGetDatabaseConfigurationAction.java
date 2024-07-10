/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransportGetDatabaseConfigurationAction extends TransportMasterNodeAction<
    GetDatabaseConfigurationAction.Request,
    GetDatabaseConfigurationAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDatabaseConfigurationAction.class);

    @Inject
    public TransportGetDatabaseConfigurationAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDatabaseConfigurationAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDatabaseConfigurationAction.Request::new,
            indexNameExpressionResolver,
            GetDatabaseConfigurationAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    @Override
    protected void masterOperation(
        final Task task,
        final GetDatabaseConfigurationAction.Request request,
        final ClusterState state,
        final ActionListener<GetDatabaseConfigurationAction.Response> listener
    ) {
        IngestGeoIpMetadata geoIpMeta = state.metadata().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);

        // halfway serious no-configurations case(s)
        if (geoIpMeta.getDatabases().isEmpty()) {
            if (request.getDatabaseIds().length == 0) { // all
                // you asked for all, and there are none, we return the none that there are
                listener.onResponse(new GetDatabaseConfigurationAction.Response(List.of()));
            } else {
                // you asked for *something*, and there are none, we 404
                listener.onFailure(
                    new ResourceNotFoundException(
                        "database configuration or configurations {} not found, no database configurations are configured",
                        Arrays.toString(request.getDatabaseIds())
                    )
                );
            }
            return;
        }

        // okay, now it's back to something almost like serious code
        final Set<String> ids = new HashSet<>(Arrays.asList(request.getDatabaseIds()));
        final var databases = geoIpMeta.getDatabases().values().stream().filter(meta -> {
            if (ids.isEmpty()) {
                return true;
            } else {
                return ids.contains(meta.database().id());
            }
        }).toList();

        if (databases.isEmpty()) {
            if (request.getDatabaseIds().length == 0) {
                listener.onResponse(new GetDatabaseConfigurationAction.Response(List.of()));
            } else {
                listener.onFailure(
                    new ResourceNotFoundException(
                        "database configuration or configurations {} not found",
                        Arrays.toString(request.getDatabaseIds())
                    )
                );
            }
        } else {
            listener.onResponse(new GetDatabaseConfigurationAction.Response(databases));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatabaseConfigurationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}

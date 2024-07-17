/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransportGetDatabaseConfigurationAction extends TransportMasterNodeAction<
    GetDatabaseConfigurationAction.Request,
    GetDatabaseConfigurationAction.Response> {

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

        final IngestGeoIpMetadata geoIpMeta = state.metadata().custom(IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.EMPTY);
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
                    listener.onFailure(new ResourceNotFoundException("database configuration not found: {}", id));
                    return;
                } else {
                    results.add(meta);
                }
            }
        }

        listener.onResponse(new GetDatabaseConfigurationAction.Response(results));
    }

    @Override
    protected ClusterBlockException checkBlock(GetDatabaseConfigurationAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}

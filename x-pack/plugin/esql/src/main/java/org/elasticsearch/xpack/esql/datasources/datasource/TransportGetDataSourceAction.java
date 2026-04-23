/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.LinkedHashMap;
import java.util.Map;

/** Local transport handler for {@link GetDataSourceAction}. */
public class TransportGetDataSourceAction extends TransportLocalProjectMetadataAction<
    GetDataSourceAction.Request,
    GetDataSourceAction.Response> {

    @Inject
    public TransportGetDataSourceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(
            GetDataSourceAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            projectResolver
        );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetDataSourceAction.Request request,
        ProjectState project,
        ActionListener<GetDataSourceAction.Response> listener
    ) {
        final DataSourceMetadata metadata = DataSourceMetadata.get(project.metadata());
        final String[] requested = request.names();
        final LinkedHashMap<String, DataSource> hits = new LinkedHashMap<>();
        if (Strings.isAllOrWildcard(requested)) {
            hits.putAll(metadata.dataSources());
        } else {
            for (String pattern : requested) {
                if (Regex.isSimpleMatchPattern(pattern)) {
                    for (Map.Entry<String, DataSource> entry : metadata.dataSources().entrySet()) {
                        if (Regex.simpleMatch(pattern, entry.getKey())) {
                            hits.putIfAbsent(entry.getKey(), entry.getValue());
                        }
                    }
                } else {
                    DataSource ds = metadata.dataSources().get(pattern);
                    if (ds == null) {
                        listener.onFailure(new ResourceNotFoundException("data source [" + pattern + "] not found"));
                        return;
                    }
                    hits.putIfAbsent(pattern, ds);
                }
            }
        }
        listener.onResponse(new GetDataSourceAction.Response(hits.values()));
    }

    @Override
    protected ClusterBlockException checkBlock(GetDataSourceAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}

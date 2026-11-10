/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.util.Arrays;
import java.util.Map;

/**
 * Read-authorization gate for a dataset a query defines where it reads it, rather than naming one that is registered.
 * <p>
 * Authorized on both objects the read names, by the two mechanisms a registration already goes through: the data
 * source's <b>name</b>, through {@code indices()} and the dataset-creation privilege, and the <b>data source</b>,
 * through {@link DataSourceRequestInfo#dataSourceClusterActionName()} and the datasource interceptor. The body asks
 * nothing, so a caller who may not use the source learns nothing about whether it exists.
 * <p>
 * Local only, and dispatched twice — by the coordinator while it binds the definition, and by a node that receives a
 * plan carrying one ({@code ExternalSourceAuthorizer}) — because a plan is not trusted to have been built by a
 * coordinator the caller was allowed to use. The registered path is authorized in both places for the same reason.
 */
public class EsqlAuthorizeDatasourceAction extends TransportLocalProjectMetadataAction<
    EsqlAuthorizeDatasourceAction.Request,
    ActionResponse.Empty> {

    public static final String NAME = EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME;
    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>(NAME);

    @Inject
    public EsqlAuthorizeDatasourceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
    }

    /** No block: this reads no project state, so parking here would hold a query behind something it does not read. */
    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return null;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        Request request,
        ProjectState project,
        ActionListener<ActionResponse.Empty> listener
    ) {
        listener.onResponse(ActionResponse.Empty.INSTANCE);
    }

    /**
     * The data sources to authorize. Deliberately <b>not</b> {@link IndicesRequest.Replaceable}: a narrowing filter
     * would silently drop a name the caller may not read, where a request fixed on its names is refused outright.
     */
    public static class Request extends LocalClusterStateRequest implements IndicesRequest, DataSourceRequestInfo {

        /** Spelled out rather than defaulted: {@link IndicesOptions#DEFAULT} errors on an unavailable target. */
        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchClosed(true).includeHidden(true))
            .build();

        private final String[] dataSources;

        public Request(TimeValue masterTimeout, String... dataSources) {
            super(masterTimeout);
            assert dataSources.length > 0 : "a dataset reference must name the data source it reads through";
            this.dataSources = dataSources;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String[] indices() {
            return dataSources;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return INDICES_OPTIONS;
        }

        @Override
        public String[] dataSourceNames() {
            return dataSources;
        }

        @Override
        public String dataSourceClusterActionName() {
            return EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "EsqlAuthorizeDatasourceAction.Request{dataSources:" + Arrays.toString(dataSources) + "}";
        }
    }
}

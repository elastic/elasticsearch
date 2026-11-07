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
 * Such a read names two objects, and this action is authorized on both, by the two mechanisms that already govern
 * them:
 * <ul>
 *   <li><b>the name</b>, by the index {@code read} privilege — this is an {@code indices:data/read} action carrying
 *       {@code indices()}, so the authorization engine checks it exactly as it checks a registered dataset's name
 *       through {@link EsqlResolveDatasetAction}. An inline read has one name, the data source's, and a role that
 *       does not cover it cannot reach the data through this door either;</li>
 *   <li><b>the data source</b>, by {@code read} under {@code global.data_source} — the request advertises
 *       {@link DataSourceRequestInfo#dataSourceClusterActionName()}, which is what {@code PutDatasetAction} uses to
 *       have the datasource interceptor settle whether the caller may attach a dataset to that source at all.</li>
 * </ul>
 * Nothing else: the body answers once both have passed, because the answer <i>is</i> that they passed. In particular
 * it does not look the data source up, so a caller who may not use it learns nothing about whether it exists; the
 * definition is bound afterwards, and only for a caller who got through here.
 * <p>
 * Local only. It is issued in the caller's thread context by the coordinator while it binds the definition, and again
 * by a node that receives a plan carrying one ({@code ExternalSourceAuthorizer}), for the reason the registered path
 * is authorized in both places: a plan arriving over the wire was not necessarily built by a coordinator the caller
 * was allowed to use.
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

    /**
     * No block: this reads no project state. The authorization that ran before it reads the caller's roles and the
     * index abstractions, which a metadata-read block does not protect, and parking here would hold a query behind a
     * block its answer does not depend on.
     */
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
     * would drop a name the caller may not read and leave the body unable to tell, whereas a request fixed on its
     * names is refused outright — and refusing reveals nothing, since a data source is not in the index namespace
     * and the name is one the caller typed.
     */
    public static class Request extends LocalClusterStateRequest implements IndicesRequest, DataSourceRequestInfo {

        /** Mirrors {@code PutDatasetAction}: nothing here is a pattern, so the options only let a name not exist yet. */
        private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().matchClosed(true).includeHidden(true))
            .build();

        private final String[] dataSources;

        public Request(TimeValue masterTimeout, String... dataSources) {
            super(masterTimeout);
            assert dataSources.length > 0 : "an empty name list means _all to the security resolver";
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

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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read-authorization gate for a query that reads a data source directly, without a registered dataset in front of
 * it — the {@code DATASET(<data source>, {...})} table function.
 * <p>
 * The dataset path has {@link EsqlResolveDatasetAction} for this, and it works because a dataset is an index
 * abstraction: the caller's index {@code read} privilege on the dataset's name governs it. A data source is not an
 * index abstraction and has no name in that namespace, so nothing about an index privilege says anything about it.
 * What governs it instead is {@code read} in the {@code global.data_source} configurable cluster privilege — the same
 * privilege attaching a dataset to a data source consumes, since a privilege meaning "query out of this data source"
 * should not depend on whether a dataset was registered first.
 * <p>
 * The whole of the check is this action existing and being dispatched. Its name is the one
 * {@code ConfigurableClusterPrivileges.DatasourcePrivileges} already maps to {@code read}, and the privilege's request
 * predicate already demands a {@link DataSourceRequestInfo} whose every {@link Request#dataSourceNames()} the
 * caller's grant covers — so the authorization happens in the security layer before this handler runs, and a caller
 * without the privilege never reaches it. The handler's own job is only to say which of the named data sources this
 * node actually has, which is what makes the round-trip worth making rather than a formality.
 * <p>
 * Dispatched locally in a handler's thread context so it runs as the caller, exactly as the dataset gate is, and for
 * the same reason: a plan arriving at a data node is not trusted to have been built by a coordinator the caller was
 * allowed to use, so the node re-derives the decision from the plan's content rather than believing the sender.
 */
public class EsqlAuthorizeDatasourceAction extends TransportLocalProjectMetadataAction<
    EsqlAuthorizeDatasourceAction.Request,
    EsqlAuthorizeDatasourceAction.Response> {

    public static final String NAME = EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME;
    public static final ActionType<EsqlAuthorizeDatasourceAction.Response> TYPE = new ActionType<>(NAME);

    @Inject
    public EsqlAuthorizeDatasourceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        // Registration, not authorization: the security layer evaluated the privilege before this ran, and a cluster
        // action is all-or-nothing, so every name asked about here is one the caller may read. An absent name
        // therefore means only that this project has not registered it.
        DataSourceMetadata dataSources = DataSourceMetadata.get(project.metadata());
        Set<String> registered = new LinkedHashSet<>();
        for (String name : request.dataSourceNames()) {
            if (dataSources.get(name) != null) {
                registered.add(name);
            }
        }
        listener.onResponse(new Response(registered));
    }

    public static class Request extends LocalClusterStateRequest implements DataSourceRequestInfo {

        private final String[] dataSourceNames;

        /**
         * @param dataSourceNames exact data source names, never patterns. The privilege's request predicate requires
         *                        the caller's grant to cover every one of them, and refuses an empty array outright —
         *                        so a caller with nothing to ask about would be denied rather than waved through, and
         *                        must not build a request at all.
         */
        public Request(TimeValue masterTimeout, String[] dataSourceNames) {
            super(masterTimeout);
            assert dataSourceNames.length > 0 : "an empty name list is vacuously authorized; do not dispatch with nothing to ask about";
            this.dataSourceNames = dataSourceNames;
        }

        @Override
        public String[] dataSourceNames() {
            return dataSourceNames;
        }

        /**
         * This action's own name. The privilege maps it to {@code read}, and unlike the dataset PUT — an
         * {@code indices:} action, whose data source privilege a cluster-privilege check cannot see and which
         * therefore needs an interceptor in the security plugin — this is a cluster action, so naming itself here is
         * all it takes for the ordinary cluster-privilege path to evaluate it.
         */
        @Override
        public String dataSourceClusterActionName() {
            return NAME;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "EsqlAuthorizeDatasourceAction.Request{dataSources:" + Arrays.toString(dataSourceNames) + "}";
        }
    }

    public static class Response extends ActionResponse {

        private final Set<String> dataSources;

        public Response(Set<String> dataSources) {
            this.dataSources = dataSources;
        }

        /** The names from the request that this node has registered; the caller refuses anything missing. */
        public Set<String> dataSources() {
            return dataSources;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }
}

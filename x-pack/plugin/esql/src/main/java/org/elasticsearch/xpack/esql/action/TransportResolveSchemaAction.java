/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * The remotable face of schema discovery. The coordinator invokes this on a remote via
 * {@code getRemoteClusterClient(alias).execute(REMOTE_TYPE, request)} (the {@code resolve_fields} idiom). The remote
 * runs the SAME resolution {@link EsqlResolveSchemaAction#resolveAgainst} runs locally, but against <em>its own</em>
 * project metadata — this is the federation design's recursion: "resolve schema on cluster X = invoke X's umbrella."
 *
 * <p>It is a sibling of the node-local {@link EsqlResolveSchemaAction}, not a replacement. The local action is built on
 * {@code LocalClusterStateRequest}, whose request is deliberately non-serializable ({@code writeTo} is
 * {@code localOnly()}) because it is only ever invoked on the same node. Cross-cluster invocation needs a
 * wire-serializable request, so this action is a {@link HandledTransportAction} carrying a real
 * {@link Request#writeTo}. The {@link IndicesOptions} carry {@code resolveDatasets}/{@code resolveViews} so the security
 * action-filter authorizes the names before the handler sees them — the same Batch-B authorization the local action gets.
 *
 * <p>This is the live caller the standing "B1" gap described: the coordinator's federating split
 * ({@code SchemaService.resolveSchemaFederated}) hits this to resolve a remote cluster's schema. Unifying the two faces
 * onto one cross-cluster-capable base (so {@code resolve_schema} has a single action) is a production follow-up; for the
 * POC the remotable face is additive and the local action is untouched.
 */
public class TransportResolveSchemaAction extends HandledTransportAction<
    TransportResolveSchemaAction.Request,
    EsqlResolveSchemaAction.Response> {

    public static final String NAME = "indices:data/read/esql/resolve_schema_remote";
    public static final ActionType<EsqlResolveSchemaAction.Response> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<EsqlResolveSchemaAction.Response> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        EsqlResolveSchemaAction.Response::new
    );

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportResolveSchemaAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<EsqlResolveSchemaAction.Response> listener) {
        var projectMetadata = projectResolver.getProjectMetadata(clusterService.state());
        listener.onResponse(
            new EsqlResolveSchemaAction.Response(
                EsqlResolveSchemaAction.resolveAgainst(projectMetadata, request.indices(), indexNameExpressionResolver)
            )
        );
    }

    /** The wire-serializable request: the (already-enumerated) index expressions to resolve on the remote. */
    public static class Request extends ActionRequest implements IndicesRequest.Replaceable {

        private static final IndicesOptions SCHEMA_INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
            .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(true).build())
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .build();

        private String[] indices;

        public Request(String[] indices) {
            this.indices = indices;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return SCHEMA_INDICES_OPTIONS;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}

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
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.session.schema.ResolvedSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The one schema-discovery action: resolve the schema of any index abstraction for already-enumerated, already-authorized
 * names. It is meant to subsume {@code resolve_views} and {@code resolve_datasets} and to be invoked uniformly — via the
 * local client for local patterns and a remote-cluster client for {@code x:foo} — so local and remote share one handler.
 * <p>The handler runs the remote node's own resolution against <em>that node's</em> cluster state — the recursion the
 * federation design calls for ("resolve schema on cluster X = invoke X's umbrella"). Two kinds resolve synchronously
 * from cluster state and so federate through this action: <b>datasets</b> (its security-filtered {@code resolveDatasets}
 * request authorizes the names exactly as {@code resolve_datasets} does, returning each one's external-source config)
 * and <b>views</b> (the {@code resolveViews} filter authorizes the view names; each is returned as a {@link ResolvedSchema}
 * carrying its identity). The wire form ({@link Response#writeTo}) is real, not {@code localOnly()}, so this is the live
 * remotable face the coordinator's federating split invokes.
 *
 * <p>POC scope: a view's full output schema is its body analyzed against the remote's state (the §4 showcase, async +
 * heavier) — not produced here; the view leg proves the name resolves through the remote umbrella and the kind crosses
 * the wire. Indices keep federating via the proven field-caps CCS path (the design's minimal carve-out), not this action.
 */
public class EsqlResolveSchemaAction extends TransportLocalProjectMetadataAction<
    EsqlResolveSchemaAction.Request,
    EsqlResolveSchemaAction.Response> {
    public static final String NAME = "indices:data/read/esql/resolve_schema";
    public static final ActionType<EsqlResolveSchemaAction.Response> TYPE = new ActionType<>(NAME);

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public EsqlResolveSchemaAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ProjectResolver projectResolver
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, actionFilters, transportService.getTaskManager(), clusterService, EsExecutors.DIRECT_EXECUTOR_SERVICE, projectResolver);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState project, ActionListener<Response> listener) {
        listener.onResponse(new Response(resolveAgainst(project.metadata(), request.indices(), indexNameExpressionResolver)));
    }

    /**
     * The schema resolution a node runs against <em>its own</em> project metadata — shared by the local
     * {@link EsqlResolveSchemaAction} handler and the remotable {@link TransportResolveSchemaAction} handler so local and
     * remote resolution are literally the same code (the federation design's "invoke the cluster's umbrella"). Datasets
     * carry their external-source config; views carry their identity (full output schema is a production follow-up).
     */
    public static List<ResolvedSchema> resolveAgainst(
        org.elasticsearch.cluster.metadata.ProjectMetadata projectMetadata,
        String[] indices,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        // indices is the security-narrowed authorized set on a secured cluster (== the raw indices without security).
        DatasetRewriter.DatasetResolution resolution = DatasetRewriter.resolve(
            indices,
            indices,
            projectMetadata,
            indexNameExpressionResolver
        );
        List<ResolvedSchema> schemas = new ArrayList<>();
        for (String name : resolution.authorizedDatasets()) {
            schemas.add(new ResolvedSchema.Dataset(name, DatasetRewriter.datasetConfig(projectMetadata, name)));
        }
        // Views resolve from cluster state too: any requested name that is a VIEW abstraction on this node is returned
        // with its identity. Its full output schema (the body analyzed against this node's state — the §4 showcase) is a
        // production follow-up; the POC view leg proves the name resolves through the remote umbrella and the VIEW kind
        // crosses the wire. The empty implementation plan is a placeholder the remote-exec handle will replace.
        var lookup = projectMetadata.getIndicesLookup();
        for (String name : indices) {
            var abstraction = lookup.get(name);
            if (abstraction != null && abstraction.getType() == org.elasticsearch.cluster.metadata.IndexAbstraction.Type.VIEW) {
                schemas.add(new ResolvedSchema.View(name, List.of(), null));
            }
        }
        return schemas;
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

        private static final IndicesOptions SCHEMA_INDICES_OPTIONS = IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
            .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(true).build())
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
            .build();

        private static final IndicesOptions CPS_SCHEMA_INDICES_OPTIONS = IndicesOptions.builder(SCHEMA_INDICES_OPTIONS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        private final IndicesOptions indicesOptions;
        private String[] indices = new String[0];
        @Nullable
        private String projectRouting;
        @Nullable
        private TargetProjects resolvedTargetProjects;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        public Request(TimeValue masterTimeout, boolean cpsEnabled) {
            super(masterTimeout);
            this.indicesOptions = cpsEnabled ? CPS_SCHEMA_INDICES_OPTIONS : SCHEMA_INDICES_OPTIONS;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        @Override
        public boolean allowsCrossProject() {
            return true;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        public void setProjectRouting(@Nullable String projectRouting) {
            this.projectRouting = projectRouting;
        }

        @Override
        public String getProjectRouting() {
            return projectRouting;
        }

        @Override
        public void setResolvedTargetProjects(TargetProjects resolvedTargetProjects) {
            this.resolvedTargetProjects = resolvedTargetProjects;
        }

        @Override
        public TargetProjects getResolvedTargetProjects() {
            return resolvedTargetProjects;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "EsqlResolveSchemaAction.Request={indices:" + Arrays.toString(indices) + "}";
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return this.resolvedIndexExpressions;
        }
    }

    /**
     * The remotable response. {@code writeTo} is real (not {@code localOnly()}): for each resolved abstraction it writes
     * the kind tag, the (qualified) name, and the resolved columns as {@link Attribute}s — the single cross-cluster merge
     * currency. {@code Attribute} is {@code NamedWriteable}, so it rides the named-writeable registry. A remote's internal
     * {@code IndexResolution}/expanded-view body never crosses the wire; only schema (Attributes) does. On read each entry
     * becomes a {@link ResolvedSchema.Remote} carrying the kind + attributes.
     *
     * <p>POC scope: the read path reconstructs the {@link ResolvedSchema.Remote} attribute-only form. Full cross-version
     * protocol-compat (versioned field gating against the negotiated {@code TransportVersion} for old/new cluster pairs)
     * is a production follow-up — the {@code #cps-project-team} "many protocol versions" problem — and is deliberately not
     * solved here. A single version gate is enough to prove the wire works.
     */
    public static class Response extends ActionResponse {
        private final List<ResolvedSchema> schemas;

        public Response(List<ResolvedSchema> schemas) {
            this.schemas = schemas;
        }

        public Response(StreamInput in) throws IOException {
            int count = in.readVInt();
            List<ResolvedSchema> read = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResolvedSchema.Remote.Kind kind = in.readEnum(ResolvedSchema.Remote.Kind.class);
                String name = in.readString();
                List<Attribute> attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
                Map<String, Object> config = in.readBoolean() ? in.readGenericMap() : Map.of();
                read.add(new ResolvedSchema.Remote(name, kind, attributes, config));
            }
            this.schemas = read;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(schemas.size());
            for (ResolvedSchema schema : schemas) {
                out.writeEnum(kindOf(schema));
                out.writeString(schema.name());
                out.writeNamedWriteableCollection(schema.attributes());
                Map<String, Object> config = configOf(schema);
                out.writeBoolean(config != null);
                if (config != null) {
                    out.writeGenericMap(config);
                }
            }
        }

        private static ResolvedSchema.Remote.Kind kindOf(ResolvedSchema schema) {
            return switch (schema) {
                case ResolvedSchema.Index ignored -> ResolvedSchema.Remote.Kind.INDEX;
                case ResolvedSchema.View ignored -> ResolvedSchema.Remote.Kind.VIEW;
                case ResolvedSchema.Dataset ignored -> ResolvedSchema.Remote.Kind.DATASET;
                case ResolvedSchema.Remote remote -> remote.kind();
            };
        }

        private static Map<String, Object> configOf(ResolvedSchema schema) {
            return switch (schema) {
                case ResolvedSchema.Dataset dataset -> dataset.config();
                case ResolvedSchema.Remote remote -> remote.config().isEmpty() ? null : remote.config();
                case ResolvedSchema.Index ignored -> null;
                case ResolvedSchema.View ignored -> null;
            };
        }

        public List<ResolvedSchema> schemas() {
            return schemas;
        }
    }
}

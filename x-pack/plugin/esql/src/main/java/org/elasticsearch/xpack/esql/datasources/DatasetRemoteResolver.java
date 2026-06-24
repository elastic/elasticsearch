/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter.DatasetResolution;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cross-project (CPS) leg of {@code FROM <dataset>} resolution: the companion of the local {@link DatasetResolver}.
 * For a Technical Preview, ES|QL must <em>detect</em> — and cleanly fail on — a {@code FROM <pattern>} that would resolve
 * to a dataset defined in a <b>linked project</b>, rather than silently returning local-only data that GA would expand.
 * (GA later flips fail→read.)
 *
 * <p>This resolver dispatches the same read-authorization that {@link EsqlResolveDatasetAction} performs locally to each
 * authorized linked project: it sends one {@link RemoteRequest} per linked project carrying a relation's raw FROM
 * patterns. The request is an {@link IndicesRequest.Replaceable} on the {@code indices:data/read/*} family, so the
 * receiving node's security filter read-authorizes and narrows the names against <em>that</em> project's state exactly as
 * the local path does — authorization is never bypassed or reimplemented here. If any linked project reports a non-empty
 * authorized {@link EsqlResolveDatasetAction.Response#datasets() dataset set}, a remote dataset the caller may read
 * exists, and {@link DatasetResolver} fails the query.
 *
 * <p>Modeled on {@code EnrichPolicyResolver}: a plain collaborator (not an {@code ActionType}) that registers its own
 * node-to-node transport handler, because the local {@link EsqlResolveDatasetAction} is a node-local action with no
 * cross-cluster handler and a non-serializable request. The reused {@link EsqlResolveDatasetAction.Response} is already
 * wire-serializable in both directions.
 */
public class DatasetRemoteResolver {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;
    private final Executor executor;

    public DatasetRemoteResolver(
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(transportService, clusterService, projectResolver, indexNameExpressionResolver, true);
    }

    /**
     * @param registerHandler whether to register the node-to-node transport handler. Always {@code true} in production;
     *                        a unit test that overrides {@link #anyLinkedProjectHasDataset} (so no real dispatch happens)
     *                        passes {@code false} to avoid needing a live {@link TransportService}.
     */
    protected DatasetRemoteResolver(
        TransportService transportService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        boolean registerHandler
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = transportService == null ? null : transportService.getThreadPool();
        this.executor = threadPool == null ? null : threadPool.executor(ThreadPool.Names.SEARCH);
        if (registerHandler) {
            transportService.registerRequestHandler(
                EsqlDatasetActionNames.ESQL_RESOLVE_DATASET_REMOTE_ACTION_NAME,
                executor,
                RemoteRequest::new,
                new RequestHandler()
            );
        }
    }

    /**
     * The aliases of the linked projects registered for {@code originProjectId}. In CPS each linked project is a remote
     * cluster connection keyed by its project alias, so {@link org.elasticsearch.transport.RemoteClusterService} is the
     * always-available source (no security-plugin dependency). The set is <em>not</em> authorization-filtered — that is
     * intentional and safe: each remote re-authorizes the dataset names against its own state, so an unauthorized linked
     * project simply reports no datasets.
     *
     * <p>Single-project assumption (TP): the aliases are resolved for {@code originProjectId}, while the connection in
     * {@link #anyLinkedProjectHasDataset} resolves against the current thread-context project. These coincide today; a
     * multi-project context would need both scoped to the same project (the {@code @FixForMultiProject} caveat on
     * {@code RemoteClusterService.getRemoteClusterConnection}).
     */
    Set<String> linkedProjectAliases(ProjectId originProjectId) {
        return transportService.getRemoteClusterService().getRegisteredRemoteClusterNames(originProjectId);
    }

    /**
     * Fans the relation's raw FROM patterns out to every supplied linked-project alias, then completes {@code listener}
     * with {@code true} if any linked project authorizes at least one dataset for the caller (a detected remote dataset).
     * Per-project BWC gate: a linked project whose connection does not {@code supports} the
     * {@link EsqlResolveDatasetAction#ESQL_RESOLVE_DATASET_REMOTE} wire contract is skipped (no detection — today's
     * behaviour). An empty alias set short-circuits to {@code false}.
     */
    void anyLinkedProjectHasDataset(String[] rawPatterns, Collection<String> linkedProjectAliases, ActionListener<Boolean> listener) {
        if (linkedProjectAliases.isEmpty()) {
            listener.onResponse(false);
            return;
        }
        // Any single positive detection fails the whole query, so a flag is sufficient — no need to attribute to a project.
        // RefCountingListener completes once every per-project response (or failure) has landed. A per-project transport
        // failure propagates and FAILS the query (fail-closed) — intentional for TP→GA safety: a transient remote error must
        // not be silently treated as "no remote dataset", which would reintroduce the silent partial-now/full-at-GA break
        // this guard prevents. GA may revisit (skip-on-error, once remote datasets actually resolve).
        final AtomicBoolean detected = new AtomicBoolean(false);
        try (RefCountingListener refs = new RefCountingListener(listener.map(unused -> detected.get()))) {
            for (String alias : linkedProjectAliases) {
                ActionListener<EsqlResolveDatasetAction.Response> perProject = refs.acquire(response -> {
                    if (response != null && response.datasets().isEmpty() == false) {
                        detected.set(true);
                    }
                });
                transportService.getRemoteClusterService()
                    .maybeEnsureConnectedAndGetConnection(alias, true, perProject.delegateFailureAndWrap((delegate, connection) -> {
                        if (connection.getTransportVersion().supports(EsqlResolveDatasetAction.ESQL_RESOLVE_DATASET_REMOTE) == false) {
                            // Below the wire contract: skip this project, no detection (matches today's CPS behaviour).
                            delegate.onResponse(null);
                            return;
                        }
                        transportService.sendRequest(
                            connection,
                            EsqlDatasetActionNames.ESQL_RESOLVE_DATASET_REMOTE_ACTION_NAME,
                            new RemoteRequest(rawPatterns),
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(delegate, EsqlResolveDatasetAction.Response::new, executor)
                        );
                    }));
            }
        }
    }

    /**
     * The wire-serializable cross-project request. Mirrors {@link EsqlResolveDatasetAction.Request}: an
     * {@link IndicesRequest.Replaceable} carrying both the (security-narrowable) {@code indices} and the original
     * {@code rawPatterns}, so the receiving node authorizes and narrows the names against its own project state.
     */
    static class RemoteRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {

        private String[] indices;
        private final String[] rawPatterns;
        private ResolvedIndexExpressions resolvedIndexExpressions;

        RemoteRequest(String[] rawPatterns) {
            this.indices = rawPatterns;
            this.rawPatterns = rawPatterns;
        }

        RemoteRequest(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
            this.rawPatterns = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeStringArray(rawPatterns);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        /** The original raw FROM patterns, unaffected by the security filter's in-flight narrowing of {@link #indices()}. */
        String[] rawPatterns() {
            return rawPatterns;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return DatasetRewriter.RESOLVER_OPTIONS;
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public String toString() {
            return "DatasetRemoteResolver.RemoteRequest{indices:" + Arrays.toString(indices) + "}";
        }
    }

    /**
     * Runs on the linked project. Re-runs the identical engine-side resolution the local action runs
     * ({@link DatasetRewriter#resolve}) against this project's own state. The security filter narrowed {@link RemoteRequest#indices()}
     * in flight before this handler ran (the action is on the {@code indices:data/read/*} family), so the response only
     * carries datasets the caller may read here.
     */
    private class RequestHandler implements TransportRequestHandler<RemoteRequest> {
        @Override
        public void messageReceived(RemoteRequest request, TransportChannel channel, Task task) {
            ThreadContext threadContext = threadPool.getThreadContext();
            ActionListener<EsqlResolveDatasetAction.Response> listener = ContextPreservingActionListener.wrapPreservingContext(
                new ChannelActionListener<>(channel),
                threadContext
            );
            ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(clusterService.state());
            DatasetResolution resolution = DatasetRewriter.resolve(
                request.indices(),
                request.rawPatterns(),
                projectMetadata,
                indexNameExpressionResolver
            );
            listener.onResponse(
                new EsqlResolveDatasetAction.Response(
                    resolution.authorizedDatasets(),
                    resolution.hasNonDatasetTargets(),
                    resolution.explicitUnauthorized()
                )
            );
        }
    }
}

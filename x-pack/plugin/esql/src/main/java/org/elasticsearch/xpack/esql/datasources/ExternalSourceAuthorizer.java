/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.esql.action.EsqlAuthorizeDatasourceAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;

import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * Decides, on the node that received a plan, whether the request's credential may read the datasets the plan reads.
 * <p>
 * Two kinds, each authorized by its own privilege on its own object before anything binds. A plan naming a
 * <b>registered</b> dataset goes through the coordinator's gate for {@code FROM <dataset>}, described below; a plan
 * carrying the <b>definition itself</b> goes through {@link EsqlAuthorizeDatasourceAction}, the gate a registration
 * against that data source needs. That refusal is reported as it arrives rather than folded into the message below,
 * since it names an object the caller typed.
 * <p>
 * The compute actions are authorized by the security filter on their {@code indices()}, which the sender sets, and
 * a plan can pair an allowed index pattern with an external source for any dataset registered here; the filter never
 * sees the dataset name. So the decision is derived from the plan's content ({@link ExternalSourceDatasets}) and made
 * through the coordinator's own gate for {@code FROM <dataset>}: a local {@link EsqlResolveDatasetAction} issued in
 * the handler's thread context, so it runs as the caller, with the names as exact patterns. The security filter
 * narrows that request to the names the caller has {@code read} on, the DLS/FLS interceptor rejects restricted
 * datasets, and the action resolves what is left against this node's project metadata. A name that does not come back
 * is denied, whether it was unauthorized, outside the credential's grant or never registered: one refusal for all
 * three, with the same message the rebind uses ({@link DatasetConfigResolver#notRegistered}), so a caller probing
 * names learns nothing from the shape of the error. A failure of the round-trip itself that says nothing about the
 * names (the node shutting down, a cluster block, task cancellation) is reported as what it is, since hiding it would
 * tell the coordinator a dataset does not exist when the node merely could not answer. Without security the action
 * resolves the registered names, which is exactly what the rebind then enforces.
 * <p>
 * Nothing the sender puts on the wire is consulted: neither the request's {@code indices()} nor its options take part.
 * The request is built only from names that are exact and local (no wildcard, exclusion, selector or remote prefix);
 * any other name is refused before a request exists, since the action narrows such parts away and would otherwise
 * be handed an empty name list, which the security resolver reads as {@code _all}.
 * <p>
 * Refusals not decided by name share the exception type and status:
 * <ul>
 *   <li>an external node with no dataset name on a release build, as in {@link DatasetConfigResolver#rebind};</li>
 *   <li>any external node under a cross-cluster-access authentication ({@link #isCrossClusterAccess}): datasets are
 *       local to the cluster that registered them, a remote cluster's plan is built from indices only, and a direct
 *       request over the remote-cluster server port must not be able to read a dataset a cross-cluster key's search
 *       grant happens to cover by pattern.</li>
 * </ul>
 * The refusals raised here are not audited by the security filter, which audits only its own decisions; the
 * authorization of the resolve action itself is.
 */
public final class ExternalSourceAuthorizer {

    private static final Logger LOGGER = LogManager.getLogger(ExternalSourceAuthorizer.class);

    private final Client client;
    private final SecurityContext securityContext;
    private final Executor executor;
    private final boolean snapshotBuild;

    /**
     * @param client        the node client the resolve action is issued through, in the caller's thread context
     * @param threadContext the handler's thread context, read for the request's authentication
     * @param executor      where the decision is delivered; the action can complete on another thread (the security
     *                      filter's role resolution, a cluster-state block wait), so the caller's continuation is
     *                      moved off it as {@link DatasetResolver} does. The handler runs on this executor already, so
     *                      the hop is forced through rather than subject to the pool's queue limit.
     * @param snapshotBuild whether an external node with no dataset name is let through unchanged (inline
     *                      {@code EXTERNAL}); injected rather than read from {@code Build} for the same reason as in
     *                      {@link DatasetConfigResolver}
     */
    public ExternalSourceAuthorizer(Client client, ThreadContext threadContext, Executor executor, boolean snapshotBuild) {
        this.client = client;
        this.securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        this.executor = executor;
        this.snapshotBuild = snapshotBuild;
    }

    /**
     * Whether the request in the current thread context is authenticated as cross-cluster access. {@code false} when
     * security is disabled, since then no authentication is in the context at all.
     */
    public boolean isCrossClusterAccess() {
        Authentication authentication = securityContext.getAuthentication();
        return authentication != null && authentication.isCrossClusterAccess();
    }

    /**
     * Completes {@code listener} once every dataset in {@code datasets} is confirmed readable by the caller, or fails
     * it with the refusal. A plan with no external node is accepted without a round-trip. The data sources behind
     * carried definitions are settled first, so a caller who may not use one is refused before any dataset name of
     * theirs is resolved. Cancellation of the parent task propagates to both actions; that failure and the others
     * that say nothing about the names are reported as such rather than as a refusal ({@link #isUnrelatedToNames}).
     *
     * @param parentTaskId the handler's task, so both actions are cancelled with it
     */
    public void authorize(ExternalSourceDatasets datasets, TaskId parentTaskId, ActionListener<Void> listener) {
        if (datasets.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        if (datasets.hasUnnamed() && snapshotBuild == false) {
            listener.onFailure(DatasetConfigResolver.notRegistered(null));
            return;
        }
        Set<String> names = datasets.names();
        for (String name : names) {
            if (isExactLocalName(name) == false) {
                listener.onFailure(DatasetConfigResolver.notRegistered(name));
                return;
            }
        }
        for (String dataSource : datasets.dataSources()) {
            if (isExactLocalName(dataSource) == false) {
                listener.onFailure(notAuthorizable(dataSource));
                return;
            }
        }
        if (datasets.dataSources().isEmpty() == false) {
            authorizeDataSources(
                datasets.dataSources(),
                parentTaskId,
                listener.delegateFailureAndWrap((delegate, ignored) -> authorizeNames(names, parentTaskId, delegate))
            );
            return;
        }
        authorizeNames(names, parentTaskId, listener);
    }

    /** The data sources behind the plan's carried definitions, in one request whatever the plan holds. */
    private void authorizeDataSources(Set<String> dataSources, TaskId parentTaskId, ActionListener<Void> listener) {
        EsqlAuthorizeDatasourceAction.Request request = new EsqlAuthorizeDatasourceAction.Request(
            REST_MASTER_TIMEOUT_DEFAULT,
            dataSources.toArray(String[]::new)
        );
        request.setParentTask(parentTaskId);
        client.execute(
            EsqlAuthorizeDatasourceAction.TYPE,
            request,
            new ThreadedActionListener<>(executor, true, listener.map(ignored -> null))
        );
    }

    private void authorizeNames(Set<String> names, TaskId parentTaskId, ActionListener<Void> listener) {
        if (names.isEmpty()) {
            // No registered dataset to authorize: the external nodes carried their own definitions, settled above.
            listener.onResponse(null);
            return;
        }
        EsqlResolveDatasetAction.Request request = new EsqlResolveDatasetAction.Request(
            REST_MASTER_TIMEOUT_DEFAULT,
            names.toArray(String[]::new),
            false
        );
        request.setParentTask(parentTaskId);
        client.execute(EsqlResolveDatasetAction.TYPE, request, new ThreadedActionListener<>(executor, true, new ActionListener<>() {
            @Override
            public void onResponse(EsqlResolveDatasetAction.Response response) {
                for (String name : names) {
                    if (response.datasets().contains(name) == false) {
                        listener.onFailure(DatasetConfigResolver.notRegistered(name));
                        return;
                    }
                }
                listener.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                if (isUnrelatedToNames(e)) {
                    listener.onFailure(e);
                    return;
                }
                // Includes the DLS/FLS interceptor's rejection. The cause is not attached: it would name the condition
                // the one refusal message deliberately hides.
                LOGGER.debug(() -> Strings.format("refusing external datasets %s: resolve action failed", names), e);
                listener.onFailure(DatasetConfigResolver.notRegistered(names.iterator().next()));
            }
        }));
    }

    /**
     * A failure the round-trip can end with that carries nothing about the names asked for, so passing it on as is
     * reveals nothing and tells the coordinator what actually happened: the task was cancelled, the node is shutting
     * down, the cluster state cannot be read, or waiting for it to become readable timed out (a retryable block, such
     * as the state not yet recovered, is reported as a timeout wrapping the block). Anything else, the security
     * filter's and the interceptor's rejections included, is folded into the one refusal.
     */
    static boolean isUnrelatedToNames(Exception e) {
        return e instanceof TaskCancelledException
            || e instanceof EsRejectedExecutionException
            || e instanceof NodeClosedException
            || e instanceof ClusterBlockException
            || e instanceof ElasticsearchTimeoutException;
    }

    /**
     * A name the resolve action can be asked about as-is: non-empty, not an exclusion, no comma, no colon (a remote
     * prefix or a selector), and nothing the authorization layer would read as a pattern. Datasets are registered
     * under plain index-style names, so anything else cannot be registered and is refused without a round-trip.
     * <p>
     * The pattern half is the security layer's notion, not {@code Regex.isSimpleMatchPattern}: {@code StringMatcher}
     * routes {@code *}, {@code ?} and a leading {@code /} to an automaton, so a name carrying one is matched against
     * a grant as a pattern.
     */
    static boolean isExactLocalName(String name) {
        return name.isEmpty() == false
            && name.charAt(0) != '-'
            && name.charAt(0) != '/'
            && name.indexOf(',') < 0
            && name.indexOf(':') < 0
            && name.indexOf('*') < 0
            && name.indexOf('?') < 0;
    }

    /** The refusal for an external node under a cross-cluster-access authentication or in a remote-cluster compute. */
    public static ElasticsearchStatusException notLocal() {
        return new ElasticsearchStatusException(
            "external datasets are local to the cluster that registered them and cannot be read by a cross-cluster request",
            RestStatus.BAD_REQUEST
        );
    }

    /** The refusal for a data source name the authorize action cannot be asked about as it stands. */
    public static ElasticsearchStatusException notAuthorizable(String dataSource) {
        return new ElasticsearchStatusException(
            "external data source [{}] cannot be read by that name",
            RestStatus.BAD_REQUEST,
            dataSource
        );
    }

    /** The refusal for an external node in a request shape no coordinator sends. */
    public static ElasticsearchStatusException unexpectedShape(String what) {
        return new ElasticsearchStatusException("external datasets cannot be read by {}", RestStatus.BAD_REQUEST, what);
    }
}

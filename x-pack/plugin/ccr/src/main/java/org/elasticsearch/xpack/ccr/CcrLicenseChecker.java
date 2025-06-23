/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.FilterClient;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.ccr.action.CcrRequests;
import org.elasticsearch.xpack.ccr.action.ShardChangesAction;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ccr.CcrConstants;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Encapsulates licensing checking for CCR.
 */
public class CcrLicenseChecker {

    private final BooleanSupplier isCcrAllowed;
    private final BooleanSupplier isAuthAllowed;

    /**
     * Constructs a CCR license checker with the default rule based on the license state for checking if CCR is allowed.
     */
    CcrLicenseChecker(Settings settings) {
        this(() -> CcrConstants.CCR_FEATURE.check(XPackPlugin.getSharedLicenseState()), () -> XPackSettings.SECURITY_ENABLED.get(settings));
    }

    /**
     * Constructs a CCR license checker with the specified boolean suppliers.
     *
     * @param isCcrAllowed  a boolean supplier that should return true if CCR is allowed and false otherwise
     * @param isAuthAllowed a boolean supplier that should return true if security, authentication, and authorization is allowed
     */
    public CcrLicenseChecker(final BooleanSupplier isCcrAllowed, final BooleanSupplier isAuthAllowed) {
        this.isCcrAllowed = Objects.requireNonNull(isCcrAllowed, "isCcrAllowed");
        this.isAuthAllowed = Objects.requireNonNull(isAuthAllowed, "isAuthAllowed");
    }

    /**
     * Returns whether or not CCR is allowed.
     *
     * @return true if CCR is allowed, otherwise false
     */
    public boolean isCcrAllowed() {
        return isCcrAllowed.getAsBoolean();
    }

    /**
     * Fetches the leader index metadata and history UUIDs for leader index shards from the remote cluster.
     * Before fetching the index metadata, the remote cluster is checked for license compatibility with CCR.
     * If the remote cluster is not licensed for CCR, the {@code onFailure} consumer is is invoked. Otherwise,
     * the specified consumer is invoked with the leader index metadata fetched from the remote cluster.
     *
     * @param client        the client
     * @param clusterAlias  the remote cluster alias
     * @param leaderIndex   the name of the leader index
     * @param onFailure     the failure consumer
     * @param consumer      the consumer for supplying the leader index metadata and historyUUIDs of all leader shards
     */
    public void checkRemoteClusterLicenseAndFetchLeaderIndexMetadataAndHistoryUUIDs(
        final Client client,
        final String clusterAlias,
        final String leaderIndex,
        final Consumer<Exception> onFailure,
        final BiConsumer<String[], Tuple<IndexMetadata, DataStream>> consumer
    ) {
        final var remoteClient = client.getRemoteClusterClient(
            clusterAlias,
            client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME),
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
        );
        doCheckRemoteClusterLicenseAndFetchClusterState(
            client,
            clusterAlias,
            remoteClient,
            CcrRequests.metadataRequest(leaderIndex),
            onFailure,
            remoteClusterStateResponse -> {
                ClusterState remoteClusterState = remoteClusterStateResponse.getState();
                final IndexMetadata leaderIndexMetadata = remoteClusterState.getMetadata().getProject().index(leaderIndex);
                if (leaderIndexMetadata == null) {
                    final IndexAbstraction indexAbstraction = remoteClusterState.getMetadata()
                        .getProject()
                        .getIndicesLookup()
                        .get(leaderIndex);
                    final Exception failure;
                    if (indexAbstraction == null) {
                        failure = new IndexNotFoundException(leaderIndex);
                    } else {
                        // provided name may be an alias or data stream and in that case throw a specific error:
                        String message = String.format(
                            Locale.ROOT,
                            "cannot follow [%s], because it is a %s",
                            leaderIndex,
                            indexAbstraction.getType()
                        );
                        failure = new IllegalArgumentException(message);
                    }
                    onFailure.accept(failure);
                    return;
                }
                if (leaderIndexMetadata.getState() == IndexMetadata.State.CLOSE) {
                    onFailure.accept(new IndexClosedException(leaderIndexMetadata.getIndex()));
                    return;
                }
                IndexAbstraction indexAbstraction = remoteClusterState.getMetadata().getProject().getIndicesLookup().get(leaderIndex);
                final DataStream remoteDataStream = indexAbstraction.getParentDataStream() != null
                    ? indexAbstraction.getParentDataStream()
                    : null;
                // Ensure that this leader index is not a failure store index, because they are not yet supported in CCR
                if (remoteDataStream != null && remoteDataStream.isFailureStoreIndex(leaderIndex)) {
                    String message = String.format(Locale.ROOT, "cannot follow [%s], because it is a failure store index", leaderIndex);
                    onFailure.accept(new IllegalArgumentException(message));
                    return;
                }
                hasPrivilegesToFollowIndices(client.threadPool().getThreadContext(), remoteClient, new String[] { leaderIndex }, e -> {
                    if (e == null) {
                        fetchLeaderHistoryUUIDs(
                            remoteClient,
                            leaderIndexMetadata,
                            onFailure,
                            historyUUIDs -> consumer.accept(historyUUIDs, Tuple.tuple(leaderIndexMetadata, remoteDataStream))
                        );
                    } else {
                        onFailure.accept(e);
                    }
                });
            },
            licenseCheck -> indexMetadataNonCompliantRemoteLicense(leaderIndex, licenseCheck),
            e -> indexMetadataUnknownRemoteLicense(leaderIndex, clusterAlias, e)
        );
    }

    /**
     * Fetches the leader cluster state from the remote cluster by the specified cluster state request. Before fetching the cluster state,
     * the remote cluster is checked for license compliance with CCR. If the remote cluster is not licensed for CCR,
     * the {@code onFailure} consumer is invoked. Otherwise, the specified consumer is invoked with the leader cluster state fetched from
     * the remote cluster.
     *
     * @param client                     the client
     * @param clusterAlias               the remote cluster alias
     * @param request                    the cluster state request
     * @param onFailure                  the failure consumer
     * @param leaderClusterStateConsumer the leader cluster state consumer
     */
    public static void checkRemoteClusterLicenseAndFetchClusterState(
        final Client client,
        final String clusterAlias,
        final ClusterStateRequest request,
        final Consumer<Exception> onFailure,
        final Consumer<ClusterStateResponse> leaderClusterStateConsumer
    ) {
        try {
            var remoteClient = systemClient(
                client.threadPool().getThreadContext(),
                client.getRemoteClusterClient(
                    clusterAlias,
                    client.threadPool().executor(Ccr.CCR_THREAD_POOL_NAME),
                    RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
                )
            );
            checkRemoteClusterLicenseAndFetchClusterState(
                client,
                clusterAlias,
                remoteClient,
                request,
                onFailure,
                leaderClusterStateConsumer,
                CcrLicenseChecker::clusterStateNonCompliantRemoteLicense,
                e -> clusterStateUnknownRemoteLicense(clusterAlias, e)
            );
        } catch (Exception e) {
            // client.getRemoteClusterClient(...) can fail with a IllegalArgumentException if remote
            // connection is unknown
            onFailure.accept(e);
        }
    }

    // overridable for testing
    protected void doCheckRemoteClusterLicenseAndFetchClusterState(
        final Client client,
        final String clusterAlias,
        final RemoteClusterClient remoteClient,
        final ClusterStateRequest request,
        final Consumer<Exception> onFailure,
        final Consumer<ClusterStateResponse> leaderClusterStateConsumer,
        final Function<RemoteClusterLicenseChecker.LicenseCheck, ElasticsearchStatusException> nonCompliantLicense,
        final Function<Exception, ElasticsearchStatusException> unknownLicense
    ) {
        checkRemoteClusterLicenseAndFetchClusterState(
            client,
            clusterAlias,
            remoteClient,
            request,
            onFailure,
            leaderClusterStateConsumer,
            nonCompliantLicense,
            unknownLicense
        );
    }

    /**
     * Fetches the leader cluster state from the remote cluster by the specified cluster state request. Before fetching the cluster state,
     * the remote cluster is checked for license compliance with CCR. If the remote cluster is not licensed for CCR,
     * the {@code onFailure} consumer is invoked. Otherwise, the specified consumer is invoked with the leader cluster state fetched from
     * the remote cluster.
     *
     * @param client                     the client
     * @param clusterAlias               the remote cluster alias
     * @param remoteClient               the remote client to use to execute cluster state API
     * @param request                    the cluster state request
     * @param onFailure                  the failure consumer
     * @param leaderClusterStateConsumer the leader cluster state consumer
     * @param nonCompliantLicense        the supplier for when the license state of the remote cluster is non-compliant
     * @param unknownLicense             the supplier for when the license state of the remote cluster is unknown due to failure
     */
    private static void checkRemoteClusterLicenseAndFetchClusterState(
        final Client client,
        final String clusterAlias,
        final RemoteClusterClient remoteClient,
        final ClusterStateRequest request,
        final Consumer<Exception> onFailure,
        final Consumer<ClusterStateResponse> leaderClusterStateConsumer,
        final Function<RemoteClusterLicenseChecker.LicenseCheck, ElasticsearchStatusException> nonCompliantLicense,
        final Function<Exception, ElasticsearchStatusException> unknownLicense
    ) {
        // we have to check the license on the remote cluster
        new RemoteClusterLicenseChecker(client, CcrConstants.CCR_FEATURE).checkRemoteClusterLicenses(
            Collections.singletonList(clusterAlias),
            new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                @Override
                public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck licenseCheck) {
                    if (licenseCheck.isSuccess()) {
                        final ActionListener<ClusterStateResponse> clusterStateListener = ActionListener.wrap(
                            leaderClusterStateConsumer::accept,
                            onFailure
                        );
                        // following an index in remote cluster, so use remote client to fetch leader index metadata
                        remoteClient.execute(ClusterStateAction.REMOTE_TYPE, request, clusterStateListener);
                    } else {
                        onFailure.accept(nonCompliantLicense.apply(licenseCheck));
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    onFailure.accept(unknownLicense.apply(e));
                }

            }
        );
    }

    /**
     * Fetches the history UUIDs for leader index on per shard basis using the specified remoteClient.
     *
     * @param remoteClient                              the remote client
     * @param leaderIndexMetadata                       the leader index metadata
     * @param onFailure                                 the failure consumer
     * @param historyUUIDConsumer                       the leader index history uuid and consumer
     */
    // NOTE: Placed this method here; in order to avoid duplication of logic for fetching history UUIDs
    // in case of following a local or a remote cluster.
    public static void fetchLeaderHistoryUUIDs(
        final RemoteClusterClient remoteClient,
        final IndexMetadata leaderIndexMetadata,
        final Consumer<Exception> onFailure,
        final Consumer<String[]> historyUUIDConsumer
    ) {

        String leaderIndex = leaderIndexMetadata.getIndex().getName();
        CheckedConsumer<IndicesStatsResponse, Exception> indicesStatsHandler = indicesStatsResponse -> {
            IndexStats indexStats = indicesStatsResponse.getIndices().get(leaderIndex);
            if (indexStats == null) {
                onFailure.accept(new IllegalArgumentException("no index stats available for the leader index"));
                return;
            }

            String[] historyUUIDs = new String[leaderIndexMetadata.getNumberOfShards()];
            for (IndexShardStats indexShardStats : indexStats) {
                for (ShardStats shardStats : indexShardStats) {
                    // Ignore replica shards as they may not have yet started and
                    // we just end up overwriting slots in historyUUIDs
                    if (shardStats.getShardRouting().primary() == false) {
                        continue;
                    }

                    CommitStats commitStats = shardStats.getCommitStats();
                    if (commitStats == null) {
                        onFailure.accept(new IllegalArgumentException("leader index's commit stats are missing"));
                        return;
                    }
                    String historyUUID = commitStats.getUserData().get(Engine.HISTORY_UUID_KEY);
                    ShardId shardId = shardStats.getShardRouting().shardId();
                    historyUUIDs[shardId.id()] = historyUUID;
                }
            }
            for (int i = 0; i < historyUUIDs.length; i++) {
                if (historyUUIDs[i] == null) {
                    onFailure.accept(new IllegalArgumentException("no history uuid for [" + leaderIndex + "][" + i + "]"));
                    return;
                }
            }
            historyUUIDConsumer.accept(historyUUIDs);
        };
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        request.indices(leaderIndex);
        remoteClient.execute(IndicesStatsAction.REMOTE_TYPE, request, ActionListener.wrap(indicesStatsHandler, onFailure));
    }

    /**
     * Check if the user executing the current action has privileges to follow the specified indices on the cluster specified by the leader
     * client. The specified callback will be invoked with null if the user has the necessary privileges to follow the specified indices,
     * otherwise the callback will be invoked with an exception outlining the authorization error.
     *
     * @param remoteClient the remote client
     * @param indices      the indices
     * @param handler      the callback
     */
    public void hasPrivilegesToFollowIndices(
        final ThreadContext threadContext,
        final RemoteClusterClient remoteClient,
        final String[] indices,
        final Consumer<Exception> handler
    ) {
        Objects.requireNonNull(remoteClient, "remoteClient");
        Objects.requireNonNull(indices, "indices");
        if (indices.length == 0) {
            throw new IllegalArgumentException("indices must not be empty");
        }
        Objects.requireNonNull(handler, "handler");
        if (isAuthAllowed.getAsBoolean() == false) {
            handler.accept(null);
            return;
        }

        final User user = getUser(threadContext);
        if (user == null) {
            handler.accept(new IllegalStateException("missing or unable to read authentication info on request"));
            return;
        }
        String username = user.principal();

        RoleDescriptor.IndicesPrivileges privileges = RoleDescriptor.IndicesPrivileges.builder()
            .indices(indices)
            .privileges(IndicesStatsAction.NAME, ShardChangesAction.NAME)
            .build();

        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(username);
        request.clusterPrivileges(Strings.EMPTY_ARRAY);
        request.indexPrivileges(privileges);
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        CheckedConsumer<HasPrivilegesResponse, Exception> responseHandler = response -> {
            if (response.isCompleteMatch()) {
                handler.accept(null);
            } else {
                StringBuilder message = new StringBuilder("insufficient privileges to follow");
                message.append(indices.length == 1 ? " index " : " indices ");
                message.append(Arrays.toString(indices));

                ResourcePrivileges resourcePrivileges = response.getIndexPrivileges().iterator().next();
                for (Map.Entry<String, Boolean> entry : resourcePrivileges.getPrivileges().entrySet()) {
                    if (entry.getValue() == false) {
                        message.append(", privilege for action [");
                        message.append(entry.getKey());
                        message.append("] is missing");
                    }
                }

                handler.accept(Exceptions.authorizationError(message.toString()));
            }
        };
        remoteClient.execute(HasPrivilegesAction.REMOTE_TYPE, request, ActionListener.wrap(responseHandler, handler));
    }

    User getUser(ThreadContext threadContext) {
        return new SecurityContext(Settings.EMPTY, threadContext).getUser();
    }

    public static RemoteClusterClient wrapRemoteClusterClient(
        ThreadContext threadContext,
        RemoteClusterClient client,
        Map<String, String> headers,
        ClusterState clusterState
    ) {
        if (headers.isEmpty()) {
            return client;
        } else {
            Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(headers, clusterState);
            if (filteredHeaders.isEmpty()) {
                return client;
            }
            return new RemoteClusterClient() {
                @Override
                public <Request extends ActionRequest, Response extends TransportResponse> void execute(
                    Transport.Connection connection,
                    RemoteClusterActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    ClientHelper.executeWithHeadersAsync(
                        threadContext,
                        filteredHeaders,
                        null,
                        request,
                        listener,
                        (r, l) -> client.execute(connection, action, r, l)
                    );
                }

                @Override
                public <Request extends ActionRequest> void getConnection(Request request, ActionListener<Transport.Connection> listener) {
                    client.getConnection(request, listener);
                }
            };
        }
    }

    public static Client wrapClient(Client client, Map<String, String> headers, ClusterState clusterState) {
        if (headers.isEmpty()) {
            return client;
        } else {
            Map<String, String> filteredHeaders = ClientHelper.getPersistableSafeSecurityHeaders(headers, clusterState);
            if (filteredHeaders.isEmpty()) {
                return client;
            }
            return new FilterClient(client) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    ClientHelper.executeWithHeadersAsync(filteredHeaders, null, client, action, request, listener);
                }
            };
        }
    }

    private static RemoteClusterClient systemClient(ThreadContext threadContext, RemoteClusterClient delegate) {
        return new RemoteClusterClient() {
            @Override
            public <Request extends ActionRequest, Response extends TransportResponse> void execute(
                Transport.Connection connection,
                RemoteClusterActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(false);
                try (var ignore = threadContext.newEmptySystemContext()) {
                    delegate.execute(connection, action, request, new ContextPreservingActionListener<>(supplier, listener));
                }
            }

            @Override
            public <Request extends ActionRequest> void getConnection(Request request, ActionListener<Transport.Connection> listener) {
                delegate.getConnection(request, listener);
            }
        };
    }

    private static ElasticsearchStatusException indexMetadataNonCompliantRemoteLicense(
        final String leaderIndex,
        final RemoteClusterLicenseChecker.LicenseCheck licenseCheck
    ) {
        final String clusterAlias = licenseCheck.remoteClusterLicenseInfo().clusterAlias();
        final String message = String.format(
            Locale.ROOT,
            "can not fetch remote index [%s:%s] metadata as the remote cluster [%s] is not licensed for [ccr]; %s",
            clusterAlias,
            leaderIndex,
            clusterAlias,
            RemoteClusterLicenseChecker.buildErrorMessage(CcrConstants.CCR_FEATURE, licenseCheck.remoteClusterLicenseInfo())
        );
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
    }

    private static ElasticsearchStatusException clusterStateNonCompliantRemoteLicense(
        final RemoteClusterLicenseChecker.LicenseCheck licenseCheck
    ) {
        final String clusterAlias = licenseCheck.remoteClusterLicenseInfo().clusterAlias();
        final String message = String.format(
            Locale.ROOT,
            "can not fetch remote cluster state as the remote cluster [%s] is not licensed for [ccr]; %s",
            clusterAlias,
            RemoteClusterLicenseChecker.buildErrorMessage(CcrConstants.CCR_FEATURE, licenseCheck.remoteClusterLicenseInfo())
        );
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
    }

    private static ElasticsearchStatusException indexMetadataUnknownRemoteLicense(
        final String leaderIndex,
        final String clusterAlias,
        final Exception cause
    ) {
        final String message = String.format(
            Locale.ROOT,
            "can not fetch remote index [%s:%s] metadata as the license state of the remote cluster [%s] could not be determined",
            clusterAlias,
            leaderIndex,
            clusterAlias
        );
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST, cause);
    }

    private static ElasticsearchStatusException clusterStateUnknownRemoteLicense(final String clusterAlias, final Exception cause) {
        final String message = String.format(
            Locale.ROOT,
            "can not fetch remote cluster state as the license state of the remote cluster [%s] could not be determined",
            clusterAlias
        );
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST, cause);
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Encapsulates licensing checking for CCR.
 */
public final class CcrLicenseChecker {

    private final BooleanSupplier isCcrAllowed;

    /**
     * Constructs a CCR license checker with the default rule based on the license state for checking if CCR is allowed.
     */
    CcrLicenseChecker() {
        this(XPackPlugin.getSharedLicenseState()::isCcrAllowed);
    }

    /**
     * Constructs a CCR license checker with the specified boolean supplier.
     *
     * @param isCcrAllowed a boolean supplier that should return true if CCR is allowed and false otherwise
     */
    CcrLicenseChecker(final BooleanSupplier isCcrAllowed) {
        this.isCcrAllowed = Objects.requireNonNull(isCcrAllowed);
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
     * Fetches the leader index metadata from the remote cluster. Before fetching the index metadata, the remote cluster is checked for
     * license compatibility with CCR. If the remote cluster is not licensed for CCR, the {@link ActionListener#onFailure(Exception)} method
     * of the specified listener is invoked. Otherwise, the specified consumer is invoked with the leader index metadata fetched from the
     * remote cluster.
     *
     * @param client                      the client
     * @param clusterAlias                the remote cluster alias
     * @param leaderIndex                 the name of the leader index
     * @param listener                    the listener
     * @param leaderIndexMetadataConsumer the leader index metadata consumer
     * @param <T>                         the type of response the listener is waiting for
     */
    public <T> void checkRemoteClusterLicenseAndFetchLeaderIndexMetadata(
            final Client client,
            final String clusterAlias,
            final String leaderIndex,
            final ActionListener<T> listener,
            final Consumer<IndexMetaData> leaderIndexMetadataConsumer) {
        // we have to check the license on the remote cluster
        new RemoteClusterLicenseChecker(client, XPackLicenseState::isCcrAllowedForOperationMode).checkRemoteClusterLicenses(
                Collections.singletonList(clusterAlias),
                new ActionListener<RemoteClusterLicenseChecker.LicenseCheck>() {

                    @Override
                    public void onResponse(final RemoteClusterLicenseChecker.LicenseCheck licenseCheck) {
                        if (licenseCheck.isSuccess()) {
                            final Client remoteClient = client.getRemoteClusterClient(clusterAlias);
                            final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
                            clusterStateRequest.clear();
                            clusterStateRequest.metaData(true);
                            clusterStateRequest.indices(leaderIndex);
                            final ActionListener<ClusterStateResponse> clusterStateListener = ActionListener.wrap(
                                    r -> {
                                        final ClusterState remoteClusterState = r.getState();
                                        final IndexMetaData leaderIndexMetadata =
                                                remoteClusterState.getMetaData().index(leaderIndex);
                                        leaderIndexMetadataConsumer.accept(leaderIndexMetadata);
                                    },
                                    listener::onFailure);
                            // following an index in remote cluster, so use remote client to fetch leader index metadata
                            remoteClient.admin().cluster().state(clusterStateRequest, clusterStateListener);
                        } else {
                            listener.onFailure(incompatibleRemoteLicense(leaderIndex, licenseCheck));
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        listener.onFailure(unknownRemoteLicense(leaderIndex, clusterAlias, e));
                    }

                });
    }

    private static ElasticsearchStatusException incompatibleRemoteLicense(
            final String leaderIndex, final RemoteClusterLicenseChecker.LicenseCheck licenseCheck) {
        final String clusterAlias = licenseCheck.remoteClusterLicenseInfo().clusterAlias();
        final String message = String.format(
                Locale.ROOT,
                "can not fetch remote index [%s:%s] metadata as the remote cluster [%s] is not licensed for [ccr]; %s",
                clusterAlias,
                leaderIndex,
                clusterAlias,
                RemoteClusterLicenseChecker.buildErrorMessage(
                        "ccr",
                        licenseCheck.remoteClusterLicenseInfo(),
                        RemoteClusterLicenseChecker::isLicensePlatinumOrTrial));
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
    }

    private static ElasticsearchStatusException unknownRemoteLicense(
            final String leaderIndex, final String clusterAlias, final Exception cause) {
        final String message = String.format(
                Locale.ROOT,
                "can not fetch remote index [%s:%s] metadata as the license state of the remote cluster [%s] could not be determined",
                clusterAlias,
                leaderIndex,
                clusterAlias);
        return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST, cause);
    }

}

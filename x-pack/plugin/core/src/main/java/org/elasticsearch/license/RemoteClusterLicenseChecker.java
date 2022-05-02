/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ClusterNameExpressionResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.license.XPackLicenseState.isAllowedByOperationMode;

/**
 * Checks remote clusters for license compatibility with a specified licensed feature.
 */
public final class RemoteClusterLicenseChecker {

    /**
     * Encapsulates the license info of a remote cluster.
     */
    public static final class RemoteClusterLicenseInfo {

        private final String clusterAlias;

        /**
         * The alias of the remote cluster.
         *
         * @return the cluster alias
         */
        public String clusterAlias() {
            return clusterAlias;
        }

        private final XPackInfoResponse.LicenseInfo licenseInfo;

        /**
         * The license info of the remote cluster.
         *
         * @return the license info
         */
        public XPackInfoResponse.LicenseInfo licenseInfo() {
            return licenseInfo;
        }

        RemoteClusterLicenseInfo(final String clusterAlias, final XPackInfoResponse.LicenseInfo licenseInfo) {
            this.clusterAlias = clusterAlias;
            this.licenseInfo = licenseInfo;
        }

    }

    /**
     * Encapsulates a remote cluster license check. The check is either successful if the license of the remote cluster is compatible with
     * the predicate used to check license compatibility, or the check is a failure.
     */
    public static final class LicenseCheck {

        private final RemoteClusterLicenseInfo remoteClusterLicenseInfo;

        /**
         * The remote cluster license info. This method should only be invoked if this instance represents a failing license check.
         *
         * @return the remote cluster license info
         */
        public RemoteClusterLicenseInfo remoteClusterLicenseInfo() {
            assert isSuccess() == false;
            return remoteClusterLicenseInfo;
        }

        private static final LicenseCheck SUCCESS = new LicenseCheck(null);

        /**
         * A successful license check.
         *
         * @return a successful license check instance
         */
        public static LicenseCheck success() {
            return SUCCESS;
        }

        /**
         * Test if this instance represents a successful license check.
         *
         * @return true if this instance represents a successful license check, otherwise false
         */
        public boolean isSuccess() {
            return this == SUCCESS;
        }

        /**
         * Creates a failing license check encapsulating the specified remote cluster license info.
         *
         * @param remoteClusterLicenseInfo the remote cluster license info
         * @return a failing license check
         */
        public static LicenseCheck failure(final RemoteClusterLicenseInfo remoteClusterLicenseInfo) {
            return new LicenseCheck(remoteClusterLicenseInfo);
        }

        private LicenseCheck(final RemoteClusterLicenseInfo remoteClusterLicenseInfo) {
            this.remoteClusterLicenseInfo = remoteClusterLicenseInfo;
        }

    }

    private static final ClusterNameExpressionResolver clusterNameExpressionResolver = new ClusterNameExpressionResolver();
    private final Client client;
    private final LicensedFeature feature;

    /**
     * Constructs a remote cluster license checker with the specified licensed feature for checking license compatibility. The feature
     * does not need to check for the active license state as this is handled by the remote cluster license checker. If the feature
     * is {@code null} a check is only performed on whether the license is active.
     *
     * @param client    the client
     * @param feature   the licensed feature
     */
    public RemoteClusterLicenseChecker(final Client client, @Nullable final LicensedFeature feature) {
        this.client = client;
        this.feature = feature;
    }

    /**
     * Checks the specified clusters for license compatibility. The specified callback will be invoked once if all clusters are
     * license-compatible, otherwise the specified callback will be invoked once on the first cluster that is not license-compatible.
     *
     * @param clusterAliases the cluster aliases to check
     * @param listener       a callback
     */
    public void checkRemoteClusterLicenses(final List<String> clusterAliases, final ActionListener<LicenseCheck> listener) {
        final Iterator<String> clusterAliasesIterator = clusterAliases.iterator();
        if (clusterAliasesIterator.hasNext() == false) {
            listener.onResponse(LicenseCheck.success());
            return;
        }

        final AtomicReference<String> clusterAlias = new AtomicReference<>();

        final ActionListener<XPackInfoResponse> infoListener = new ActionListener<XPackInfoResponse>() {

            @Override
            public void onResponse(final XPackInfoResponse xPackInfoResponse) {
                final XPackInfoResponse.LicenseInfo licenseInfo = xPackInfoResponse.getLicenseInfo();
                if (licenseInfo == null) {
                    listener.onFailure(new ResourceNotFoundException("license info is missing for cluster [" + clusterAlias.get() + "]"));
                    return;
                }

                if (isActive(feature, licenseInfo) == false || isAllowed(feature, licenseInfo) == false) {
                    listener.onResponse(LicenseCheck.failure(new RemoteClusterLicenseInfo(clusterAlias.get(), licenseInfo)));
                    return;
                }

                if (clusterAliasesIterator.hasNext()) {
                    clusterAlias.set(clusterAliasesIterator.next());
                    // recurse to the next cluster
                    remoteClusterLicense(clusterAlias.get(), this);
                } else {
                    listener.onResponse(LicenseCheck.success());
                }
            }

            @Override
            public void onFailure(final Exception e) {
                final String message = "could not determine the license type for cluster [" + clusterAlias.get() + "]";
                listener.onFailure(new ElasticsearchException(message, e));
            }

        };

        // check the license on the first cluster, and then we recursively check licenses on the remaining clusters
        clusterAlias.set(clusterAliasesIterator.next());
        remoteClusterLicense(clusterAlias.get(), infoListener);
    }

    private static boolean isActive(LicensedFeature feature, XPackInfoResponse.LicenseInfo licenseInfo) {
        return feature != null && feature.isNeedsActive() == false || licenseInfo.getStatus() == LicenseStatus.ACTIVE;
    }

    private static boolean isAllowed(LicensedFeature feature, XPackInfoResponse.LicenseInfo licenseInfo) {
        License.OperationMode mode = License.OperationMode.parse(licenseInfo.getMode());
        return feature == null || isAllowedByOperationMode(mode, feature.getMinimumOperationMode());
    }

    private void remoteClusterLicense(final String clusterAlias, final ActionListener<XPackInfoResponse> listener) {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        final ContextPreservingActionListener<XPackInfoResponse> contextPreservingActionListener = new ContextPreservingActionListener<>(
            threadContext.newRestorableContext(false),
            listener
        );
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we stash any context here since this is an internal execution and should not leak any existing context information
            threadContext.markAsSystemContext();

            final XPackInfoRequest request = new XPackInfoRequest();
            request.setCategories(EnumSet.of(XPackInfoRequest.Category.LICENSE));
            try {
                client.getRemoteClusterClient(clusterAlias).execute(XPackInfoAction.INSTANCE, request, contextPreservingActionListener);
            } catch (final Exception e) {
                contextPreservingActionListener.onFailure(e);
            }
        }
    }

    /**
     * Predicate to test if the index name represents the name of a remote index.
     *
     * @param index the index name
     * @return true if the collection of indices contains a remote index, otherwise false
     */
    public static boolean isRemoteIndex(final String index) {
        return index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) != -1;
    }

    /**
     * Predicate to test if the collection of index names contains any that represent the name of a remote index.
     *
     * @param indices the collection of index names
     * @return true if the collection of index names contains a name that represents a remote index, otherwise false
     */
    public static boolean containsRemoteIndex(final Collection<String> indices) {
        return indices.stream().anyMatch(RemoteClusterLicenseChecker::isRemoteIndex);
    }

    /**
     * Filters the collection of index names for names that represent a remote index. Remote index names are of the form
     * {@code cluster_name:index_name}.
     *
     * @param indices the collection of index names
     * @return list of index names that represent remote index names
     */
    public static List<String> remoteIndices(final Collection<String> indices) {
        return indices.stream().filter(RemoteClusterLicenseChecker::isRemoteIndex).collect(Collectors.toList());
    }

    /**
     * Extract the list of remote cluster aliases from the list of index names. Remote index names are of the form
     * {@code cluster_alias:index_name} and the cluster_alias is extracted (and expanded if it is a wildcard) for
     * each index name that represents a remote index.
     *
     * @param remoteClusters the aliases for remote clusters
     * @param indices        the collection of index names
     * @return the remote cluster names
     */
    public static List<String> remoteClusterAliases(final Set<String> remoteClusters, final List<String> indices) {
        return indices.stream()
            .filter(RemoteClusterLicenseChecker::isRemoteIndex)
            .map(index -> index.substring(0, index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR)))
            .distinct()
            .flatMap(clusterExpression -> ClusterNameExpressionResolver.resolveClusterNames(remoteClusters, clusterExpression).stream())
            .distinct()
            .collect(Collectors.toList());
    }

    /**
     * Constructs an error message for license incompatibility.
     *
     * @param feature                  the name of the feature that initiated the remote cluster license check.
     * @param remoteClusterLicenseInfo the remote cluster license info of the cluster that failed the license check
     * @return an error message representing license incompatibility
     */
    public static String buildErrorMessage(final LicensedFeature feature, final RemoteClusterLicenseInfo remoteClusterLicenseInfo) {
        final StringBuilder error = new StringBuilder();
        if (isActive(feature, remoteClusterLicenseInfo.licenseInfo()) == false) {
            error.append(String.format(Locale.ROOT, "the license on cluster [%s] is not active", remoteClusterLicenseInfo.clusterAlias()));
        } else {
            assert isAllowed(feature, remoteClusterLicenseInfo.licenseInfo()) == false
                : "license must be incompatible to build error message";
            final String message = String.format(
                Locale.ROOT,
                "the license mode [%s] on cluster [%s] does not enable [%s]",
                License.OperationMode.parse(remoteClusterLicenseInfo.licenseInfo().getMode()),
                remoteClusterLicenseInfo.clusterAlias(),
                feature.getName()
            );
            error.append(message);
        }

        return error.toString();
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.license;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Checks remote clusters for license compatibility with a specified license predicate.
 */
public final class RemoteClusterLicenseChecker {

    /**
     * Encapsulates the license info of a remote cluster.
     */
    public static class RemoteClusterLicenseInfo {

        private final String clusterName;

        /**
         * The name of the remote cluster.
         *
         * @return the cluster name
         */
        public String clusterName() {
            return clusterName;
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

        RemoteClusterLicenseInfo(final String clusterName, final XPackInfoResponse.LicenseInfo licenseInfo) {
            this.clusterName = clusterName;
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

    private final Client client;
    private final Predicate<XPackInfoResponse.LicenseInfo> licensePredicate;

    /**
     * Constructs a remote license checker with the specified license predicate for checking license compatibility. The predicate does not
     * need to check for the active license state as this is handled by the remote cluster license checker.
     *
     * @param client           the client
     * @param licensePredicate the license predicate
     */
    public RemoteClusterLicenseChecker(final Client client, final Predicate<XPackInfoResponse.LicenseInfo> licensePredicate) {
        this.client = client;
        this.licensePredicate = licensePredicate;
    }

    public static boolean isLicensePlatinumOrTrial(final XPackInfoResponse.LicenseInfo licenseInfo) {
        final License.OperationMode mode = License.OperationMode.resolve(licenseInfo.getMode());
        return mode == License.OperationMode.PLATINUM || mode == License.OperationMode.TRIAL;
    }

    /**
     * Checks the specified clusters for license compatibility. The specified callback will be invoked once if all clusters are
     * license-compatible, otherwise the specified callback will be invoked once on the first cluster that is not license-compatible.
     *
     * @param clusterNames the cluster names to check
     * @param listener     a callback
     */
    public void checkRemoteClusterLicenses(final List<String> clusterNames, final ActionListener<LicenseCheck> listener) {
        final Iterator<String> clusterNamesIterator = clusterNames.iterator();
        if (clusterNamesIterator.hasNext() == false) {
            listener.onResponse(LicenseCheck.success());
            return;
        }

        final AtomicReference<String> clusterName = new AtomicReference<>();

        final ActionListener<XPackInfoResponse> infoListener = new ActionListener<XPackInfoResponse>() {

            @Override
            public void onResponse(final XPackInfoResponse xPackInfoResponse) {
                final XPackInfoResponse.LicenseInfo licenseInfo = xPackInfoResponse.getLicenseInfo();
                if (licenseInfo.getStatus() == LicenseStatus.ACTIVE == false || licensePredicate.test(licenseInfo) == false) {
                    listener.onResponse(LicenseCheck.failure(new RemoteClusterLicenseInfo(clusterName.get(), licenseInfo)));
                    return;
                }

                if (clusterNamesIterator.hasNext()) {
                    clusterName.set(clusterNamesIterator.next());
                    remoteClusterLicense(clusterName.get(), this);
                } else {
                    listener.onResponse(LicenseCheck.success());
                }
            }

            @Override
            public void onFailure(final Exception e) {
                final String message = "could not determine the licence type for cluster [" + clusterName.get() + "]";
                listener.onFailure(new ElasticsearchException(message, e));
            }

        };

        // check the license on the first cluster, and then we recursively check licenses on the remaining clusters
        clusterName.set(clusterNamesIterator.next());
        remoteClusterLicense(clusterName.get(), infoListener);
    }

    private void remoteClusterLicense(final String clusterName, final ActionListener<XPackInfoResponse> listener) {
        final Client remoteClusterClient = client.getRemoteClusterClient(clusterName);
        final ThreadContext threadContext = remoteClusterClient.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we stash any context here since this is an internal execution and should not leak any existing context information
            threadContext.markAsSystemContext();

            final XPackInfoRequest request = new XPackInfoRequest();
            request.setCategories(EnumSet.of(XPackInfoRequest.Category.LICENSE));
            remoteClusterClient.execute(XPackInfoAction.INSTANCE, request, listener);
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
    public static boolean containsRemoteIndex(final List<String> indices) {
        return indices.stream().anyMatch(RemoteClusterLicenseChecker::isRemoteIndex);
    }

    /**
     * Filters the collection of index names for names that represent a remote index. Remote index names are of the form
     * {@code cluster_name:index_name}.
     *
     * @param indices the collection of index names
     * @return list of index names that represent remote index names
     */
    public static List<String> remoteIndices(final List<String> indices) {
        return indices.stream().filter(RemoteClusterLicenseChecker::isRemoteIndex).collect(Collectors.toList());
    }

    /**
     * Extract the list of remote cluster names from the list of index names. Remote index names are of the form
     * {@code cluster_name:index_name} and the cluster_name is extracted for each index name that represents a remote index.
     *
     * @param indices the collection of index names
     * @return the remote cluster names
     */
    public static List<String> remoteClusterNames(final List<String> indices) {
        return indices.stream()
                .filter(RemoteClusterLicenseChecker::isRemoteIndex)
                .map(index -> index.substring(0, index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR)))
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
    public static String buildErrorMessage(
            final String feature,
            final RemoteClusterLicenseInfo remoteClusterLicenseInfo,
            final Predicate<XPackInfoResponse.LicenseInfo> predicate) {
        final StringBuilder error = new StringBuilder();
        if (remoteClusterLicenseInfo.licenseInfo().getStatus() != LicenseStatus.ACTIVE) {
            error.append(String.format("The license on cluster [%s] is not active. ", remoteClusterLicenseInfo.clusterName()));
        } else {
            final License.OperationMode mode = License.OperationMode.resolve(remoteClusterLicenseInfo.licenseInfo().getMode());
            if (predicate.test(remoteClusterLicenseInfo.licenseInfo())) {
                throw new IllegalStateException("license must be incompatible to build error message");
            } else {
                final String message = String.format(
                        "The license mode [%s] on cluster [%s] does not enable [%s]. ",
                        mode,
                        remoteClusterLicenseInfo.clusterName(),
                        feature);
                error.append(message);
            }
        }

        error.append(Strings.toString(remoteClusterLicenseInfo.licenseInfo()));
        return error.toString();
    }

}

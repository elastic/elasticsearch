/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.License;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.action.XPackInfoAction;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * ML datafeeds can use cross cluster search to access data in a remote cluster.
 * The remote cluster should be licenced for ML this class performs that check
 * using the _xpack (info) endpoint.
 */
public class MlRemoteLicenseChecker {

    private final Client client;

    public static class RemoteClusterLicenseInfo {
        private final String clusterName;
        private final XPackInfoResponse.LicenseInfo licenseInfo;

        RemoteClusterLicenseInfo(String clusterName, XPackInfoResponse.LicenseInfo licenseInfo) {
            this.clusterName = clusterName;
            this.licenseInfo = licenseInfo;
        }

        public String getClusterName() {
            return clusterName;
        }

        public XPackInfoResponse.LicenseInfo getLicenseInfo() {
            return licenseInfo;
        }
    }

    public class LicenseViolation {
        private final RemoteClusterLicenseInfo licenseInfo;

        private LicenseViolation(@Nullable RemoteClusterLicenseInfo licenseInfo) {
            this.licenseInfo = licenseInfo;
        }

        public boolean isViolated() {
            return licenseInfo != null;
        }

        public RemoteClusterLicenseInfo get() {
            return licenseInfo;
        }
    }

    public MlRemoteLicenseChecker(Client client) {
        this.client = client;
    }

    /**
     * Check each cluster is licensed for ML.
     * This function evaluates lazily and will terminate when the first cluster
     * that is not licensed is found or an error occurs.
     *
     * @param clusterNames List of remote cluster names
     * @param listener Response listener
     */
    public void checkRemoteClusterLicenses(List<String> clusterNames, ActionListener<LicenseViolation> listener) {
        final Iterator<String> itr = clusterNames.iterator();
        if (itr.hasNext() == false) {
            listener.onResponse(new LicenseViolation(null));
            return;
        }

        final AtomicReference<String> clusterName = new AtomicReference<>(itr.next());

        ActionListener<XPackInfoResponse> infoListener = new ActionListener<XPackInfoResponse>() {
            @Override
            public void onResponse(XPackInfoResponse xPackInfoResponse) {
                if (licenseSupportsML(xPackInfoResponse.getLicenseInfo()) == false) {
                    listener.onResponse(new LicenseViolation(
                            new RemoteClusterLicenseInfo(clusterName.get(), xPackInfoResponse.getLicenseInfo())));
                    return;
                }

                if (itr.hasNext()) {
                    clusterName.set(itr.next());
                    remoteClusterLicense(clusterName.get(), this);
                } else {
                    listener.onResponse(new LicenseViolation(null));
                }
            }

            @Override
            public void onFailure(Exception e) {
                String message = "Could not determine the X-Pack licence type for cluster [" + clusterName.get() + "]";
                if (e instanceof ActionNotFoundTransportException) {
                    // This is likely to be because x-pack is not installed in the target cluster
                    message += ". Is X-Pack installed on the target cluster?";
                }
                listener.onFailure(new ElasticsearchException(message, e));
            }
        };

        remoteClusterLicense(clusterName.get(), infoListener);
    }

    private void remoteClusterLicense(String clusterName, ActionListener<XPackInfoResponse> listener) {
        Client remoteClusterClient = client.getRemoteClusterClient(clusterName);
        ThreadContext threadContext = remoteClusterClient.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we stash any context here since this is an internal execution and should not leak any
            // existing context information.
            threadContext.markAsSystemContext();

            XPackInfoRequest request = new XPackInfoRequest();
            request.setCategories(EnumSet.of(XPackInfoRequest.Category.LICENSE));
            remoteClusterClient.execute(XPackInfoAction.INSTANCE, request, listener);
        }
    }

    static boolean licenseSupportsML(XPackInfoResponse.LicenseInfo licenseInfo) {
        License.OperationMode mode = License.OperationMode.resolve(licenseInfo.getMode());
        return licenseInfo.getStatus() == LicenseStatus.ACTIVE &&
                (mode == License.OperationMode.PLATINUM || mode == License.OperationMode.TRIAL);
    }

    public static boolean isRemoteIndex(String index) {
        return index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) != -1;
    }

    public static boolean containsRemoteIndex(List<String> indices) {
        return indices.stream().anyMatch(MlRemoteLicenseChecker::isRemoteIndex);
    }

    /**
     * Get any remote indices used in cross cluster search.
     * Remote indices are of the form {@code cluster_name:index_name}
     * @return List of remote cluster indices
     */
    public static List<String> remoteIndices(List<String> indices) {
        return indices.stream().filter(MlRemoteLicenseChecker::isRemoteIndex).collect(Collectors.toList());
    }

    /**
     * Extract the list of remote cluster names from the list of indices.
     * @param indices List of indices. Remote cluster indices are prefixed
     *                with {@code cluster-name:}
     * @return Every cluster name found in {@code indices}
     */
    public static List<String> remoteClusterNames(List<String> indices) {
        return indices.stream()
                .filter(MlRemoteLicenseChecker::isRemoteIndex)
                .map(index -> index.substring(0, index.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR)))
                .distinct()
                .collect(Collectors.toList());
    }

    public static String buildErrorMessage(RemoteClusterLicenseInfo clusterLicenseInfo) {
        StringBuilder error = new StringBuilder();
        if (clusterLicenseInfo.licenseInfo.getStatus() != LicenseStatus.ACTIVE) {
            error.append("The license on cluster [").append(clusterLicenseInfo.clusterName)
                    .append("] is not active. ");
        } else {
            License.OperationMode mode = License.OperationMode.resolve(clusterLicenseInfo.licenseInfo.getMode());
            if (mode != License.OperationMode.PLATINUM && mode != License.OperationMode.TRIAL) {
                error.append("The license mode [").append(mode)
                        .append("] on cluster [")
                        .append(clusterLicenseInfo.clusterName)
                        .append("] does not enable Machine Learning. ");
            }
        }

        error.append(Strings.toString(clusterLicenseInfo.licenseInfo));
        return error.toString();
    }
}

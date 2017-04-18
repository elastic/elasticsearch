/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.license.XPackLicenseState;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * {@code ClusterAlertHttpResource}s allow the checking, uploading, and deleting of Watches to a remote cluster based on the current license
 * state.
 */
public class ClusterAlertHttpResource extends PublishableHttpResource {

    private static final Logger logger = Loggers.getLogger(ClusterAlertHttpResource.class);

    /**
     * License State is used to determine if we should even be add or delete our watches.
     */
    private final XPackLicenseState licenseState;
    /**
     * The name of the Watch that is sent to the remote cluster.
     */
    private final Supplier<String> watchId;
    /**
     * Provides a fully formed Watch (e.g., no variables that need replaced).
     */
    private final Supplier<String> watch;

    /**
     * Create a new {@link ClusterAlertHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param watchId The name of the watch, which is lazily loaded.
     * @param watch The watch provider.
     */
    public ClusterAlertHttpResource(final String resourceOwnerName,
                                    final XPackLicenseState licenseState,
                                    final Supplier<String> watchId, final Supplier<String> watch) {
        // Watcher does not support master_timeout
        super(resourceOwnerName, null, PublishableHttpResource.NO_BODY_PARAMETERS);

        this.licenseState = Objects.requireNonNull(licenseState);
        this.watchId = Objects.requireNonNull(watchId);
        this.watch = Objects.requireNonNull(watch);
    }

    /**
     * Determine if the current {@linkplain #watchId Watch} exists.
     */
    @Override
    protected CheckResponse doCheck(final RestClient client) {
        // if we should be adding, then we need to check for existence
        if (licenseState.isMonitoringClusterAlertsAllowed()) {
            return simpleCheckForResource(client, logger,
                                          "/_xpack/watcher/watch", watchId.get(), "monitoring cluster alert",
                                          resourceOwnerName, "monitoring cluster");
        }

        // if we should be deleting, then just try to delete it (same level of effort as checking)
        final boolean deleted = deleteResource(client, logger, "/_xpack/watcher/watch", watchId.get(),
                                               "monitoring cluster alert",
                                               resourceOwnerName, "monitoring cluster");

        return deleted ? CheckResponse.EXISTS : CheckResponse.ERROR;
    }

    /**
     * Publish the missing {@linkplain #watchId Watch}.
     */
    @Override
    protected boolean doPublish(final RestClient client) {
        return putResource(client, logger,
                           "/_xpack/watcher/watch", watchId.get(), this::watchToHttpEntity, "monitoring cluster alert",
                           resourceOwnerName, "monitoring cluster");
    }

    /**
     * Create a {@link HttpEntity} for the {@link #watch}.
     *
     * @return Never {@code null}.
     */
    HttpEntity watchToHttpEntity() {
        return new StringEntity(watch.get(), ContentType.APPLICATION_JSON);
    }

}

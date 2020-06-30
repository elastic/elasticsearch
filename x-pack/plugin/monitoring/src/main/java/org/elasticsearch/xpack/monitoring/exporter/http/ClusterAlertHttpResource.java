/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.XPackLicenseState;

import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil.LAST_UPDATED_VERSION;

/**
 * {@code ClusterAlertHttpResource}s allow the checking, uploading, and deleting of Watches to a remote cluster based on the current license
 * state.
 */
public class ClusterAlertHttpResource extends PublishableHttpResource {

    private static final Logger logger = LogManager.getLogger(ClusterAlertHttpResource.class);

    /**
     * Use this to retrieve the version of Cluster Alert in the Watch's JSON response from a request.
     */
    public static final Map<String, String> CLUSTER_ALERT_VERSION_PARAMETERS =
            Collections.singletonMap("filter_path", "metadata.xpack.version_created");

    /**
     * License State is used to determine if we should even be add or delete our watches.
     */
    private final XPackLicenseState licenseState;
    /**
     * The name of the Watch that is sent to the remote cluster.
     */
    private final Supplier<String> watchId;
    /**
     * Provides a fully formed Watch (e.g., no variables that need replaced). If {@code null}, then we are always going to delete this
     * Cluster Alert.
     */
    @Nullable
    private final Supplier<String> watch;

    /**
     * Create a new {@link ClusterAlertHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param watchId The name of the watch, which is lazily loaded.
     * @param watch The watch provider. {@code null} indicates that we should always delete this Watch.
     */
    public ClusterAlertHttpResource(final String resourceOwnerName,
                                    final XPackLicenseState licenseState,
                                    final Supplier<String> watchId,
                                    @Nullable final Supplier<String> watch) {
        // Watcher does not support master_timeout
        super(resourceOwnerName, null, CLUSTER_ALERT_VERSION_PARAMETERS);

        this.licenseState = Objects.requireNonNull(licenseState);
        this.watchId = Objects.requireNonNull(watchId);
        this.watch = watch;
    }

    /**
     * Determine if the current {@linkplain #watchId Watch} exists.
     */
    @Override
    protected void doCheck(final RestClient client, final ActionListener<Boolean> listener) {
        // if we should be adding, then we need to check for existence
        if (isWatchDefined() && licenseState.isAllowed(XPackLicenseState.Feature.MONITORING_CLUSTER_ALERTS)) {
            final CheckedFunction<Response, Boolean, IOException> watchChecker =
                    (response) -> shouldReplaceClusterAlert(response, XContentType.JSON.xContent(), LAST_UPDATED_VERSION);

            checkForResource(client, listener, logger,
                             "/_watcher/watch", watchId.get(), "monitoring cluster alert",
                             resourceOwnerName, "monitoring cluster",
                             GET_EXISTS, GET_DOES_NOT_EXIST,
                             watchChecker, this::alwaysReplaceResource);
        } else {
            // if we should be deleting, then just try to delete it (same level of effort as checking)
            deleteResource(client, listener, logger, "/_watcher/watch", watchId.get(),
                           "monitoring cluster alert",
                           resourceOwnerName, "monitoring cluster");
        }
    }

    /**
     * Publish the missing {@linkplain #watchId Watch}.
     */
    @Override
    protected void doPublish(final RestClient client, final ActionListener<Boolean> listener) {
        putResource(client, listener, logger,
                    "/_watcher/watch", watchId.get(), Collections.emptyMap(), this::watchToHttpEntity, "monitoring cluster alert",
                    resourceOwnerName, "monitoring cluster");
    }

    /**
     * Determine if the {@link #watch} is defined. If not, then we should always delete the watch.
     *
     * @return {@code true} if {@link #watch} is defined (non-{@code null}). Otherwise {@code false}.
     */
    boolean isWatchDefined() {
        return watch != null;
    }

    /**
     * Create a {@link HttpEntity} for the {@link #watch}.
     *
     * @return Never {@code null}.
     */
    HttpEntity watchToHttpEntity() {
        return new StringEntity(watch.get(), ContentType.APPLICATION_JSON);
    }

    /**
     * Determine if the {@code response} contains a Watch whose value
     *
     * <p>
     * This expects a response like:
     * <pre><code>
     * {
     *   "metadata": {
     *     "xpack": {
     *       "version": 6000002
     *     }
     *   }
     * }
     * </code></pre>
     *
     * @param response The filtered response from the Get Watcher API
     * @param xContent The XContent parser to use
     * @param minimumVersion The minimum version allowed without being replaced (expected to be the last updated version).
     * @return {@code true} represents that it should be replaced. {@code false} that it should be left alone.
     * @throws IOException if any issue occurs while parsing the {@code xContent} {@code response}.
     * @throws RuntimeException if the response format is changed.
     */
    boolean shouldReplaceClusterAlert(final Response response, final XContent xContent, final int minimumVersion) throws IOException {
        // no named content used; so EMPTY is fine
        final Map<String, Object> resources = XContentHelper.convertToMap(xContent, response.getEntity().getContent(), false);

        // if it's empty, then there's no version in the response thanks to filter_path
        if (resources.isEmpty() == false) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> metadata = (Map<String, Object>) resources.get("metadata");
            @SuppressWarnings("unchecked")
            final Map<String, Object> xpack = metadata != null ? (Map<String, Object>) metadata.get("xpack") : null;
            final Object version = xpack != null ? xpack.get("version_created") : null;

            // if we don't have it (perhaps more fields were returned), then we need to replace it
            if (version instanceof Number) {
                // the version in the cluster alert is expected to include the alpha/beta/rc codes as well
                return ((Number) version).intValue() < minimumVersion;
            }
        }

        return true;
    }

}

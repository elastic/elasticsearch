/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@code WatcherExistsHttpResource} checks for the availability of Watcher in the remote cluster and, if it is enabled, attempts to
 * execute the {@link MultiHttpResource} assumed to contain Watches to add to the remote cluster.
 */
public class WatcherExistsHttpResource extends PublishableHttpResource {

    private static final Logger logger = LogManager.getLogger(WatcherExistsHttpResource.class);
    /**
     * Use this to avoid getting any JSON response from a request.
     */
    public static final Map<String, String> WATCHER_CHECK_PARAMETERS =
            Collections.singletonMap("filter_path", "features.watcher.available,features.watcher.enabled");
    /**
     * Valid response codes that note explicitly that {@code _xpack} does not exist.
     */
    public static final Set<Integer> XPACK_DOES_NOT_EXIST =
            Sets.newHashSet(RestStatus.NOT_FOUND.getStatus(), RestStatus.BAD_REQUEST.getStatus());

    /**
     * The cluster service allows this check to be limited to only handling <em>elected</em> master nodes
     */
    private final ClusterService clusterService;
    /**
     * Resources that are used if the check passes
     */
    private final MultiHttpResource watches;

    /**
     * Create a {@link WatcherExistsHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param watches The Watches to create if Watcher is available and enabled.
     */
    public WatcherExistsHttpResource(final String resourceOwnerName, final ClusterService clusterService, final MultiHttpResource watches) {
        // _xpack does not support master_timeout
        super(resourceOwnerName, null, WATCHER_CHECK_PARAMETERS);

        this.clusterService = Objects.requireNonNull(clusterService);
        this.watches = Objects.requireNonNull(watches);
    }

    /**
     * Get the Watch resources that are managed by this resource.
     *
     * @return Never {@code null}.
     */
    public MultiHttpResource getWatches() {
        return watches;
    }

    /**
     * Determine if X-Pack is installed and, if so, if Watcher is both available <em>and</em> enabled so that it can be used.
     * <p>
     * If it is not both available and enabled, then we mark that it {@code EXISTS} so that no follow-on work is performed relative to
     * Watcher. We do the same thing if the current node is not the elected master node.
     */
    @Override
    protected void doCheck(final RestClient client, final ActionListener<Boolean> listener) {
        // only the master manages watches
        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            checkXPackForWatcher(client, listener);
        } else {
            // not the elected master
            listener.onResponse(true);
        }
    }

    /**
     * Reach out to the remote cluster to determine the usability of Watcher.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} to <em>skip</em> cluster alert creation. {@code false} to check/create them.
     */
    private void checkXPackForWatcher(final RestClient client, final ActionListener<Boolean> listener) {
        final CheckedFunction<Response, Boolean, IOException> responseChecker =
            (response) -> canUseWatcher(response, XContentType.JSON.xContent());
        // use DNE to pretend that we're all set; it means that Watcher is unusable
        final CheckedFunction<Response, Boolean, IOException> doesNotExistChecker = (response) -> false;

        checkForResource(client, listener, logger,
                         "", "_xpack", "watcher check",
                         resourceOwnerName, "monitoring cluster",
                         GET_EXISTS, Sets.newHashSet(RestStatus.NOT_FOUND.getStatus(), RestStatus.BAD_REQUEST.getStatus()),
                         responseChecker, doesNotExistChecker);
    }

    /**
     * Determine if Watcher exists ({@code EXISTS}) or does not exist ({@code DOES_NOT_EXIST}).
     *
     * @param response The filtered response from the _xpack info API
     * @param xContent The XContent parser to use
     * @return {@code true} represents it can be used. {@code false} that it cannot be used.
     * @throws IOException if any issue occurs while parsing the {@code xContent} {@code response}.
     * @throws RuntimeException if the response format is changed.
     */
    private boolean canUseWatcher(final Response response, final XContent xContent) throws IOException {
        // no named content used; so EMPTY is fine
        final Map<String, Object> xpackInfo = XContentHelper.convertToMap(xContent, response.getEntity().getContent(), false);

        // if it's empty, then there's no features.watcher response because of filter_path usage
        if (xpackInfo.isEmpty() == false) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> features = (Map<String, Object>) xpackInfo.get("features");
            @SuppressWarnings("unchecked")
            final Map<String, Object> watcher = (Map<String, Object>) features.get("watcher");

            // if Watcher is both available _and_ enabled, then we can use it; either being true is not sufficient
            return Boolean.TRUE == watcher.get("available") && Boolean.TRUE == watcher.get("enabled");
        }

        return false;
    }

    /**
     * Add Watches to the remote cluster.
     */
    @Override
    protected void doPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        watches.checkAndPublish(client, listener);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
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

    private static final Logger logger = Loggers.getLogger(WatcherExistsHttpResource.class);
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
    protected CheckResponse doCheck(final RestClient client) {
        // only the master manages watches
        if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
            return checkXPackForWatcher(client);
        }

        // not the elected master
        return CheckResponse.EXISTS;
    }

    /**
     * Reach out to the remote cluster to determine the usability of Watcher.
     *
     * @param client The REST client to make the request(s).
     * @return Never {@code null}.
     */
    private CheckResponse checkXPackForWatcher(final RestClient client) {
        final Tuple<CheckResponse, Response> response =
                checkForResource(client, logger,
                                 "", "_xpack", "watcher check",
                                 resourceOwnerName, "monitoring cluster",
                                 GET_EXISTS,
                                 Sets.newHashSet(RestStatus.NOT_FOUND.getStatus(), RestStatus.BAD_REQUEST.getStatus()));

        final CheckResponse checkResponse = response.v1();

        // if the response succeeds overall, then we have X-Pack, but we need to inspect to verify Watcher existence
        if (checkResponse == CheckResponse.EXISTS) {
            try {
                if (canUseWatcher(response.v2(), XContentType.JSON.xContent())) {
                    return CheckResponse.DOES_NOT_EXIST;
                }
            } catch (final IOException | RuntimeException e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to parse [_xpack] on the [{}]", resourceOwnerName), e);

                return CheckResponse.ERROR;
            }
        } else if (checkResponse == CheckResponse.ERROR) {
            return CheckResponse.ERROR;
        }

        // we return _exists_ to SKIP the work of putting Watches because WATCHER does not exist, so follow-on work cannot succeed
        return CheckResponse.EXISTS;
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
            if (Boolean.TRUE == watcher.get("available") && Boolean.TRUE == watcher.get("enabled")) {
                return true;
            }
        }

        return false;
    }

    /**
     * Add Watches to the remote cluster.
     */
    @Override
    protected boolean doPublish(final RestClient client) {
        return watches.checkAndPublish(client);
    }
}

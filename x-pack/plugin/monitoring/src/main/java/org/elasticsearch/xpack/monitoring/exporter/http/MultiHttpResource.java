/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.logging.Loggers;

import java.util.Collections;
import java.util.List;

/**
 * {@code MultiHttpResource} serves as a wrapper of a {@link List} of {@link HttpResource}s.
 * <p>
 * By telling the {@code MultiHttpResource} to become dirty, it effectively marks all of its sub-resources dirty as well.
 * <p>
 * Sub-resources should be the sole responsibility of the the {@code MultiHttpResource}; there should not be something using them directly
 * if they are included in a {@code MultiHttpResource}.
 */
public class MultiHttpResource extends HttpResource {

    private static final Logger logger = Loggers.getLogger(MultiHttpResource.class);

    /**
     * Sub-resources that are grouped to simplify notification.
     */
    private final List<HttpResource> resources;

    /**
     * Create a {@link MultiHttpResource}.
     *
     * @param resourceOwnerName The user-recognizable name.
     * @param resources The sub-resources to aggregate.
     */
    public MultiHttpResource(final String resourceOwnerName, final List<? extends HttpResource> resources) {
        super(resourceOwnerName);

        this.resources = Collections.unmodifiableList(resources);
    }

    /**
     * Get the resources that are checked by this {@link MultiHttpResource}.
     *
     * @return Never {@code null}.
     */
    public List<HttpResource> getResources() {
        return resources;
    }

    /**
     * Check and publish all {@linkplain #resources sub-resources}.
     */
    @Override
    protected boolean doCheckAndPublish(RestClient client) {
        logger.trace("checking sub-resources existence and publishing on the [{}]", resourceOwnerName);

        boolean exists = true;

        // short-circuits on the first failure, thus marking the whole thing dirty
        for (final HttpResource resource : resources) {
            if (resource.checkAndPublish(client) == false) {
                exists = false;
                break;
            }
        }

        logger.trace("all sub-resources exist [{}] on the [{}]", exists, resourceOwnerName);

        return exists;
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import java.util.Iterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;

import java.util.Collections;
import java.util.List;

/**
 * {@code MultiHttpResource} serves as a wrapper of a {@link List} of {@link HttpResource}s.
 * <p>
 * By telling the {@code MultiHttpResource} to become dirty, it effectively marks all of its sub-resources dirty as well.
 * <p>
 * Sub-resources should be the sole responsibility of the {@code MultiHttpResource}; there should not be something using them directly
 * if they are included in a {@code MultiHttpResource}.
 */
public class MultiHttpResource extends HttpResource {

    private static final Logger logger = LogManager.getLogger(MultiHttpResource.class);

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

        if (resources.isEmpty()) {
            throw new IllegalArgumentException("[resources] cannot be empty");
        }

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
    protected void doCheckAndPublish(final RestClient client, final ActionListener<Boolean> listener) {
        logger.trace("checking sub-resources existence and publishing on the [{}]", resourceOwnerName);

        final Iterator<HttpResource> iterator = resources.iterator();

        // short-circuits on the first failure, thus marking the whole thing dirty
        iterator.next().checkAndPublish(client, new ActionListener<Boolean>() {

            @Override
            public void onResponse(final Boolean success) {
                // short-circuit on the first failure
                if (success && iterator.hasNext()) {
                    iterator.next().checkAndPublish(client, this);
                } else {
                    logger.trace("all sub-resources exist [{}] on the [{}]", success, resourceOwnerName);

                    listener.onResponse(success);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                logger.trace("all sub-resources exist [false] on the [{}]", resourceOwnerName);

                listener.onFailure(e);
            }

        });
    }

}

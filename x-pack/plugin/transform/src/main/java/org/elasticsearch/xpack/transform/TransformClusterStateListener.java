/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class TransformClusterStateListener implements ClusterStateListener, Supplier<Optional<ClusterState>> {

    private static final Logger logger = LogManager.getLogger(TransformClusterStateListener.class);

    private final Client client;
    private final AtomicBoolean isIndexCreationInProgress = new AtomicBoolean(false);
    private final AtomicReference<ClusterState> clusterState = new AtomicReference<>();

    TransformClusterStateListener(ClusterService clusterService, Client client) {
        this.client = client;
        clusterService.addListener(this);
        logger.debug("Created TransformClusterStateListener");
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        clusterState.set(event.state());
    }

    /**
     * Retrieves the saved cluster state from the most recent update.
     * This differs from {@link ClusterService#state()} in that it will not throw an exception when ClusterState is null.
     */
    @Override
    public Optional<ClusterState> get() {
        return Optional.ofNullable(clusterState.get());
    }
}

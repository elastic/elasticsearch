/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.gateway.GatewayService;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Waits for the cluster state to be recovered before initializing the authorization handler.
 */
public class AuthorizationInitializer implements ClusterStateListener {

    private final ElasticInferenceServiceAuthorizationHandlerV2 authorizationHandler;
    private final AtomicBoolean initializedAuthorization = new AtomicBoolean(false);

    public AuthorizationInitializer(ElasticInferenceServiceAuthorizationHandlerV2 authorizationHandler) {
        this.authorizationHandler = Objects.requireNonNull(authorizationHandler);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }

        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (initializedAuthorization.compareAndSet(false, true)) {
            authorizationHandler.init();
        }
    }
}

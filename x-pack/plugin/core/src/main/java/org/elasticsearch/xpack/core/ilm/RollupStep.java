/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Action;
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Config;

import java.util.Objects;

/**
 * Rolls up index using a {@link RollupV2Config}
 */
public class RollupStep extends AsyncActionStep {
    public static final String NAME = "rollup";

    private final RollupV2Config config;

    public RollupStep(StepKey key, StepKey nextStepKey, Client client, RollupV2Config config) {
        super(key, nextStepKey, client);
        this.config = config;
    }

    @Override
    public boolean isRetryable() {
        return false;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        config.setSourceIndex(indexMetadata.getIndex().getName());
        RollupV2Action.Request request = new RollupV2Action.Request(config);
        getClient().execute(RollupV2Action.INSTANCE, request,
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    public RollupV2Config getConfig() {
        return config;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), config);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RollupStep other = (RollupStep) obj;
        return super.equals(obj) &&
                Objects.equals(config, other.config);
    }
}

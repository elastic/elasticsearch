/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

/**
 * Rolls up index using a {@link RollupActionConfig}
 */
public class RollupStep extends AsyncActionStep {
    public static final String NAME = "rollup";
    public static final String ROLLUP_INDEX_NAME_PREFIX = "rollup-";

    private final RollupActionConfig config;

    public RollupStep(StepKey key, StepKey nextStepKey, Client client, RollupActionConfig config) {
        super(key, nextStepKey, client);
        this.config = config;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentState,
                              ClusterStateObserver observer, ActionListener<Boolean> listener) {
        final String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        final String indexName = indexMetadata.getIndex().getName();
        final LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetadata);
        final String rollupIndexName = lifecycleState.getRollupIndexName();
        if (Strings.hasText(rollupIndexName) == false) {
            listener.onFailure(new IllegalStateException("rollup index name was not generated for policy [" + policyName +
                "] and index [" + indexName + "]"));
            return;
        }
        RollupAction.Request request = new RollupAction.Request(indexName, rollupIndexName, config).masterNodeTimeout(TimeValue.MAX_VALUE);
        // currently RollupAction always acknowledges action was complete when no exceptions are thrown.
        getClient().execute(RollupAction.INSTANCE, request,
            ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    public RollupActionConfig getConfig() {
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
        return super.equals(obj)
            && Objects.equals(config, other.config);
    }
}

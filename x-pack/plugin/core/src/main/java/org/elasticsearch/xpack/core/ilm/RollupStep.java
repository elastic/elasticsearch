/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;

import java.util.Objects;

/**
 * Rolls up index using a {@link RollupActionConfig}
 */
public class RollupStep extends AsyncActionStep {
    public static final String NAME = "rollup";
    public static final String ROLLUP_INDEX_NAME_POSTFIX = "-rollup";

    private final RollupActionConfig config;
    private final String rollupPolicy;

    public RollupStep(StepKey key, StepKey nextStepKey, Client client, RollupActionConfig config, String rollupPolicy) {
        super(key, nextStepKey, client);
        this.config = config;
        this.rollupPolicy = rollupPolicy;
    }

    @Override
    public boolean isRetryable() {
        return false;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        String originalIndex = indexMetadata.getIndex().getName();
        String rollupIndex = originalIndex + ROLLUP_INDEX_NAME_POSTFIX;
        // TODO(talevy): change config to be immutable
        config.setRollupIndex(rollupIndex);
        RollupAction.Request request = new RollupAction.Request(originalIndex, config);
        if (rollupPolicy == null) {
            getClient().execute(RollupAction.INSTANCE, request,
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
        } else {
            Settings setPolicySettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, rollupPolicy).build();
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(rollupIndex)
                .masterNodeTimeout(getMasterTimeout(currentState)).settings(setPolicySettings);
            getClient().execute(RollupAction.INSTANCE, request,
                ActionListener.wrap(rollupResponse -> {
                    getClient().admin().indices().updateSettings(updateSettingsRequest,
                        ActionListener.wrap(settingsResponse -> listener.onResponse(true), listener::onFailure));
                }, listener::onFailure));
        }
    }

    public RollupActionConfig getConfig() {
        return config;
    }

    public String getRollupPolicy() {
        return rollupPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), config, rollupPolicy);
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
            && Objects.equals(config, other.config)
            && Objects.equals(rollupPolicy, other.rollupPolicy);
    }
}

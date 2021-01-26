/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

/**
 * Updates the lifecycle policy for the rollup index for the original/currently managed index
 */
public class UpdateRollupIndexPolicyStep extends AsyncActionStep {
    public static final String NAME = "update-rollup-policy";

    private final String rollupPolicy;

    public UpdateRollupIndexPolicyStep(StepKey key, StepKey nextStepKey, Client client, String rollupPolicy) {
        super(key, nextStepKey, client);
        this.rollupPolicy = rollupPolicy;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public String getRollupPolicy() {
        return rollupPolicy;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        String rollupIndex = RollupStep.getRollupIndexName(indexMetadata.getIndex().getName());
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, rollupPolicy).build();
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(rollupIndex)
            .masterNodeTimeout(getMasterTimeout(currentState))
            .settings(settings);
        getClient().admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                listener.onResponse(true);
            } else {
                listener.onFailure(new ElasticsearchException("settings update not acknowledged in step [" + getKey().toString() + "]"));
            }
        }, listener::onFailure));
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rollupPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateRollupIndexPolicyStep other = (UpdateRollupIndexPolicyStep) obj;
        return super.equals(obj) &&
                Objects.equals(rollupPolicy, other.rollupPolicy);
    }
}

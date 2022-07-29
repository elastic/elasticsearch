/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

/**
 * Updates the settings for an index.
 */
public class UpdateSettingsStep extends AsyncActionStep {
    public static final String NAME = "update-settings";

    private final Settings settings;

    public UpdateSettingsStep(StepKey key, StepKey nextStepKey, Client client, Settings settings) {
        super(key, nextStepKey, client);
        this.settings = settings;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ClusterState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexMetadata.getIndex().getName()).masterNodeTimeout(
            TimeValue.MAX_VALUE
        ).settings(settings);
        getClient().admin()
            .indices()
            .updateSettings(updateSettingsRequest, ActionListener.wrap(response -> listener.onResponse(null), listener::onFailure));
    }

    public Settings getSettings() {
        return settings;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), settings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateSettingsStep other = (UpdateSettingsStep) obj;
        return super.equals(obj) && Objects.equals(settings, other.settings);
    }
}

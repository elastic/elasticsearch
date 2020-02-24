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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

/**
 * Copy the provided settings from the source to the target index.
 * <p>
 * The target index is derived from the source index using the provided prefix.
 * This is useful for actions like shrink or searchable snapshot that create a new index and migrate the ILM execution from the source
 * to the target index.
 */
public class CopySettingsStep extends AsyncActionStep {
    public static final String NAME = "copy-settings";

    private final String[] settingsKeys;
    private final String indexPrefix;

    public CopySettingsStep(StepKey key, StepKey nextStepKey, Client client, String indexPrefix, String... settingsKeys) {
        super(key, nextStepKey, client);
        Objects.requireNonNull(indexPrefix);
        Objects.requireNonNull(settingsKeys);
        this.indexPrefix = indexPrefix;
        this.settingsKeys = settingsKeys;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        String indexName = indexPrefix + indexMetaData.getIndex().getName();

        Settings.Builder settings = Settings.builder();
        for (String key : settingsKeys) {
            String value = indexMetaData.getSettings().get(key);
            settings.put(key, value);
        }

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName)
            .masterNodeTimeout(getMasterTimeout(currentState))
            .settings(settings);
        getClient().admin().indices().updateSettings(updateSettingsRequest,
            ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        CopySettingsStep that = (CopySettingsStep) o;
        return Objects.equals(settingsKeys, that.settingsKeys) &&
            Objects.equals(indexPrefix, that.indexPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), settingsKeys, indexPrefix);
    }
}

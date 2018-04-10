/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Objects;

public class UpdateReplicaSettingsStep extends AsyncActionStep {
    public static final String NAME = "update-replicas";

    private int numberOfReplicas;

    public UpdateReplicaSettingsStep(StepKey key, StepKey nextStepKey, Client client, int numberOfReplicas) {
        super(key, nextStepKey, client);
        this.numberOfReplicas = numberOfReplicas;
    }

    @Override
    public void performAction(Index index, Listener listener) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index.getName())
                .settings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas));
        getClient().admin().indices().updateSettings(updateSettingsRequest,
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberOfReplicas);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UpdateReplicaSettingsStep other = (UpdateReplicaSettingsStep) obj;
        return super.equals(obj) &&
                Objects.equals(numberOfReplicas, other.numberOfReplicas);
    }
}

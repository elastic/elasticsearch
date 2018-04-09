/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import java.util.Objects;

public class UpdateReplicaSettingsStep extends ClusterStateActionStep {
    public static final String NAME = "update-replicas";

    private int numberOfReplicas;

    public UpdateReplicaSettingsStep(StepKey key, StepKey nextStepKey, int numberOfReplicas) {
        super(key, nextStepKey);
        this.numberOfReplicas = numberOfReplicas;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetaData idxMeta = clusterState.metaData().index(index);
        if (idxMeta == null) {
            return clusterState;
        }
        Settings.Builder newSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas);

        return ClusterState.builder(clusterState)
            .metaData(MetaData.builder(clusterState.metaData())
                .updateSettings(newSettings.build(), index.getName())).build();
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

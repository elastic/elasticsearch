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

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;

abstract class AbstractUnfollowIndexStep extends AsyncActionStep {

    AbstractUnfollowIndexStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public final void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState,
                                    ClusterStateObserver observer, ActionListener<Void> listener) {
        String followerIndex = indexMetadata.getIndex().getName();
        Map<String, String> customIndexMetadata = indexMetadata.getCustomData(CCR_METADATA_KEY);
        if (customIndexMetadata == null) {
            listener.onResponse(null);
            return;
        }

        innerPerformAction(followerIndex, currentClusterState, listener);
    }

    abstract void innerPerformAction(String followerIndex, ClusterState currentClusterState, ActionListener<Void> listener);
}

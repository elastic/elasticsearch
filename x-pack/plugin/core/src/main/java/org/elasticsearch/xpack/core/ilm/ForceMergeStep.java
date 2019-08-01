/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Objects;

/**
 * Invokes a force merge on a single index.
 */
public class ForceMergeStep extends AsyncActionStep {
    public static final String NAME = "forcemerge";
    private final int maxNumSegments;

    public ForceMergeStep(StepKey key, StepKey nextStepKey, Client client, int maxNumSegments) {
        super(key, nextStepKey, client);
        this.maxNumSegments = maxNumSegments;
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        ForceMergeRequest request = new ForceMergeRequest(indexMetaData.getIndex().getName());
        request.maxNumSegments(maxNumSegments);
        getClient().admin().indices()
            .forceMerge(request, ActionListener.wrap(response -> listener.onResponse(true),
                listener::onFailure));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxNumSegments);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ForceMergeStep other = (ForceMergeStep) obj;
        return super.equals(obj) &&
            Objects.equals(maxNumSegments, other.maxNumSegments);
    }
}

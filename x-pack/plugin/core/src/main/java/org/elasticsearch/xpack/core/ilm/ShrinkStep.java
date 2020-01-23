/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

/**
 * Shrinks an index, using a prefix prepended to the original index name for the name of the shrunken index.
 */
public class ShrinkStep extends AsyncActionStep {
    public static final String NAME = "shrink";

    private int numberOfShards;
    private String shrunkIndexPrefix;

    public ShrinkStep(StepKey key, StepKey nextStepKey, Client client, int numberOfShards, String shrunkIndexPrefix) {
        super(key, nextStepKey, client);
        this.numberOfShards = numberOfShards;
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        if (lifecycleState.getLifecycleDate() == null) {
            throw new IllegalStateException("source index [" + indexMetaData.getIndex().getName() +
                "] is missing lifecycle date");
        }

        String lifecycle = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetaData.getSettings());

        Settings relevantTargetSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, indexMetaData.getNumberOfReplicas())
            .put(LifecycleSettings.LIFECYCLE_NAME, lifecycle)
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", (String) null) // need to remove the single shard
                                                                                             // allocation so replicas can be allocated
            .build();

        String shrunkenIndexName = shrunkIndexPrefix + indexMetaData.getIndex().getName();
        ResizeRequest resizeRequest = new ResizeRequest(shrunkenIndexName, indexMetaData.getIndex().getName())
            .masterNodeTimeout(getMasterTimeout(currentState));
        resizeRequest.getTargetIndexRequest().settings(relevantTargetSettings);

        getClient().admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(response -> {
            listener.onResponse(response.isAcknowledged());
        }, listener::onFailure));

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberOfShards, shrunkIndexPrefix);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShrinkStep other = (ShrinkStep) obj;
        return super.equals(obj) &&
                Objects.equals(numberOfShards, other.numberOfShards) &&
                Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }

}

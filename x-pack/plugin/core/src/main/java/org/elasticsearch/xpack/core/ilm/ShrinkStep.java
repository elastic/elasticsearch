/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Objects;

/**
 * Shrinks an index, using a prefix prepended to the original index name for the name of the shrunken index.
 */
public class ShrinkStep extends AsyncActionStep {
    public static final String NAME = "shrink";

    private Integer numberOfShards;
    private ByteSizeValue maxSinglePrimarySize;
    private String shrunkIndexPrefix;

    public ShrinkStep(StepKey key, StepKey nextStepKey, Client client, Integer numberOfShards,
                      ByteSizeValue maxSinglePrimarySize, String shrunkIndexPrefix) {
        super(key, nextStepKey, client);
        this.numberOfShards = numberOfShards;
        this.maxSinglePrimarySize = maxSinglePrimarySize;
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    public Integer getNumberOfShards() {
        return numberOfShards;
    }

    public ByteSizeValue getMaxSinglePrimarySize() {
        return maxSinglePrimarySize;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentState, ClusterStateObserver observer, Listener listener) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        if (lifecycleState.getLifecycleDate() == null) {
            throw new IllegalStateException("source index [" + indexMetadata.getIndex().getName() +
                "] is missing lifecycle date");
        }

        String lifecycle = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings());

        Settings.Builder builder = Settings.builder();
        // need to remove the single shard, allocation so replicas can be allocated
        builder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, indexMetadata.getNumberOfReplicas())
            .put(LifecycleSettings.LIFECYCLE_NAME, lifecycle)
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", (String) null);
        if (numberOfShards != null) {
            builder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards);
        }
        Settings relevantTargetSettings = builder.build();

        String shrunkenIndexName = shrunkIndexPrefix + indexMetadata.getIndex().getName();
        ResizeRequest resizeRequest = new ResizeRequest(shrunkenIndexName, indexMetadata.getIndex().getName())
            .masterNodeTimeout(getMasterTimeout(currentState));
        resizeRequest.setMaxSinglePrimarySize(maxSinglePrimarySize);
        resizeRequest.getTargetIndexRequest().settings(relevantTargetSettings);

        getClient().admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(response -> {
            // Hard coding this to true as the resize request was executed and the corresponding cluster change was committed, so the
            // eventual retry will not be able to succeed anymore (shrunk index was created already)
            // The next step in the ShrinkAction will wait for the shrunk index to be created and for the shards to be allocated.
            listener.onResponse(true);
        }, listener::onFailure));

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numberOfShards, maxSinglePrimarySize, shrunkIndexPrefix);
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
                Objects.equals(maxSinglePrimarySize, other.maxSinglePrimarySize) &&
                Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }

}

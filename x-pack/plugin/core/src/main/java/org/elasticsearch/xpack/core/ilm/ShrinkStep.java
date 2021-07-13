/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.getShrinkIndexName;

/**
 * Shrinks an index, using a prefix prepended to the original index name for the name of the shrunken index.
 */
public class ShrinkStep extends AsyncActionStep {
    public static final String NAME = "shrink";
    private static final Logger logger = LogManager.getLogger(ShrinkStep.class);


    private Integer numberOfShards;
    private ByteSizeValue maxPrimaryShardSize;

    public ShrinkStep(StepKey key, StepKey nextStepKey, Client client, Integer numberOfShards,
                      ByteSizeValue maxPrimaryShardSize) {
        super(key, nextStepKey, client);
        this.numberOfShards = numberOfShards;
        this.maxPrimaryShardSize = maxPrimaryShardSize;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public Integer getNumberOfShards() {
        return numberOfShards;
    }

    public ByteSizeValue getMaxPrimaryShardSize() {
        return maxPrimaryShardSize;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentState,
                              ClusterStateObserver observer, ActionListener<Boolean> listener) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        if (lifecycleState.getLifecycleDate() == null) {
            throw new IllegalStateException("source index [" + indexMetadata.getIndex().getName() +
                "] is missing lifecycle date");
        }

        String shrunkenIndexName = getShrinkIndexName(indexMetadata.getIndex().getName(), lifecycleState);
        if (currentState.metadata().index(shrunkenIndexName) != null) {
            logger.warn("skipping [{}] step for index [{}] as part of policy [{}] as the shrunk index [{}] already exists",
                ShrinkStep.NAME, indexMetadata.getIndex().getName(),
                LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings()), shrunkenIndexName);
            listener.onResponse(true);
            return;
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

        ResizeRequest resizeRequest = new ResizeRequest(shrunkenIndexName, indexMetadata.getIndex().getName())
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        resizeRequest.setMaxPrimaryShardSize(maxPrimaryShardSize);
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
        return Objects.hash(super.hashCode(), numberOfShards, maxPrimaryShardSize);
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
                Objects.equals(maxPrimaryShardSize, other.maxPrimaryShardSize);
    }

}

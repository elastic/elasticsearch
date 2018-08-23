/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

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
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
        // if operating on the shrunken index, do nothing

        Long lifecycleDate = LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.get(indexMetaData.getSettings());
        if (lifecycleDate == null) {
            throw new IllegalStateException("source index[" + indexMetaData.getIndex().getName() +
                "] is missing setting[" + LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE);
        }
        String lifecycle = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetaData.getSettings());
        String phase = LifecycleSettings.LIFECYCLE_PHASE_SETTING.get(indexMetaData.getSettings());
        String action = LifecycleSettings.LIFECYCLE_ACTION_SETTING.get(indexMetaData.getSettings());

        Settings relevantTargetSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, indexMetaData.getNumberOfReplicas())
            .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, lifecycleDate)
            .put(LifecycleSettings.LIFECYCLE_NAME, lifecycle)
            .put(LifecycleSettings.LIFECYCLE_PHASE, phase)
            .put(LifecycleSettings.LIFECYCLE_ACTION, action)
            .put(LifecycleSettings.LIFECYCLE_STEP, ShrunkenIndexCheckStep.NAME) // skip source-index steps
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", (String) null) // need to remove the single shard 
                                                                                             // allocation so replicas can be allocated
            .build();
        String shrunkenIndexName = shrunkIndexPrefix + indexMetaData.getIndex().getName();
        ResizeRequest resizeRequest = new ResizeRequest(shrunkenIndexName, indexMetaData.getIndex().getName());
        indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
            resizeRequest.getTargetIndexRequest().alias(new Alias(aliasMetaDataObjectCursor.value.alias()));
        });
        resizeRequest.getTargetIndexRequest().settings(relevantTargetSettings);

        getClient().admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(response -> {
            // TODO(talevy): when is this not acknowledged?
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

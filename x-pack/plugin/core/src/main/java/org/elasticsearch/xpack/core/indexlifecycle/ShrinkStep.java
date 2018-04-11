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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import java.util.Objects;

public class ShrinkStep extends AsyncActionStep {
    public static final String NAME = "shrink";

    private String shrunkenIndexName;

    public ShrinkStep(StepKey key, StepKey nextStepKey, Client client, String shrunkenIndexName) {
        super(key, nextStepKey, client);
        this.shrunkenIndexName = shrunkenIndexName;
    }

    public String getShrunkenIndexName() {
        return shrunkenIndexName;
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, Listener listener) {
        Long lifecycleDate = LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.get(indexMetaData.getSettings());
        if (lifecycleDate == null) {
            throw new IllegalStateException("source index[" + indexMetaData.getIndex().getName() +
                "] is missing setting[" + LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE);
        }

        Settings relevantTargetSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, indexMetaData.getNumberOfShards())
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, indexMetaData.getNumberOfReplicas())
            .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, lifecycleDate)
            .build();
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
        return Objects.hash(super.hashCode(), shrunkenIndexName);
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
        return super.equals(obj) && Objects.equals(shrunkenIndexName, other.shrunkenIndexName);
    }

}

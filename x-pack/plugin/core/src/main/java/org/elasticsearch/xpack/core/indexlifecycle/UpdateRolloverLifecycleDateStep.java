/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;

public class UpdateRolloverLifecycleDateStep extends AsyncActionStep {
    public static final String NAME = "update-rollover-lifecycle-date";

    public UpdateRolloverLifecycleDateStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
        // find the newly created index from the rollover and fetch its index.creation_date
        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());
        if (Strings.isNullOrEmpty(rolloverAlias)) {
            listener.onFailure(new IllegalStateException("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                + "] is not set on index [" + indexMetaData.getIndex().getName() + "]"));
            return;
        }
        RolloverInfo rolloverInfo = indexMetaData.getRolloverInfos().get(rolloverAlias);
        if (rolloverInfo == null) {
            listener.onFailure(new IllegalStateException("index [" + indexMetaData.getIndex().getName() + "] has not rolled over yet"));
            return;
        }
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, rolloverInfo.getTime()).build();
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexMetaData.getIndex().getName()).settings(settings);
        getClient().admin().indices().updateSettings(updateSettingsRequest,
                ActionListener.wrap(response -> listener.onResponse(true), listener::onFailure));
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass() == obj.getClass() && super.equals(obj);
    }
}

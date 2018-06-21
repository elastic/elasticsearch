/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

public class UpdateRolloverLifecycleDateStep extends ClusterStateActionStep {
    public static final String NAME = "update-rollover-lifecycle-date";

    public UpdateRolloverLifecycleDateStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public ClusterState performAction(Index index, ClusterState currentState) {
        IndexMetaData indexMetaData = currentState.metaData().getIndexSafe(index);
        // find the newly created index from the rollover and fetch its index.creation_date
        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());
        if (Strings.isNullOrEmpty(rolloverAlias)) {
            throw new IllegalStateException("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                + "] is not set on index [" + indexMetaData.getIndex().getName() + "]");
        }
        RolloverInfo rolloverInfo = indexMetaData.getRolloverInfos().get(rolloverAlias);
        if (rolloverInfo == null) {
            throw new IllegalStateException("index [" + indexMetaData.getIndex().getName() + "] has not rolled over yet");
        }
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, rolloverInfo.getTime()).build();
        return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
                .updateSettings(settings, indexMetaData.getIndex().getName())).build();
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

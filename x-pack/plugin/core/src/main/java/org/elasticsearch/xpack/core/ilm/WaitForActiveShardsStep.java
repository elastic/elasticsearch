/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.AliasOrIndex.Alias;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;

import java.util.List;

/**
 * After we performed the index rollover we wait for the the configured number of shards for the rolled over index (ie. newly created
 * index) to become available.
 */
public class WaitForActiveShardsStep extends ClusterStateWaitStep {

    public static final String NAME = "wait-for-active-shards";

    WaitForActiveShardsStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData originalIndexMeta = clusterState.metaData().index(index);
        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(originalIndexMeta.getSettings());
        if (Strings.isNullOrEmpty(rolloverAlias)) {
            throw new IllegalStateException("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                + "] is not set on index [" + originalIndexMeta.getIndex().getName() + "]");
        }

        AliasOrIndex aliasOrIndex = clusterState.metaData().getAliasAndIndexLookup().get(rolloverAlias);
        assert aliasOrIndex.isAlias() : rolloverAlias + " must be an alias but it is an index";

        Alias alias = (Alias) aliasOrIndex;
        IndexMetaData aliasWriteIndex = alias.getWriteIndex();
        String rolledIndexName;
        String waitForActiveShardsSettingValue;
        if (aliasWriteIndex != null) {
            rolledIndexName = aliasWriteIndex.getIndex().getName();
            waitForActiveShardsSettingValue = aliasWriteIndex.getSettings().get("index.write.wait_for_active_shards");
        } else {
            // if the rollover was not performed on a write index alias, the alias will be moved to the new index and it will be the only
            // index this alias points to
            List<IndexMetaData> indices = alias.getIndices();
            assert indices.size() == 1 : "when performing rollover on alias with is_write_index = false the alias must point to only " +
                "one index";
            IndexMetaData indexMetaData = indices.get(0);
            rolledIndexName = indexMetaData.getIndex().getName();
            waitForActiveShardsSettingValue = indexMetaData.getSettings().get("index.write.wait_for_active_shards");
        }

        ActiveShardCount activeShardCount = ActiveShardCount.parseString(waitForActiveShardsSettingValue);
        return new Result(activeShardCount.enoughShardsActive(clusterState, rolledIndexName), null);
    }
}

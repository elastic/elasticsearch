package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;

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
        assert aliasOrIndex.isAlias() : "the " + rolloverAlias + " alias must be an alias but it is an index";

        String rolledIndexName = null;
        for (IndexMetaData indexMeta : aliasOrIndex.getIndices()) {
            ImmutableOpenMap<String, AliasMetaData> aliases = indexMeta.getAliases();
            if (aliases != null) {
                AliasMetaData aliasMetaData = aliases.get(rolloverAlias);
                if (aliasMetaData != null && aliasMetaData.writeIndex() != null && aliasMetaData.writeIndex()) {
                    rolledIndexName = indexMeta.getIndex().getName();
                    break;
                }
            }
        }
        assert rolledIndexName != null : "the " + rolloverAlias + " alias must be a write alias on one index";

        int waitForActiveShardsCount = originalIndexMeta.getSettings().getAsInt("index.write.wait_for_active_shards", -2);
        ActiveShardCount activeShardCount = ActiveShardCount.DEFAULT;
        if (waitForActiveShardsCount == -1) {
            activeShardCount = ActiveShardCount.ALL;
        } else if (waitForActiveShardsCount >= 0) {
            activeShardCount = ActiveShardCount.from(waitForActiveShardsCount);
        }
        return new Result(activeShardCount.enoughShardsActive(clusterState, rolledIndexName), null);
    }
}

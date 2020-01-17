/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.AliasOrIndex.Alias;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * After we performed the index rollover we wait for the the configured number of shards for the rolled over index (ie. newly created
 * index) to become available.
 */
public class WaitForActiveShardsStep extends ClusterStateWaitStep {

    public static final String NAME = "wait-for-active-shards";

    private static final Logger logger = LogManager.getLogger(WaitForActiveShardsStep.class);

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

        if (originalIndexMeta == null) {
            // Index must have been since deleted
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return new Result(false, null);
        }

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(originalIndexMeta.getSettings());
        if (indexingComplete) {
            logger.trace(originalIndexMeta.getIndex() + " has lifecycle complete set, skipping " + WaitForActiveShardsStep.NAME);
            return new Result(true, null);
        }

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
            List<IndexMetaData> indices = alias.getIndices();
            int maxIndexCounter = -1;
            IndexMetaData rolledIndexMeta = null;
            for (IndexMetaData indexMetaData : indices) {
                int indexNameCounter = parseIndexNameCounter(indexMetaData.getIndex().getName());
                if (maxIndexCounter < indexNameCounter) {
                    maxIndexCounter = indexNameCounter;
                    rolledIndexMeta = indexMetaData;
                }
            }
            if (rolledIndexMeta == null) {
                // Index must have been since deleted
                logger.debug("unable to find the index that was rolled over from [{}] as part of lifecycle action [{}]", index.getName(),
                    getKey().getAction());
                return new Result(false, null);
            }
            rolledIndexName = rolledIndexMeta.getIndex().getName();
            waitForActiveShardsSettingValue = rolledIndexMeta.getSettings().get("index.write.wait_for_active_shards");
        }

        ActiveShardCount activeShardCount = ActiveShardCount.parseString(waitForActiveShardsSettingValue);
        boolean enoughShardsActive = activeShardCount.enoughShardsActive(clusterState, rolledIndexName);

        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(rolledIndexName);
        int currentActiveShards = 0;
        for (final IntObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards()) {
            currentActiveShards += shardRouting.value.activeShards().size();
        }
        return new Result(enoughShardsActive, new Info(currentActiveShards, activeShardCount.toString(), enoughShardsActive));
    }

    /**
     * Parses the number from the rolled over index name. It also supports the date-math format (ie. index name is wrapped in &lt; and &gt;)
     * <p>
     * Eg.
     * <p>
     * - For "logs-000002" it'll return 2
     * - For "&lt;logs-{now/d}-3&gt;" it'll return 3
     */
    static int parseIndexNameCounter(String indexName) {
        int numberIndex = indexName.lastIndexOf("-");
        assert numberIndex != -1 : "no separator '-' found";
        return Integer.parseInt(indexName.substring(numberIndex + 1, indexName.endsWith(">") ? indexName.length() - 1 :
            indexName.length()));
    }

    public static final class Info implements ToXContentObject {

        private final long currentActiveShardsCount;
        private final String targetActiveShardsCount;
        private final boolean enoughShardsActive;
        private final String message;

        static final ParseField CURRENT_ACTIVE_SHARDS_COUNT = new ParseField("current_active_shards_count");
        static final ParseField TARGET_ACTIVE_SHARDS_COUNT = new ParseField("target_active_shards_count");
        static final ParseField ENOUGH_SHARDS_ACTIVE = new ParseField("enough_shards_active");
        static final ParseField MESSAGE = new ParseField("message");
        static final ConstructingObjectParser<Info, Void> PARSER = new ConstructingObjectParser<>("wait_for_active_shards_step_info",
            a -> new Info((long) a[0], (String) a[1], (boolean) a[2]));

        static {
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), CURRENT_ACTIVE_SHARDS_COUNT);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TARGET_ACTIVE_SHARDS_COUNT);
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENOUGH_SHARDS_ACTIVE);
            PARSER.declareString((i, s) -> {
            }, MESSAGE);
        }

        public Info(long currentActiveShardsCount, String targetActiveShardsCount, boolean enoughShardsActive) {
            this.currentActiveShardsCount = currentActiveShardsCount;
            this.targetActiveShardsCount = targetActiveShardsCount;
            this.enoughShardsActive = enoughShardsActive;

            if (enoughShardsActive) {
                message = "The target of [" + targetActiveShardsCount + "] are active. Don't need to wait anymore.";
            } else {
                message = "Waiting for [" + targetActiveShardsCount + "] shards to become active, but only [" + currentActiveShardsCount +
                    "] are active.";
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.field(CURRENT_ACTIVE_SHARDS_COUNT.getPreferredName(), currentActiveShardsCount);
            builder.field(TARGET_ACTIVE_SHARDS_COUNT.getPreferredName(), targetActiveShardsCount);
            builder.field(ENOUGH_SHARDS_ACTIVE.getPreferredName(), enoughShardsActive);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Info info = (Info) o;
            return currentActiveShardsCount == info.currentActiveShardsCount &&
                enoughShardsActive == info.enoughShardsActive &&
                Objects.equals(targetActiveShardsCount, info.targetActiveShardsCount) &&
                Objects.equals(message, info.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(currentActiveShardsCount, targetActiveShardsCount, enoughShardsActive, message);
        }
    }
}

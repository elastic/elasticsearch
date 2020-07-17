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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.IndexMetadata.parseIndexNameCounter;

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
        Metadata metadata = clusterState.metadata();
        IndexMetadata originalIndexMeta = metadata.index(index);

        if (originalIndexMeta == null) {
            String errorMessage = String.format(Locale.ROOT, "[%s] lifecycle action for index [%s] executed but index no longer exists",
                getKey().getAction(), index.getName());
            // Index must have been since deleted
            logger.debug(errorMessage);
            return new Result(false, new Info(errorMessage));
        }

        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(originalIndexMeta.getSettings());
        if (indexingComplete) {
            String message = String.format(Locale.ROOT, "index [%s] has lifecycle complete set, skipping [%s]",
                originalIndexMeta.getIndex().getName(), WaitForActiveShardsStep.NAME);
            logger.trace(message);
            return new Result(true, new Info(message));
        }

        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index.getName());
        final String rolledIndexName;
        final String waitForActiveShardsSettingValue;
        if (indexAbstraction.getParentDataStream() != null) {
            DataStream dataStream = indexAbstraction.getParentDataStream().getDataStream();
            IndexAbstraction dataStreamAbstraction = metadata.getIndicesLookup().get(dataStream.getName());
            assert dataStreamAbstraction != null : dataStream.getName() + " datastream is not present in the metadata indices lookup";
            IndexMetadata rolledIndexMeta = dataStreamAbstraction.getWriteIndex();
            if (rolledIndexMeta == null) {
                return getErrorResultOnNullMetadata(getKey(), index);
            }
            rolledIndexName = rolledIndexMeta.getIndex().getName();
            waitForActiveShardsSettingValue = rolledIndexMeta.getSettings().get(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey());
        } else {
            String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(originalIndexMeta.getSettings());
            if (Strings.isNullOrEmpty(rolloverAlias)) {
                throw new IllegalStateException("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                    + "] is not set on index [" + originalIndexMeta.getIndex().getName() + "]");
            }

            IndexAbstraction aliasAbstraction = metadata.getIndicesLookup().get(rolloverAlias);
            assert aliasAbstraction.getType() == IndexAbstraction.Type.ALIAS : rolloverAlias + " must be an alias but it is not";

            IndexMetadata aliasWriteIndex = aliasAbstraction.getWriteIndex();
            if (aliasWriteIndex != null) {
                rolledIndexName = aliasWriteIndex.getIndex().getName();
                waitForActiveShardsSettingValue = aliasWriteIndex.getSettings().get(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey());
            } else {
                List<IndexMetadata> indices = aliasAbstraction.getIndices();
                int maxIndexCounter = -1;
                IndexMetadata rolledIndexMeta = null;
                for (IndexMetadata indexMetadata : indices) {
                    int indexNameCounter = parseIndexNameCounter(indexMetadata.getIndex().getName());
                    if (maxIndexCounter < indexNameCounter) {
                        maxIndexCounter = indexNameCounter;
                        rolledIndexMeta = indexMetadata;
                    }
                }
                if (rolledIndexMeta == null) {
                    return getErrorResultOnNullMetadata(getKey(), index);
                }
                rolledIndexName = rolledIndexMeta.getIndex().getName();
                waitForActiveShardsSettingValue = rolledIndexMeta.getSettings().get("index.write.wait_for_active_shards");
            }
        }

        ActiveShardCount activeShardCount = ActiveShardCount.parseString(waitForActiveShardsSettingValue);
        boolean enoughShardsActive = activeShardCount.enoughShardsActive(clusterState, rolledIndexName);

        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(rolledIndexName);
        int currentActiveShards = 0;
        for (final IntObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards()) {
            currentActiveShards += shardRouting.value.activeShards().size();
        }
        return new Result(enoughShardsActive, new ActiveShardsInfo(currentActiveShards, activeShardCount.toString(), enoughShardsActive));
    }

    private static Result getErrorResultOnNullMetadata(StepKey key, Index originalIndex) {
        String errorMessage = String.format(Locale.ROOT,
            "unable to find the index that was rolled over from [%s] as part of lifecycle action [%s]", originalIndex.getName(),
            key.getAction());

        // Index must have been since deleted
        logger.debug(errorMessage);
        return new Result(false, new Info(errorMessage));
    }

    static final class ActiveShardsInfo implements ToXContentObject {

        private final long currentActiveShardsCount;
        private final String targetActiveShardsCount;
        private final boolean enoughShardsActive;
        private final String message;

        static final ParseField CURRENT_ACTIVE_SHARDS_COUNT = new ParseField("current_active_shards_count");
        static final ParseField TARGET_ACTIVE_SHARDS_COUNT = new ParseField("target_active_shards_count");
        static final ParseField ENOUGH_SHARDS_ACTIVE = new ParseField("enough_shards_active");
        static final ParseField MESSAGE = new ParseField("message");

        ActiveShardsInfo(long currentActiveShardsCount, String targetActiveShardsCount, boolean enoughShardsActive) {
            this.currentActiveShardsCount = currentActiveShardsCount;
            this.targetActiveShardsCount = targetActiveShardsCount;
            this.enoughShardsActive = enoughShardsActive;

            if (enoughShardsActive) {
                message = "the target of [" + targetActiveShardsCount + "] are active. Don't need to wait anymore";
            } else {
                message = "waiting for [" + targetActiveShardsCount + "] shards to become active, but only [" + currentActiveShardsCount +
                    "] are active";
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
            ActiveShardsInfo info = (ActiveShardsInfo) o;
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

    static final class Info implements ToXContentObject {

        private final String message;

        static final ParseField MESSAGE = new ParseField("message");

        Info(String message) {
            this.message = message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
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
            return Objects.equals(message, info.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }
    }
}

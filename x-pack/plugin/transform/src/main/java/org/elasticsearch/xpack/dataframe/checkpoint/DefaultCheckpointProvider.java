/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DefaultCheckpointProvider implements CheckpointProvider {

    // threshold when to audit concrete index names, above this threshold we only report the number of changes
    private static final int AUDIT_CONCRETED_SOURCE_INDEX_CHANGES = 10;

    /**
     * Builder for collecting checkpointing information for the purpose of _stats
     */
    private static class DataFrameTransformCheckpointingInfoBuilder {
        private DataFrameIndexerPosition nextCheckpointPosition;
        private DataFrameTransformProgress nextCheckpointProgress;
        private DataFrameTransformCheckpoint lastCheckpoint;
        private DataFrameTransformCheckpoint nextCheckpoint;
        private DataFrameTransformCheckpoint sourceCheckpoint;

        DataFrameTransformCheckpointingInfoBuilder() {
        }

        DataFrameTransformCheckpointingInfo build() {
            if (lastCheckpoint == null) {
                lastCheckpoint = DataFrameTransformCheckpoint.EMPTY;
            }
            if (nextCheckpoint == null) {
                nextCheckpoint = DataFrameTransformCheckpoint.EMPTY;
            }
            if (sourceCheckpoint == null) {
                sourceCheckpoint = DataFrameTransformCheckpoint.EMPTY;
            }

            // checkpointstats requires a non-negative checkpoint number
            long lastCheckpointNumber = lastCheckpoint.getCheckpoint() > 0 ? lastCheckpoint.getCheckpoint() : 0;
            long nextCheckpointNumber = nextCheckpoint.getCheckpoint() > 0 ? nextCheckpoint.getCheckpoint() : 0;

            return new DataFrameTransformCheckpointingInfo(
                new DataFrameTransformCheckpointStats(lastCheckpointNumber, null, null,
                    lastCheckpoint.getTimestamp(), lastCheckpoint.getTimeUpperBound()),
                new DataFrameTransformCheckpointStats(nextCheckpointNumber, nextCheckpointPosition,
                    nextCheckpointProgress, nextCheckpoint.getTimestamp(), nextCheckpoint.getTimeUpperBound()),
                DataFrameTransformCheckpoint.getBehind(lastCheckpoint, sourceCheckpoint));
        }

        public DataFrameTransformCheckpointingInfoBuilder setLastCheckpoint(DataFrameTransformCheckpoint lastCheckpoint) {
            this.lastCheckpoint = lastCheckpoint;
            return this;
        }

        public DataFrameTransformCheckpointingInfoBuilder setNextCheckpoint(DataFrameTransformCheckpoint nextCheckpoint) {
            this.nextCheckpoint = nextCheckpoint;
            return this;
        }

        public DataFrameTransformCheckpointingInfoBuilder setSourceCheckpoint(DataFrameTransformCheckpoint sourceCheckpoint) {
            this.sourceCheckpoint = sourceCheckpoint;
            return this;
        }

        public DataFrameTransformCheckpointingInfoBuilder setNextCheckpointProgress(DataFrameTransformProgress nextCheckpointProgress) {
            this.nextCheckpointProgress = nextCheckpointProgress;
            return this;
        }

        public DataFrameTransformCheckpointingInfoBuilder setNextCheckpointPosition(DataFrameIndexerPosition nextCheckpointPosition) {
            this.nextCheckpointPosition = nextCheckpointPosition;
            return this;
        }
    }

    private static final Logger logger = LogManager.getLogger(DefaultCheckpointProvider.class);

    protected final Client client;
    protected final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    protected final DataFrameAuditor dataFrameAuditor;
    protected final DataFrameTransformConfig transformConfig;

    public DefaultCheckpointProvider(final Client client,
                                     final DataFrameTransformsConfigManager dataFrameTransformsConfigManager,
                                     final DataFrameAuditor dataFrameAuditor,
                                     final DataFrameTransformConfig transformConfig) {
        this.client = client;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.dataFrameAuditor = dataFrameAuditor;
        this.transformConfig = transformConfig;
    }

    @Override
    public void sourceHasChanged(final DataFrameTransformCheckpoint lastCheckpoint, final ActionListener<Boolean> listener) {
        listener.onResponse(false);
    }

    @Override
    public void createNextCheckpoint(final DataFrameTransformCheckpoint lastCheckpoint,
                              final ActionListener<DataFrameTransformCheckpoint> listener) {
        final long timestamp = System.currentTimeMillis();
        final long checkpoint = lastCheckpoint != null ? lastCheckpoint.getCheckpoint() + 1 : 1;

        getIndexCheckpoints(ActionListener.wrap(checkpointsByIndex -> {
            reportSourceIndexChanges(lastCheckpoint != null ? lastCheckpoint.getIndicesCheckpoints().keySet() : Collections.emptySet(),
                                     checkpointsByIndex.keySet());

            listener.onResponse(new DataFrameTransformCheckpoint(transformConfig.getId(), timestamp, checkpoint, checkpointsByIndex, 0L));
        }, listener::onFailure));
    }

    protected void getIndexCheckpoints (ActionListener<Map<String, long[]>> listener) {
     // 1st get index to see the indexes the user has access to
        GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(transformConfig.getSource().getIndex())
            .features(new GetIndexRequest.Feature[0])
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, GetIndexAction.INSTANCE,
                getIndexRequest, ActionListener.wrap(getIndexResponse -> {
                    Set<String> userIndices = getIndexResponse.getIndices() != null
                            ? new HashSet<>(Arrays.asList(getIndexResponse.getIndices()))
                            : Collections.emptySet();
                    // 2nd get stats request
                    ClientHelper.executeAsyncWithOrigin(client,
                        ClientHelper.DATA_FRAME_ORIGIN,
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest()
                            .indices(transformConfig.getSource().getIndex())
                            .clear()
                            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN),
                        ActionListener.wrap(
                            response -> {
                                if (response.getFailedShards() != 0) {
                                    listener.onFailure(
                                        new CheckpointException("Source has [" + response.getFailedShards() + "] failed shards"));
                                    return;
                                }

                                listener.onResponse(extractIndexCheckPoints(response.getShards(), userIndices));
                            },
                            e-> listener.onFailure(new CheckpointException("Failed to create checkpoint", e))
                        ));
                },
                e -> listener.onFailure(new CheckpointException("Failed to create checkpoint", e))
            ));
    }

    static Map<String, long[]> extractIndexCheckPoints(ShardStats[] shards, Set<String> userIndices) {
        Map<String, TreeMap<Integer, Long>> checkpointsByIndex = new TreeMap<>();

        for (ShardStats shard : shards) {
            String indexName = shard.getShardRouting().getIndexName();

            if (userIndices.contains(indexName)) {
                // SeqNoStats could be `null`, assume the global checkpoint to be -1 in this case
                long globalCheckpoint = shard.getSeqNoStats() == null ? -1L : shard.getSeqNoStats().getGlobalCheckpoint();
                if (checkpointsByIndex.containsKey(indexName)) {
                    // we have already seen this index, just check/add shards
                    TreeMap<Integer, Long> checkpoints = checkpointsByIndex.get(indexName);
                    if (checkpoints.containsKey(shard.getShardRouting().getId())) {
                        // there is already a checkpoint entry for this index/shard combination, check if they match
                        if (checkpoints.get(shard.getShardRouting().getId()) != globalCheckpoint) {
                            throw new CheckpointException("Global checkpoints mismatch for index [" + indexName + "] between shards of id ["
                                    + shard.getShardRouting().getId() + "]");
                        }
                    } else {
                        // 1st time we see this shard for this index, add the entry for the shard
                        checkpoints.put(shard.getShardRouting().getId(), globalCheckpoint);
                    }
                } else {
                    // 1st time we see this index, create an entry for the index and add the shard checkpoint
                    checkpointsByIndex.put(indexName, new TreeMap<>());
                    checkpointsByIndex.get(indexName).put(shard.getShardRouting().getId(), globalCheckpoint);
                }
            }
        }

        // checkpoint extraction is done in 2 steps:
        // 1. GetIndexRequest to retrieve the indices the user has access to
        // 2. IndicesStatsRequest to retrieve stats about indices
        // between 1 and 2 indices could get deleted or created
        if (logger.isDebugEnabled()) {
            Set<String> userIndicesClone = new HashSet<>(userIndices);

            userIndicesClone.removeAll(checkpointsByIndex.keySet());
            if (userIndicesClone.isEmpty() == false) {
                logger.debug("Original set of user indices contained more indexes [{}]", userIndicesClone);
            }
        }

        // create the final structure
        Map<String, long[]> checkpointsByIndexReduced = new TreeMap<>();

        checkpointsByIndex.forEach((indexName, checkpoints) -> {
            checkpointsByIndexReduced.put(indexName, checkpoints.values().stream().mapToLong(l -> l).toArray());
        });

        return checkpointsByIndexReduced;
    }

    @Override
    public void getCheckpointingInfo(DataFrameTransformCheckpoint lastCheckpoint,
                                     DataFrameTransformCheckpoint nextCheckpoint,
                                     DataFrameIndexerPosition nextCheckpointPosition,
                                     DataFrameTransformProgress nextCheckpointProgress,
                                     ActionListener<DataFrameTransformCheckpointingInfo> listener) {

        DataFrameTransformCheckpointingInfoBuilder checkpointingInfoBuilder = new DataFrameTransformCheckpointingInfoBuilder();

        checkpointingInfoBuilder.setLastCheckpoint(lastCheckpoint)
            .setNextCheckpoint(nextCheckpoint)
            .setNextCheckpointPosition(nextCheckpointPosition)
            .setNextCheckpointProgress(nextCheckpointProgress);

        long timestamp = System.currentTimeMillis();

        getIndexCheckpoints(ActionListener.wrap(checkpointsByIndex -> {
            checkpointingInfoBuilder.setSourceCheckpoint(
                    new DataFrameTransformCheckpoint(transformConfig.getId(), timestamp, -1L, checkpointsByIndex, 0L));
            listener.onResponse(checkpointingInfoBuilder.build());
        }, listener::onFailure));
    }

    @Override
    public void getCheckpointingInfo(long lastCheckpointNumber, DataFrameIndexerPosition nextCheckpointPosition,
                                     DataFrameTransformProgress nextCheckpointProgress,
                                     ActionListener<DataFrameTransformCheckpointingInfo> listener) {

        DataFrameTransformCheckpointingInfoBuilder checkpointingInfoBuilder = new DataFrameTransformCheckpointingInfoBuilder();

        checkpointingInfoBuilder.setNextCheckpointPosition(nextCheckpointPosition).setNextCheckpointProgress(nextCheckpointProgress);

        long timestamp = System.currentTimeMillis();

        // <3> got the source checkpoint, notify the user
        ActionListener<Map<String, long[]>> checkpointsByIndexListener = ActionListener.wrap(
                checkpointsByIndex -> {
                    checkpointingInfoBuilder.setSourceCheckpoint(
                        new DataFrameTransformCheckpoint(transformConfig.getId(), timestamp, -1L, checkpointsByIndex, 0L));
                    listener.onResponse(checkpointingInfoBuilder.build());
                },
                e -> {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "Failed to retrieve source checkpoint for data frame [{}]", transformConfig.getId()), e);
                    listener.onFailure(new CheckpointException("Failure during source checkpoint info retrieval", e));
                }
            );

        // <2> got the next checkpoint, get the source checkpoint
        ActionListener<DataFrameTransformCheckpoint> nextCheckpointListener = ActionListener.wrap(
                nextCheckpointObj -> {
                    checkpointingInfoBuilder.setNextCheckpoint(nextCheckpointObj);
                    getIndexCheckpoints(checkpointsByIndexListener);
                },
                e -> {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "Failed to retrieve next checkpoint [{}] for data frame [{}]", lastCheckpointNumber + 1,
                            transformConfig.getId()), e);
                    listener.onFailure(new CheckpointException("Failure during next checkpoint info retrieval", e));
                }
            );

        // <1> got last checkpoint, get the next checkpoint
        ActionListener<DataFrameTransformCheckpoint> lastCheckpointListener = ActionListener.wrap(
            lastCheckpointObj -> {
                checkpointingInfoBuilder.lastCheckpoint = lastCheckpointObj;
                dataFrameTransformsConfigManager.getTransformCheckpoint(transformConfig.getId(), lastCheckpointNumber + 1,
                        nextCheckpointListener);
            },
            e -> {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                        "Failed to retrieve last checkpoint [{}] for data frame [{}]", lastCheckpointNumber,
                        transformConfig.getId()), e);
                listener.onFailure(new CheckpointException("Failure during last checkpoint info retrieval", e));
            }
        );

        if (lastCheckpointNumber != 0) {
            dataFrameTransformsConfigManager.getTransformCheckpoint(transformConfig.getId(), lastCheckpointNumber, lastCheckpointListener);
        } else {
            getIndexCheckpoints(checkpointsByIndexListener);
        }
    }

    /**
     * Inspect source changes and report differences
     *
     * @param lastSourceIndexes the set of indexes seen in the previous checkpoint
     * @param newSourceIndexes the set of indexes seen in the new checkpoint
     */
    void reportSourceIndexChanges(final Set<String> lastSourceIndexes, final Set<String> newSourceIndexes) {
        // spam protection: only warn the first time
        if (newSourceIndexes.isEmpty() && lastSourceIndexes.isEmpty() == false) {
            String message = "Source did not resolve to any open indexes";
            logger.warn("{} for transform [{}]", message, transformConfig.getId());
            dataFrameAuditor.warning(transformConfig.getId(), message);
        } else {
            Set<String> removedIndexes = Sets.difference(lastSourceIndexes, newSourceIndexes);
            Set<String> addedIndexes = Sets.difference(newSourceIndexes, lastSourceIndexes);

            if (removedIndexes.size() + addedIndexes.size() > AUDIT_CONCRETED_SOURCE_INDEX_CHANGES) {
                String message = "Source index resolve found more than " + AUDIT_CONCRETED_SOURCE_INDEX_CHANGES + " changes, ["
                        + removedIndexes.size() + "] removed indexes, [" + addedIndexes.size() + "] new indexes";
                logger.debug("{} for transform [{}]", message, transformConfig.getId());
                dataFrameAuditor.info(transformConfig.getId(), message);
            } else if (removedIndexes.size() + addedIndexes.size() > 0) {
                String message = "Source index resolve found changes, removedIndexes: " + removedIndexes + ", new indexes: " + addedIndexes;
                logger.debug("{} for transform [{}]", message, transformConfig.getId());
                dataFrameAuditor.info(transformConfig.getId(), message);
            }
        }
    }

}

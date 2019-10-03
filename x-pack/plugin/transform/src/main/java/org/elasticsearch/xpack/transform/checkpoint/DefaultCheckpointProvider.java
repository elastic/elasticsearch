/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

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
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

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
    private static class TransformCheckpointingInfoBuilder {
        private TransformIndexerPosition nextCheckpointPosition;
        private TransformProgress nextCheckpointProgress;
        private TransformCheckpoint lastCheckpoint;
        private TransformCheckpoint nextCheckpoint;
        private TransformCheckpoint sourceCheckpoint;

        TransformCheckpointingInfoBuilder() {
        }

        TransformCheckpointingInfo build() {
            if (lastCheckpoint == null) {
                lastCheckpoint = TransformCheckpoint.EMPTY;
            }
            if (nextCheckpoint == null) {
                nextCheckpoint = TransformCheckpoint.EMPTY;
            }
            if (sourceCheckpoint == null) {
                sourceCheckpoint = TransformCheckpoint.EMPTY;
            }

            // checkpointstats requires a non-negative checkpoint number
            long lastCheckpointNumber = lastCheckpoint.getCheckpoint() > 0 ? lastCheckpoint.getCheckpoint() : 0;
            long nextCheckpointNumber = nextCheckpoint.getCheckpoint() > 0 ? nextCheckpoint.getCheckpoint() : 0;

            return new TransformCheckpointingInfo(
                new TransformCheckpointStats(lastCheckpointNumber, null, null,
                    lastCheckpoint.getTimestamp(), lastCheckpoint.getTimeUpperBound()),
                new TransformCheckpointStats(nextCheckpointNumber, nextCheckpointPosition,
                    nextCheckpointProgress, nextCheckpoint.getTimestamp(), nextCheckpoint.getTimeUpperBound()),
                TransformCheckpoint.getBehind(lastCheckpoint, sourceCheckpoint));
        }

        public TransformCheckpointingInfoBuilder setLastCheckpoint(TransformCheckpoint lastCheckpoint) {
            this.lastCheckpoint = lastCheckpoint;
            return this;
        }

        public TransformCheckpointingInfoBuilder setNextCheckpoint(TransformCheckpoint nextCheckpoint) {
            this.nextCheckpoint = nextCheckpoint;
            return this;
        }

        public TransformCheckpointingInfoBuilder setSourceCheckpoint(TransformCheckpoint sourceCheckpoint) {
            this.sourceCheckpoint = sourceCheckpoint;
            return this;
        }

        public TransformCheckpointingInfoBuilder setNextCheckpointProgress(TransformProgress nextCheckpointProgress) {
            this.nextCheckpointProgress = nextCheckpointProgress;
            return this;
        }

        public TransformCheckpointingInfoBuilder setNextCheckpointPosition(TransformIndexerPosition nextCheckpointPosition) {
            this.nextCheckpointPosition = nextCheckpointPosition;
            return this;
        }
    }

    private static final Logger logger = LogManager.getLogger(DefaultCheckpointProvider.class);

    protected final Client client;
    protected final TransformConfigManager transformConfigManager;
    protected final TransformAuditor transformAuditor;
    protected final TransformConfig transformConfig;

    public DefaultCheckpointProvider(final Client client,
                                     final TransformConfigManager transformConfigManager,
                                     final TransformAuditor transformAuditor,
                                     final TransformConfig transformConfig) {
        this.client = client;
        this.transformConfigManager = transformConfigManager;
        this.transformAuditor = transformAuditor;
        this.transformConfig = transformConfig;
    }

    @Override
    public void sourceHasChanged(final TransformCheckpoint lastCheckpoint, final ActionListener<Boolean> listener) {
        listener.onResponse(false);
    }

    @Override
    public void createNextCheckpoint(final TransformCheckpoint lastCheckpoint,
                              final ActionListener<TransformCheckpoint> listener) {
        final long timestamp = System.currentTimeMillis();
        final long checkpoint = lastCheckpoint != null ? lastCheckpoint.getCheckpoint() + 1 : 1;

        getIndexCheckpoints(ActionListener.wrap(checkpointsByIndex -> {
            reportSourceIndexChanges(lastCheckpoint != null ? lastCheckpoint.getIndicesCheckpoints().keySet() : Collections.emptySet(),
                                     checkpointsByIndex.keySet());

            listener.onResponse(new TransformCheckpoint(transformConfig.getId(), timestamp, checkpoint, checkpointsByIndex, 0L));
        }, listener::onFailure));
    }

    protected void getIndexCheckpoints (ActionListener<Map<String, long[]>> listener) {
     // 1st get index to see the indexes the user has access to
        GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(transformConfig.getSource().getIndex())
            .features(new GetIndexRequest.Feature[0])
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.TRANSFORM_ORIGIN, client, GetIndexAction.INSTANCE,
                getIndexRequest, ActionListener.wrap(getIndexResponse -> {
                    Set<String> userIndices = getIndexResponse.getIndices() != null
                            ? new HashSet<>(Arrays.asList(getIndexResponse.getIndices()))
                            : Collections.emptySet();
                    // 2nd get stats request
                    ClientHelper.executeAsyncWithOrigin(client,
                        ClientHelper.TRANSFORM_ORIGIN,
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
    public void getCheckpointingInfo(TransformCheckpoint lastCheckpoint,
                                     TransformCheckpoint nextCheckpoint,
                                     TransformIndexerPosition nextCheckpointPosition,
                                     TransformProgress nextCheckpointProgress,
                                     ActionListener<TransformCheckpointingInfo> listener) {

        TransformCheckpointingInfoBuilder checkpointingInfoBuilder = new TransformCheckpointingInfoBuilder();

        checkpointingInfoBuilder.setLastCheckpoint(lastCheckpoint)
            .setNextCheckpoint(nextCheckpoint)
            .setNextCheckpointPosition(nextCheckpointPosition)
            .setNextCheckpointProgress(nextCheckpointProgress);

        long timestamp = System.currentTimeMillis();

        getIndexCheckpoints(ActionListener.wrap(checkpointsByIndex -> {
            checkpointingInfoBuilder.setSourceCheckpoint(
                    new TransformCheckpoint(transformConfig.getId(), timestamp, -1L, checkpointsByIndex, 0L));
            listener.onResponse(checkpointingInfoBuilder.build());
        }, listener::onFailure));
    }

    @Override
    public void getCheckpointingInfo(long lastCheckpointNumber, TransformIndexerPosition nextCheckpointPosition,
                                     TransformProgress nextCheckpointProgress,
                                     ActionListener<TransformCheckpointingInfo> listener) {

        TransformCheckpointingInfoBuilder checkpointingInfoBuilder = new TransformCheckpointingInfoBuilder();

        checkpointingInfoBuilder.setNextCheckpointPosition(nextCheckpointPosition).setNextCheckpointProgress(nextCheckpointProgress);

        long timestamp = System.currentTimeMillis();

        // <3> got the source checkpoint, notify the user
        ActionListener<Map<String, long[]>> checkpointsByIndexListener = ActionListener.wrap(
                checkpointsByIndex -> {
                    checkpointingInfoBuilder.setSourceCheckpoint(
                        new TransformCheckpoint(transformConfig.getId(), timestamp, -1L, checkpointsByIndex, 0L));
                    listener.onResponse(checkpointingInfoBuilder.build());
                },
                e -> {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "Failed to retrieve source checkpoint for transform [{}]", transformConfig.getId()), e);
                    listener.onFailure(new CheckpointException("Failure during source checkpoint info retrieval", e));
                }
            );

        // <2> got the next checkpoint, get the source checkpoint
        ActionListener<TransformCheckpoint> nextCheckpointListener = ActionListener.wrap(
                nextCheckpointObj -> {
                    checkpointingInfoBuilder.setNextCheckpoint(nextCheckpointObj);
                    getIndexCheckpoints(checkpointsByIndexListener);
                },
                e -> {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                            "Failed to retrieve next checkpoint [{}] for transform [{}]", lastCheckpointNumber + 1,
                            transformConfig.getId()), e);
                    listener.onFailure(new CheckpointException("Failure during next checkpoint info retrieval", e));
                }
            );

        // <1> got last checkpoint, get the next checkpoint
        ActionListener<TransformCheckpoint> lastCheckpointListener = ActionListener.wrap(
            lastCheckpointObj -> {
                checkpointingInfoBuilder.lastCheckpoint = lastCheckpointObj;
                transformConfigManager.getTransformCheckpoint(transformConfig.getId(), lastCheckpointNumber + 1,
                        nextCheckpointListener);
            },
            e -> {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                        "Failed to retrieve last checkpoint [{}] for transform [{}]", lastCheckpointNumber,
                        transformConfig.getId()), e);
                listener.onFailure(new CheckpointException("Failure during last checkpoint info retrieval", e));
            }
        );

        if (lastCheckpointNumber != 0) {
            transformConfigManager.getTransformCheckpoint(transformConfig.getId(), lastCheckpointNumber, lastCheckpointListener);
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
            transformAuditor.warning(transformConfig.getId(), message);
        } else {
            Set<String> removedIndexes = Sets.difference(lastSourceIndexes, newSourceIndexes);
            Set<String> addedIndexes = Sets.difference(newSourceIndexes, lastSourceIndexes);

            if (removedIndexes.size() + addedIndexes.size() > AUDIT_CONCRETED_SOURCE_INDEX_CHANGES) {
                String message = "Source index resolve found more than " + AUDIT_CONCRETED_SOURCE_INDEX_CHANGES + " changes, ["
                        + removedIndexes.size() + "] removed indexes, [" + addedIndexes.size() + "] new indexes";
                logger.debug("{} for transform [{}]", message, transformConfig.getId());
                transformAuditor.info(transformConfig.getId(), message);
            } else if (removedIndexes.size() + addedIndexes.size() > 0) {
                String message = "Source index resolve found changes, removedIndexes: " + removedIndexes + ", new indexes: " + addedIndexes;
                logger.debug("{} for transform [{}]", message, transformConfig.getId());
                transformAuditor.info(transformConfig.getId(), message);
            }
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo.TransformCheckpointingInfoBuilder;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.transform.checkpoint.RemoteClusterResolver.ResolvedIndices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;

import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

class DefaultCheckpointProvider implements CheckpointProvider {

    // threshold when to audit concrete index names, above this threshold we only report the number of changes
    private static final int AUDIT_CONCRETED_SOURCE_INDEX_CHANGES = 10;

    private static final Logger logger = LogManager.getLogger(DefaultCheckpointProvider.class);

    protected final Clock clock;
    protected final Client client;
    protected final RemoteClusterResolver remoteClusterResolver;
    protected final TransformConfigManager transformConfigManager;
    protected final TransformAuditor transformAuditor;
    protected final TransformConfig transformConfig;

    DefaultCheckpointProvider(
        final Clock clock,
        final Client client,
        final RemoteClusterResolver remoteClusterResolver,
        final TransformConfigManager transformConfigManager,
        final TransformAuditor transformAuditor,
        final TransformConfig transformConfig
    ) {
        this.clock = clock;
        this.client = client;
        this.remoteClusterResolver = remoteClusterResolver;
        this.transformConfigManager = transformConfigManager;
        this.transformAuditor = transformAuditor;
        this.transformConfig = transformConfig;
    }

    @Override
    public void sourceHasChanged(final TransformCheckpoint lastCheckpoint, final ActionListener<Boolean> listener) {
        listener.onResponse(false);
    }

    @Override
    public void createNextCheckpoint(final TransformCheckpoint lastCheckpoint, final ActionListener<TransformCheckpoint> listener) {
        final long timestamp = clock.millis();
        final long checkpoint = TransformCheckpoint.isNullOrEmpty(lastCheckpoint) ? 1 : lastCheckpoint.getCheckpoint() + 1;

        getIndexCheckpoints(
            ActionListener.wrap(
                checkpointsByIndex -> {
                    reportSourceIndexChanges(
                        TransformCheckpoint.isNullOrEmpty(lastCheckpoint)
                            ? Collections.emptySet()
                            : lastCheckpoint.getIndicesCheckpoints().keySet(),
                        checkpointsByIndex.keySet()
                    );

                    listener.onResponse(new TransformCheckpoint(transformConfig.getId(), timestamp, checkpoint, checkpointsByIndex, 0L));
                },
                listener::onFailure)
        );
    }

    protected void getIndexCheckpoints(ActionListener<Map<String, long[]>> listener) {
        try {
            ResolvedIndices resolvedIndexes = remoteClusterResolver.resolve(transformConfig.getSource().getIndex());
            ActionListener<Map<String, long[]>> groupedListener = listener;

            if (resolvedIndexes.numClusters() > 1) {
                ActionListener<Collection<Map<String, long[]>>> mergeMapsListener = ActionListener.wrap(indexCheckpoints -> {
                    listener.onResponse(
                        indexCheckpoints.stream()
                            .flatMap(m -> m.entrySet().stream())
                            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()))
                    );
                }, listener::onFailure);

                groupedListener = new GroupedActionListener<>(mergeMapsListener, resolvedIndexes.numClusters());
            }

            if (resolvedIndexes.getLocalIndices().isEmpty() == false) {
                getCheckpointsFromOneCluster(
                    client,
                    transformConfig.getHeaders(),
                    resolvedIndexes.getLocalIndices().toArray(new String[0]),
                    RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
                    groupedListener
                );
            }

            for (Map.Entry<String, List<String>> remoteIndex : resolvedIndexes.getRemoteIndicesPerClusterAlias().entrySet()) {
                Client remoteClient = client.getRemoteClusterClient(remoteIndex.getKey());
                getCheckpointsFromOneCluster(
                    remoteClient,
                    transformConfig.getHeaders(),
                    remoteIndex.getValue().toArray(new String[0]),
                    remoteIndex.getKey() + RemoteClusterService.REMOTE_CLUSTER_INDEX_SEPARATOR,
                    groupedListener
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void getCheckpointsFromOneCluster(
        Client client,
        Map<String, String> headers,
        String[] indices,
        String prefix,
        ActionListener<Map<String, long[]>> listener
    ) {
        // 1st get index to see the indexes the user has access to
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(indices)
            .features(new GetIndexRequest.Feature[0])
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            GetIndexAction.INSTANCE,
            getIndexRequest,
            ActionListener.wrap(getIndexResponse -> {
                Set<String> userIndices = getIndexResponse.getIndices() != null
                    ? new HashSet<>(Arrays.asList(getIndexResponse.getIndices()))
                    : Collections.emptySet();
                // 2nd get stats request
                ClientHelper.executeAsyncWithOrigin(
                    client,
                    ClientHelper.TRANSFORM_ORIGIN,
                    IndicesStatsAction.INSTANCE,
                    new IndicesStatsRequest().indices(indices).clear().indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN),
                    ActionListener.wrap(response -> {
                        if (response.getFailedShards() != 0) {
                            for (int i = 0; i < response.getShardFailures().length; ++i) {
                                logger.warn(
                                    new ParameterizedMessage(
                                        "Source has [{}] failed shards, shard failure [{}]",
                                        response.getFailedShards(), i).getFormattedMessage(),
                                    response.getShardFailures()[i]);
                            }
                            listener.onFailure(
                                new CheckpointException(
                                    "Source has [{}] failed shards, first shard failure: {}",
                                    response.getShardFailures()[0],
                                    response.getFailedShards(),
                                    response.getShardFailures()[0].toString()));
                            return;
                        }
                        listener.onResponse(extractIndexCheckPoints(response.getShards(), userIndices, prefix));
                    }, e -> listener.onFailure(new CheckpointException("Failed to create checkpoint", e)))
                );
            }, e -> listener.onFailure(new CheckpointException("Failed to create checkpoint", e)))
        );
    }

    static Map<String, long[]> extractIndexCheckPoints(ShardStats[] shards, Set<String> userIndices, String prefix) {
        Map<String, TreeMap<Integer, Long>> checkpointsByIndex = new TreeMap<>();

        for (ShardStats shard : shards) {
            String indexName = shard.getShardRouting().getIndexName();

            if (userIndices.contains(indexName)) {
                // SeqNoStats could be `null`, assume the global checkpoint to be -1 in this case
                long globalCheckpoint = shard.getSeqNoStats() == null ? -1L : shard.getSeqNoStats().getGlobalCheckpoint();
                String fullIndexName = prefix + indexName;
                if (checkpointsByIndex.containsKey(fullIndexName)) {
                    // we have already seen this index, just check/add shards
                    TreeMap<Integer, Long> checkpoints = checkpointsByIndex.get(fullIndexName);
                    // 1st time we see this shard for this index, add the entry for the shard
                    // or there is already a checkpoint entry for this index/shard combination
                    // but with a higher global checkpoint. This is by design(not a problem) and
                    // we take the higher value
                    if (checkpoints.containsKey(shard.getShardRouting().getId()) == false
                        || checkpoints.get(shard.getShardRouting().getId()) < globalCheckpoint) {
                        checkpoints.put(shard.getShardRouting().getId(), globalCheckpoint);
                    }
                } else {
                    // 1st time we see this index, create an entry for the index and add the shard checkpoint
                    checkpointsByIndex.put(fullIndexName, new TreeMap<>());
                    checkpointsByIndex.get(fullIndexName).put(shard.getShardRouting().getId(), globalCheckpoint);
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

        checkpointsByIndex.forEach(
            (indexName, checkpoints) -> {
                checkpointsByIndexReduced.put(indexName, checkpoints.values().stream().mapToLong(l -> l).toArray());
            }
        );

        return checkpointsByIndexReduced;
    }

    @Override
    public void getCheckpointingInfo(
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        ActionListener<TransformCheckpointingInfoBuilder> listener
    ) {
        TransformCheckpointingInfo.TransformCheckpointingInfoBuilder checkpointingInfoBuilder =
            new TransformCheckpointingInfo.TransformCheckpointingInfoBuilder();

        checkpointingInfoBuilder.setLastCheckpoint(lastCheckpoint)
            .setNextCheckpoint(nextCheckpoint)
            .setNextCheckpointPosition(nextCheckpointPosition)
            .setNextCheckpointProgress(nextCheckpointProgress);

        long timestamp = clock.millis();

        getIndexCheckpoints(ActionListener.wrap(checkpointsByIndex -> {
            TransformCheckpoint sourceCheckpoint = new TransformCheckpoint(transformConfig.getId(), timestamp, -1L, checkpointsByIndex, 0L);
            checkpointingInfoBuilder.setSourceCheckpoint(sourceCheckpoint);
            checkpointingInfoBuilder.setOperationsBehind(TransformCheckpoint.getBehind(lastCheckpoint, sourceCheckpoint));
            listener.onResponse(checkpointingInfoBuilder);
        }, listener::onFailure));
    }

    @Override
    public void getCheckpointingInfo(
        long lastCheckpointNumber,
        TransformIndexerPosition nextCheckpointPosition,
        TransformProgress nextCheckpointProgress,
        ActionListener<TransformCheckpointingInfoBuilder> listener
    ) {

        TransformCheckpointingInfo.TransformCheckpointingInfoBuilder checkpointingInfoBuilder =
            new TransformCheckpointingInfo.TransformCheckpointingInfoBuilder();

        checkpointingInfoBuilder.setNextCheckpointPosition(nextCheckpointPosition).setNextCheckpointProgress(nextCheckpointProgress);
        checkpointingInfoBuilder.setLastCheckpoint(TransformCheckpoint.EMPTY);
        long timestamp = clock.millis();

        // <3> got the source checkpoint, notify the user
        ActionListener<Map<String, long[]>> checkpointsByIndexListener = ActionListener.wrap(checkpointsByIndex -> {
            TransformCheckpoint sourceCheckpoint = new TransformCheckpoint(transformConfig.getId(), timestamp, -1L, checkpointsByIndex, 0L);
            checkpointingInfoBuilder.setSourceCheckpoint(sourceCheckpoint);
            checkpointingInfoBuilder.setOperationsBehind(
                TransformCheckpoint.getBehind(checkpointingInfoBuilder.getLastCheckpoint(), sourceCheckpoint)
            );
            listener.onResponse(checkpointingInfoBuilder);
        }, e -> {
            logger.debug(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "[{}] failed to retrieve source checkpoint for transform",
                    transformConfig.getId()
                ),
                e
            );
            listener.onFailure(new CheckpointException("Failure during source checkpoint info retrieval", e));
        });

        // <2> got the next checkpoint, get the source checkpoint
        ActionListener<TransformCheckpoint> nextCheckpointListener = ActionListener.wrap(nextCheckpointObj -> {
            checkpointingInfoBuilder.setNextCheckpoint(nextCheckpointObj);
            getIndexCheckpoints(checkpointsByIndexListener);
        }, e -> {
            logger.debug(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "[{}] failed to retrieve next checkpoint [{}]",
                    transformConfig.getId(),
                    lastCheckpointNumber + 1

                ),
                e
            );
            listener.onFailure(new CheckpointException("Failure during next checkpoint info retrieval", e));
        });

        // <1> got last checkpoint, get the next checkpoint
        ActionListener<TransformCheckpoint> lastCheckpointListener = ActionListener.wrap(lastCheckpointObj -> {
            checkpointingInfoBuilder.setChangesLastDetectedAt(Instant.ofEpochMilli(lastCheckpointObj.getTimestamp()));
            checkpointingInfoBuilder.setLastCheckpoint(lastCheckpointObj);
            transformConfigManager.getTransformCheckpoint(transformConfig.getId(), lastCheckpointNumber + 1, nextCheckpointListener);
        }, e -> {
            logger.debug(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "[{}] failed to retrieve last checkpoint [{}]",
                    transformConfig.getId(),
                    lastCheckpointNumber
                ),
                e
            );
            listener.onFailure(new CheckpointException("Failure during last checkpoint info retrieval", e));
        });

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
            logger.warn("[{}] {}", transformConfig.getId(), message);
            transformAuditor.warning(transformConfig.getId(), message);
        } else {
            Set<String> removedIndexes = Sets.difference(lastSourceIndexes, newSourceIndexes);
            Set<String> addedIndexes = Sets.difference(newSourceIndexes, lastSourceIndexes);

            if (removedIndexes.size() + addedIndexes.size() > AUDIT_CONCRETED_SOURCE_INDEX_CHANGES) {
                String message = "Source index resolve found more than "
                    + AUDIT_CONCRETED_SOURCE_INDEX_CHANGES
                    + " changes, ["
                    + removedIndexes.size()
                    + "] removed indexes, ["
                    + addedIndexes.size()
                    + "] new indexes";
                logger.debug("[{}] {}", transformConfig.getId(), message);
                transformAuditor.info(transformConfig.getId(), message);
            } else if (removedIndexes.size() + addedIndexes.size() > 0) {
                String message = "Source index resolve found changes, removedIndexes: " + removedIndexes + ", new indexes: " + addedIndexes;
                logger.debug("[{}] {}", transformConfig.getId(), message);
                transformAuditor.info(transformConfig.getId(), message);
            }
        }
    }

}

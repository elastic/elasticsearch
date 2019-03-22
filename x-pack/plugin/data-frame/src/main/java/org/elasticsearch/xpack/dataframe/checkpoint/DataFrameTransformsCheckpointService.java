/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * DataFrameTransform Checkpoint Service
 *
 * Allows checkpointing a source of a data frame transform which includes all relevant checkpoints of the source.
 *
 * This will be used to checkpoint a transform, detect changes, run the transform in continuous mode.
 *
 */
public class DataFrameTransformsCheckpointService {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformsCheckpointService.class);

    private final Client client;

    public DataFrameTransformsCheckpointService(final Client client) {
        this.client = client;
    }

    /**
     * Get an unnumbered checkpoint. These checkpoints are for persistence but comparing state.
     *
     * @param transformConfig the @link{DataFrameTransformConfig}
     * @param listener listener to call after inner request returned
     */
    public void getCheckpoint(DataFrameTransformConfig transformConfig, ActionListener<DataFrameTransformCheckpoint> listener) {
        getCheckpoint(transformConfig, -1L, listener);
    }

    /**
     * Get a checkpoint, used to store a checkpoint.
     *
     * @param transformConfig the @link{DataFrameTransformConfig}
     * @param checkpoint the number of the checkpoint
     * @param listener listener to call after inner request returned
     */
    public void getCheckpoint(DataFrameTransformConfig transformConfig, long checkpoint,
            ActionListener<DataFrameTransformCheckpoint> listener) {
        long timestamp = System.currentTimeMillis();

        // placeholder for time based synchronization
        long timeUpperBound = 0;

        // 1st get index to see the indexes the user has access to
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices(transformConfig.getSource().getIndex());

        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client, GetIndexAction.INSTANCE,
                getIndexRequest, ActionListener.wrap(getIndexResponse -> {
                    Set<String> userIndices = new HashSet<>(Arrays.asList(getIndexResponse.getIndices()));

                    // 2nd get stats request
                    ClientHelper.executeAsyncWithOrigin(client, ClientHelper.DATA_FRAME_ORIGIN, IndicesStatsAction.INSTANCE,
                            new IndicesStatsRequest().indices(transformConfig.getSource().getIndex()), ActionListener.wrap(response -> {
                                if (response.getFailedShards() != 0) {
                                    throw new CheckpointException("Source has [" + response.getFailedShards() + "] failed shards");
                                }

                                Map<String, long[]> checkpointsByIndex = extractIndexCheckPoints(response.getShards(), userIndices);
                                DataFrameTransformCheckpoint checkpointDoc = new DataFrameTransformCheckpoint(transformConfig.getId(),
                                        timestamp, checkpoint, checkpointsByIndex, timeUpperBound);
                                listener.onResponse(checkpointDoc);

                            }, IndicesStatsRequestException -> {
                                throw new CheckpointException("Failed to retrieve indices stats", IndicesStatsRequestException);
                            }));

                }, getIndexException -> {
                    throw new CheckpointException("Failed to retrieve list of indices", getIndexException);
                }));

    }

    static Map<String, long[]> extractIndexCheckPoints(ShardStats[] shards, Set<String> userIndices) {
        Map<String, TreeMap<Integer, Long>> checkpointsByIndex = new TreeMap<>();

        for (ShardStats shard : shards) {
            String indexName = shard.getShardRouting().getIndexName();
            if (userIndices.contains(indexName)) {
                if (checkpointsByIndex.containsKey(indexName)) {
                    // we have already seen this index, just check/add shards
                    TreeMap<Integer, Long> checkpoints = checkpointsByIndex.get(indexName);
                    if (checkpoints.containsKey(shard.getShardRouting().getId())) {
                        // there is already a checkpoint entry for this index/shard combination, check if they match
                        if (checkpoints.get(shard.getShardRouting().getId()) != shard.getSeqNoStats().getGlobalCheckpoint()) {
                            throw new CheckpointException("Global checkpoints mismatch for index [" + indexName + "] between shards of id ["
                                    + shard.getShardRouting().getId() + "]");
                        }
                    } else {
                        // 1st time we see this shard for this index, add the entry for the shard
                        checkpoints.put(shard.getShardRouting().getId(), shard.getSeqNoStats().getGlobalCheckpoint());
                    }
                } else {
                    // 1st time we see this index, create an entry for the index and add the shard checkpoint
                    checkpointsByIndex.put(indexName, new TreeMap<>());
                    checkpointsByIndex.get(indexName).put(shard.getShardRouting().getId(), shard.getSeqNoStats().getGlobalCheckpoint());
                }
            }
        }

        // create the final structure
        Map<String, long[]> checkpointsByIndexReduced = new TreeMap<>();

        checkpointsByIndex.forEach((indexName, checkpoints) -> {
            checkpointsByIndexReduced.put(indexName, checkpoints.values().stream().mapToLong(l -> l).toArray());
        });

        return checkpointsByIndexReduced;
    }

}

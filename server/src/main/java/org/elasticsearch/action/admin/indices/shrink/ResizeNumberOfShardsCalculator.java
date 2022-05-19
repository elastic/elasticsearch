/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;

import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * This class calculates and verifies the number of shards of a target index after a resize operation. It supports three operations SHRINK,
 * SPLIT and CLONE.
 */
public interface ResizeNumberOfShardsCalculator {

    /**
     * Calculates the target number of shards based on the parameters of the request
     * @param numberOfShards requested number of shards or null if it was not provided
     * @param maxPrimaryShardSize requested max primary shard size or null if it was not provided
     * @param sourceMetadata the index metadata of the source index
     * @return the number of shards for the target index
     */
    int calculate(@Nullable Integer numberOfShards, @Nullable ByteSizeValue maxPrimaryShardSize, IndexMetadata sourceMetadata);

    /**
     * Validates the target number of shards based on the operation. For example, in the case of SHRINK it will check if the doc count per
     * shard is within limits and in the other opetations it will ensure we get the right exceptions if the number of shards is wrong or
     * less than etc.
     * @param numberOfShards the number of shards the target index is going to have
     * @param sourceMetadata the index metadata of the source index
     */
    void validate(int numberOfShards, IndexMetadata sourceMetadata);

    class ShrinkShardsCalculator implements ResizeNumberOfShardsCalculator {
        private static final Logger logger = LogManager.getLogger(TransportResizeAction.class);

        private final StoreStats indexStoreStats;
        private final IntFunction<DocsStats> perShardDocStats;

        public ShrinkShardsCalculator(StoreStats indexStoreStats, IntFunction<DocsStats> perShardDocStats) {
            this.indexStoreStats = indexStoreStats;
            this.perShardDocStats = perShardDocStats;
        }

        @Override
        public int calculate(Integer numberOfShards, ByteSizeValue maxPrimaryShardSize, IndexMetadata sourceMetadata) {
            if (numberOfShards != null) {
                if (maxPrimaryShardSize != null) {
                    throw new IllegalArgumentException(
                        "Cannot set both index.number_of_shards and max_primary_shard_size for the target index"
                    );
                } else {
                    return numberOfShards;
                }
            } else if (maxPrimaryShardSize != null) {
                int sourceIndexShardsNum = sourceMetadata.getNumberOfShards();
                long sourceIndexStorageBytes = indexStoreStats.getSizeInBytes();
                long maxPrimaryShardSizeBytes = maxPrimaryShardSize.getBytes();
                long minShardsNum = sourceIndexStorageBytes / maxPrimaryShardSizeBytes;
                if (minShardsNum * maxPrimaryShardSizeBytes < sourceIndexStorageBytes) {
                    minShardsNum = minShardsNum + 1;
                }
                if (minShardsNum > sourceIndexShardsNum) {
                    logger.info(
                        "By setting max_primary_shard_size to [{}], the shrunk index will contain [{}] shards,"
                            + " which will be greater than [{}] shards in the source index [{}],"
                            + " using [{}] for the shard count of the shrunk index",
                        maxPrimaryShardSize.toString(),
                        minShardsNum,
                        sourceIndexShardsNum,
                        sourceMetadata.getIndex().getName(),
                        sourceIndexShardsNum
                    );
                    return sourceIndexShardsNum;
                } else {
                    return calculateAcceptableNumberOfShards(sourceIndexShardsNum, (int) minShardsNum);
                }
            } else {
                return 1;
            }
        }

        @Override
        public void validate(int numberOfShards, IndexMetadata sourceMetadata) {
            for (int i = 0; i < numberOfShards; i++) {
                Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(i, sourceMetadata, numberOfShards);
                long count = 0;
                for (ShardId shardId : shardIds) {
                    DocsStats docsStats = perShardDocStats.apply(shardId.id());
                    if (docsStats != null) {
                        count += docsStats.getCount();
                    }
                    if (count > IndexWriter.MAX_DOCS) {
                        throw new IllegalStateException(
                            "Can't merge index with more than ["
                                + IndexWriter.MAX_DOCS
                                + "] docs - too many documents in shards "
                                + shardIds
                        );
                    }
                }
            }
        }

        // An acceptable number of shards for a shrink action needs to be a factor of the sourceIndexShardsNum. For this reason we get the
        // minimum factor of sourceIndexShardsNum which is greater minShardsNum
        protected static int calculateAcceptableNumberOfShards(final int sourceIndexShardsNum, final int minShardsNum) {
            if (sourceIndexShardsNum <= 0 || minShardsNum <= 0) {
                return 1;
            }
            if (sourceIndexShardsNum % minShardsNum == 0) {
                return minShardsNum;
            }
            int num = (int) Math.floor(Math.sqrt(sourceIndexShardsNum));
            if (minShardsNum >= num) {
                for (int i = num; i >= 1; i--) {
                    if (sourceIndexShardsNum % i == 0 && minShardsNum <= sourceIndexShardsNum / i) {
                        return sourceIndexShardsNum / i;
                    }
                }
            } else {
                for (int i = 1; i <= num; i++) {
                    if (sourceIndexShardsNum % i == 0 && minShardsNum <= i) {
                        return i;
                    }
                }
            }
            return sourceIndexShardsNum;
        }
    }

    class CloneShardsCalculator implements ResizeNumberOfShardsCalculator {

        @Override
        public int calculate(Integer numberOfShards, ByteSizeValue maxPrimaryShardSize, IndexMetadata sourceMetadata) {
            return numberOfShards != null ? numberOfShards : sourceMetadata.getNumberOfShards();
        }

        @Override
        public void validate(int numberOfShards, IndexMetadata sourceMetadata) {
            for (int i = 0; i < numberOfShards; i++) {
                // we just execute this to ensure we get the right exceptions if the number of shards is wrong etc.
                Objects.requireNonNull(IndexMetadata.selectCloneShard(i, sourceMetadata, numberOfShards));
            }
        }
    }

    class SplitShardsCalculator implements ResizeNumberOfShardsCalculator {

        @Override
        public int calculate(Integer numberOfShards, ByteSizeValue maxPrimaryShardSize, IndexMetadata sourceMetadata) {
            assert numberOfShards != null : "split must specify the number of shards explicitly";
            return numberOfShards;
        }

        @Override
        public void validate(int numberOfShards, IndexMetadata sourceMetadata) {
            for (int i = 0; i < numberOfShards; i++) {
                // we just execute this to ensure we get the right exceptions if the number of shards is wrong or less than etc.
                Objects.requireNonNull(IndexMetadata.selectSplitShard(i, sourceMetadata, numberOfShards));
            }
        }
    }
}

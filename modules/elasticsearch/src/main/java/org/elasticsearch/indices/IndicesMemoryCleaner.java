/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.FlushNotAllowedEngineException;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.translog.Translog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import static org.elasticsearch.common.collect.Sets.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndicesMemoryCleaner extends AbstractComponent {

    private final IndicesService indicesService;

    @Inject public IndicesMemoryCleaner(Settings settings, IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;
    }

    public TranslogCleanResult cleanTranslog(int translogNumberOfOperationsThreshold) {
        int totalShards = 0;
        int cleanedShards = 0;
        long cleaned = 0;
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (indexShard.state() != IndexShardState.STARTED) {
                    continue;
                }
                totalShards++;
                Translog translog = ((InternalIndexShard) indexShard).translog();
                if (translog.size() > translogNumberOfOperationsThreshold) {
                    cleanedShards++;
                    cleaned = indexShard.estimateFlushableMemorySize().bytes();
                    indexShard.flush(new Engine.Flush());
                }
            }
        }
        return new TranslogCleanResult(totalShards, cleanedShards, new ByteSizeValue(cleaned, ByteSizeUnit.BYTES));
    }

    public void cacheClearUnreferenced() {
        for (IndexService indexService : indicesService) {
            indexService.cache().clearUnreferenced();
        }
    }

    public void cacheClear() {
        for (IndexService indexService : indicesService) {
            indexService.cache().clear();
        }
    }

    public void fullMemoryClean() {
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                indexShard.flush(new Engine.Flush().full(true));
            }
        }
    }

    public void forceCleanMemory(Set<ShardId> shardsToIgnore) {
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (!shardsToIgnore.contains(indexShard.shardId())) {
                    try {
                        indexShard.flush(new Engine.Flush().full(false));
                    } catch (FlushNotAllowedEngineException e) {
                        // ignore this one, its temporal
                    } catch (IllegalIndexShardStateException e) {
                        // ignore this one as well
                    } catch (Exception e) {
                        logger.warn(indexShard.shardId() + ": Failed to force flush in order to clean memory", e);
                    }
                }
            }
        }
    }

    /**
     * Checks if memory needs to be cleaned and cleans it. Returns the amount of memory cleaned.
     */
    public MemoryCleanResult cleanMemory(long memoryToClean, ByteSizeValue minimumFlushableSizeToClean) {
        int totalShards = 0;
        long estimatedFlushableSize = 0;
        ArrayList<Tuple<ByteSizeValue, IndexShard>> shards = new ArrayList<Tuple<ByteSizeValue, IndexShard>>();
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (indexShard.state() != IndexShardState.STARTED) {
                    continue;
                }
                totalShards++;
                ByteSizeValue estimatedSize = indexShard.estimateFlushableMemorySize();
                estimatedFlushableSize += estimatedSize.bytes();
                if (estimatedSize != null) {
                    shards.add(new Tuple<ByteSizeValue, IndexShard>(estimatedSize, indexShard));
                }
            }
        }
        Collections.sort(shards, new Comparator<Tuple<ByteSizeValue, IndexShard>>() {
            @Override public int compare(Tuple<ByteSizeValue, IndexShard> o1, Tuple<ByteSizeValue, IndexShard> o2) {
                return (int) (o1.v1().bytes() - o2.v1().bytes());
            }
        });
        int cleanedShards = 0;
        long cleaned = 0;
        Set<ShardId> shardsCleaned = newHashSet();
        for (Tuple<ByteSizeValue, IndexShard> tuple : shards) {
            if (tuple.v1().bytes() < minimumFlushableSizeToClean.bytes()) {
                // we passed the minimum threshold, don't flush
                break;
            }
            try {
                tuple.v2().flush(new Engine.Flush());
            } catch (FlushNotAllowedEngineException e) {
                // ignore this one, its temporal
            } catch (IllegalIndexShardStateException e) {
                // ignore this one as well
            } catch (Exception e) {
                logger.warn(tuple.v2().shardId() + ": Failed to flush in order to clean memory", e);
            }
            shardsCleaned.add(tuple.v2().shardId());
            cleanedShards++;
            cleaned += tuple.v1().bytes();
            if (cleaned > memoryToClean) {
                break;
            }
        }
        return new MemoryCleanResult(totalShards, cleanedShards, new ByteSizeValue(estimatedFlushableSize), new ByteSizeValue(cleaned), shardsCleaned);
    }

    public static class TranslogCleanResult {
        private final int totalShards;
        private final int cleanedShards;
        private final ByteSizeValue cleaned;

        public TranslogCleanResult(int totalShards, int cleanedShards, ByteSizeValue cleaned) {
            this.totalShards = totalShards;
            this.cleanedShards = cleanedShards;
            this.cleaned = cleaned;
        }

        public int totalShards() {
            return totalShards;
        }

        public int cleanedShards() {
            return cleanedShards;
        }

        public ByteSizeValue cleaned() {
            return cleaned;
        }

        @Override public String toString() {
            return "cleaned [" + cleaned + "], cleaned_shards [" + cleanedShards + "], total_shards [" + totalShards + "]";
        }
    }

    public static class MemoryCleanResult {
        private final int totalShards;
        private final int cleanedShards;
        private final ByteSizeValue estimatedFlushableSize;
        private final ByteSizeValue cleaned;
        private final Set<ShardId> shardsCleaned;

        public MemoryCleanResult(int totalShards, int cleanedShards, ByteSizeValue estimatedFlushableSize, ByteSizeValue cleaned, Set<ShardId> shardsCleaned) {
            this.totalShards = totalShards;
            this.cleanedShards = cleanedShards;
            this.estimatedFlushableSize = estimatedFlushableSize;
            this.cleaned = cleaned;
            this.shardsCleaned = shardsCleaned;
        }

        public int totalShards() {
            return totalShards;
        }

        public int cleanedShards() {
            return cleanedShards;
        }

        public ByteSizeValue estimatedFlushableSize() {
            return estimatedFlushableSize;
        }

        public ByteSizeValue cleaned() {
            return cleaned;
        }

        public Set<ShardId> shardsCleaned() {
            return this.shardsCleaned;
        }

        @Override public String toString() {
            return "cleaned [" + cleaned + "], estimated_flushable_size [" + estimatedFlushableSize + "], cleaned_shards [" + cleanedShards + "], total_shards [" + totalShards + "]";
        }
    }
}

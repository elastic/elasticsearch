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

import com.google.inject.Inject;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.FlushNotAllowedEngineException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.InternalIndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.Tuple;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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
                    indexShard.flush();
                }
            }
        }
        return new TranslogCleanResult(totalShards, cleanedShards, new SizeValue(cleaned, SizeUnit.BYTES));
    }

    /**
     * Checks if memory needs to be cleaned and cleans it. Returns the amount of memory cleaned.
     */
    public MemoryCleanResult cleanMemory(long memoryToClean, SizeValue minimumFlushableSizeToClean) {
        int totalShards = 0;
        long estimatedFlushableSize = 0;
        ArrayList<Tuple<SizeValue, IndexShard>> shards = new ArrayList<Tuple<SizeValue, IndexShard>>();
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (indexShard.state() != IndexShardState.STARTED) {
                    continue;
                }
                totalShards++;
                SizeValue estimatedSize = indexShard.estimateFlushableMemorySize();
                estimatedFlushableSize += estimatedSize.bytes();
                if (estimatedSize != null) {
                    shards.add(new Tuple<SizeValue, IndexShard>(estimatedSize, indexShard));
                }
            }
        }
        Collections.sort(shards, new Comparator<Tuple<SizeValue, IndexShard>>() {
            @Override public int compare(Tuple<SizeValue, IndexShard> o1, Tuple<SizeValue, IndexShard> o2) {
                return (int) (o1.v1().bytes() - o2.v1().bytes());
            }
        });
        int cleanedShards = 0;
        long cleaned = 0;
        for (Tuple<SizeValue, IndexShard> tuple : shards) {
            if (tuple.v1().bytes() < minimumFlushableSizeToClean.bytes()) {
                // we passed the minimum threshold, don't flush
                break;
            }
            try {
                tuple.v2().flush();
            } catch (FlushNotAllowedEngineException e) {
                // ignore this one, its temporal
            } catch (IllegalIndexShardStateException e) {
                // ignore this one as well
            } catch (Exception e) {
                logger.warn(tuple.v2().shardId() + ": Failed to flush in order to clean memory", e);
            }
            cleanedShards++;
            cleaned += tuple.v1().bytes();
            if (cleaned > memoryToClean) {
                break;
            }
        }
        return new MemoryCleanResult(totalShards, cleanedShards, new SizeValue(estimatedFlushableSize), new SizeValue(cleaned));
    }

    public static class TranslogCleanResult {
        private final int totalShards;
        private final int cleanedShards;
        private final SizeValue cleaned;

        public TranslogCleanResult(int totalShards, int cleanedShards, SizeValue cleaned) {
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

        public SizeValue cleaned() {
            return cleaned;
        }

        @Override public String toString() {
            return "cleaned[" + cleaned + "], cleanedShards[" + cleanedShards + "], totalShards[" + totalShards + "]";
        }
    }

    public static class MemoryCleanResult {
        private final int totalShards;
        private final int cleanedShards;
        private final SizeValue estimatedFlushableSize;
        private final SizeValue cleaned;

        public MemoryCleanResult(int totalShards, int cleanedShards, SizeValue estimatedFlushableSize, SizeValue cleaned) {
            this.totalShards = totalShards;
            this.cleanedShards = cleanedShards;
            this.estimatedFlushableSize = estimatedFlushableSize;
            this.cleaned = cleaned;
        }

        public int totalShards() {
            return totalShards;
        }

        public int cleanedShards() {
            return cleanedShards;
        }

        public SizeValue estimatedFlushableSize() {
            return estimatedFlushableSize;
        }

        public SizeValue cleaned() {
            return cleaned;
        }

        @Override public String toString() {
            return "cleaned[" + cleaned + "], estimatedFlushableSize[" + estimatedFlushableSize + "], cleanedShards[" + cleanedShards + "], totalShards[" + totalShards + "]";
        }
    }
}

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

package org.elasticsearch.indices.memory;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.jvm.JvmInfo;

/**
 * @author kimchy (shay.banon)
 */
public class IndexingMemoryBufferController extends AbstractComponent {

    private final ByteSizeValue indexingBuffer;

    private final ByteSizeValue minShardIndexBufferSize;

    private final IndicesService indicesService;

    private final Listener listener = new Listener();

    @Inject public IndexingMemoryBufferController(Settings settings, IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;

        ByteSizeValue indexingBuffer;
        String indexingBufferSetting = componentSettings.get("index_buffer_size", "10%");
        if (indexingBufferSetting.endsWith("%")) {
            double percent = Double.parseDouble(indexingBufferSetting.substring(0, indexingBufferSetting.length() - 1));
            indexingBuffer = new ByteSizeValue((long) (((double) JvmInfo.jvmInfo().mem().heapMax().bytes()) * (percent / 100)));
            ByteSizeValue minIndexingBuffer = componentSettings.getAsBytesSize("min_index_buffer_size", new ByteSizeValue(48, ByteSizeUnit.MB));
            ByteSizeValue maxIndexingBuffer = componentSettings.getAsBytesSize("max_index_buffer_size", null);

            if (indexingBuffer.bytes() < minIndexingBuffer.bytes()) {
                indexingBuffer = minIndexingBuffer;
            }
            if (maxIndexingBuffer != null && indexingBuffer.bytes() > maxIndexingBuffer.bytes()) {
                indexingBuffer = maxIndexingBuffer;
            }
        } else {
            indexingBuffer = ByteSizeValue.parseBytesSizeValue(indexingBufferSetting, null);
        }

        this.indexingBuffer = indexingBuffer;
        this.minShardIndexBufferSize = componentSettings.getAsBytesSize("min_shard_index_buffer_size", new ByteSizeValue(4, ByteSizeUnit.MB));

        logger.debug("using index_buffer_size [{}], with min_shard_index_buffer_size [{}]", this.indexingBuffer, this.minShardIndexBufferSize);

        indicesService.indicesLifecycle().addListener(listener);
    }

    private class Listener extends IndicesLifecycle.Listener {

        @Override public void afterIndexShardCreated(IndexShard indexShard) {
            calcAndSetShardIndexingBuffer("created_shard[" + indexShard.shardId().index().name() + "][" + indexShard.shardId().id() + "]");
        }

        @Override public void afterIndexShardClosed(ShardId shardId, boolean delete) {
            calcAndSetShardIndexingBuffer("removed_shard[" + shardId.index().name() + "][" + shardId.id() + "]");
        }

        private void calcAndSetShardIndexingBuffer(String reason) {
            int shardsCount = countShards();
            if (shardsCount == 0) {
                return;
            }
            ByteSizeValue shardIndexingBufferSize = calcShardIndexingBuffer(shardsCount);
            if (shardIndexingBufferSize == null) {
                return;
            }
            if (shardIndexingBufferSize.bytes() < minShardIndexBufferSize.bytes()) {
                shardIndexingBufferSize = minShardIndexBufferSize;
            }
            logger.debug("recalculating shard indexing buffer (reason={}), total is [{}] with [{}] shards, each shard set to [{}]", reason, indexingBuffer, shardsCount, shardIndexingBufferSize);
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    ((InternalIndexShard) indexShard).engine().updateIndexingBufferSize(shardIndexingBufferSize);
                }
            }
        }

        private ByteSizeValue calcShardIndexingBuffer(int shardsCount) {
            return new ByteSizeValue(indexingBuffer.bytes() / shardsCount);
        }

        private int countShards() {
            int shardsCount = 0;
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    shardsCount++;
                }
            }
            return shardsCount;
        }
    }
}

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

package org.elasticsearch.index.store;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Index store is an index level information of the {@link Store} each shard will use.
 *
 * @author kimchy (shay.banon)
 */
public interface IndexStore extends IndexComponent {

    /**
     * Is the store a persistent store that can survive full restarts.
     */
    boolean persistent();

    /**
     * The shard store class that should be used for each shard.
     */
    Class<? extends Store> shardStoreClass();

    /**
     * Returns the backing store total space. Return <tt>-1</tt> if not available.
     */
    ByteSizeValue backingStoreTotalSpace();

    /**
     * Returns the backing store free space. Return <tt>-1</tt> if not available.
     */
    ByteSizeValue backingStoreFreeSpace();

    void deleteUnallocated(ShardId shardId) throws IOException;

    /**
     * Lists the store files metadata for a shard. Note, this should be able to list also
     * metadata for shards that are no allocated as well.
     */
    StoreFilesMetaData listStoreMetaData(ShardId shardId) throws IOException;

    static class StoreFilesMetaData implements Iterable<StoreFileMetaData>, Streamable {
        private boolean allocated;
        private ShardId shardId;
        private Map<String, StoreFileMetaData> files;

        StoreFilesMetaData() {
        }

        public StoreFilesMetaData(boolean allocated, ShardId shardId, Map<String, StoreFileMetaData> files) {
            this.allocated = allocated;
            this.shardId = shardId;
            this.files = files;
        }

        public boolean allocated() {
            return allocated;
        }

        public ShardId shardId() {
            return this.shardId;
        }

        public long totalSizeInBytes() {
            long totalSizeInBytes = 0;
            for (StoreFileMetaData file : this) {
                totalSizeInBytes += file.sizeInBytes();
            }
            return totalSizeInBytes;
        }

        @Override public Iterator<StoreFileMetaData> iterator() {
            return files.values().iterator();
        }

        public boolean fileExists(String name) {
            return files.containsKey(name);
        }

        public StoreFileMetaData file(String name) {
            return files.get(name);
        }

        public static StoreFilesMetaData readStoreFilesMetaData(StreamInput in) throws IOException {
            StoreFilesMetaData md = new StoreFilesMetaData();
            md.readFrom(in);
            return md;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            allocated = in.readBoolean();
            shardId = ShardId.readShardId(in);
            int size = in.readVInt();
            files = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                StoreFileMetaData md = StoreFileMetaData.readStoreFileMetaData(in);
                files.put(md.name(), md);
            }
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(allocated);
            shardId.writeTo(out);
            out.writeVInt(files.size());
            for (StoreFileMetaData md : files.values()) {
                md.writeTo(out);
            }
        }
    }
}

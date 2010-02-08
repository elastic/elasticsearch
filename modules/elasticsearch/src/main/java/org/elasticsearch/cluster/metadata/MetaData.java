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

package org.elasticsearch.cluster.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.concurrent.Immutable;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (Shay Banon)
 */
@Immutable
public class MetaData implements Iterable<IndexMetaData> {

    public static MetaData EMPTY_META_DATA = newMetaDataBuilder().build();

    private final ImmutableMap<String, IndexMetaData> indices;

    // limits the number of shards per node
    private final int maxNumberOfShardsPerNode;

    private final transient int totalNumberOfShards;

    private MetaData(ImmutableMap<String, IndexMetaData> indices, int maxNumberOfShardsPerNode) {
        this.indices = ImmutableMap.copyOf(indices);
        this.maxNumberOfShardsPerNode = maxNumberOfShardsPerNode;
        int totalNumberOfShards = 0;
        for (IndexMetaData indexMetaData : indices.values()) {
            totalNumberOfShards += indexMetaData.totalNumberOfShards();
        }
        this.totalNumberOfShards = totalNumberOfShards;
    }

    public boolean hasIndex(String index) {
        return indices.containsKey(index);
    }

    public IndexMetaData index(String index) {
        return indices.get(index);
    }

    public ImmutableMap<String, IndexMetaData> indices() {
        return this.indices;
    }

    public int maxNumberOfShardsPerNode() {
        return this.maxNumberOfShardsPerNode;
    }

    public int totalNumberOfShards() {
        return this.totalNumberOfShards;
    }

    @Override public UnmodifiableIterator<IndexMetaData> iterator() {
        return indices.values().iterator();
    }

    public static Builder newMetaDataBuilder() {
        return new Builder();
    }

    public static class Builder {

        // limits the number of shards per node
        private int maxNumberOfShardsPerNode = 100;

        private MapBuilder<String, IndexMetaData> indices = newMapBuilder();

        public Builder put(IndexMetaData.Builder indexMetaDataBuilder) {
            return put(indexMetaDataBuilder.build());
        }

        public Builder put(IndexMetaData indexMetaData) {
            indices.put(indexMetaData.index(), indexMetaData);
            return this;
        }

        public Builder remove(String index) {
            indices.remove(index);
            return this;
        }

        public Builder metaData(MetaData metaData) {
            indices.putAll(metaData.indices);
            return this;
        }

        public Builder maxNumberOfShardsPerNode(int maxNumberOfShardsPerNode) {
            this.maxNumberOfShardsPerNode = maxNumberOfShardsPerNode;
            return this;
        }

        public MetaData build() {
            return new MetaData(indices.immutableMap(), maxNumberOfShardsPerNode);
        }

        public static MetaData readFrom(DataInput in, @Nullable Settings globalSettings) throws IOException, ClassNotFoundException {
            Builder builder = new Builder();
            builder.maxNumberOfShardsPerNode(in.readInt());
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                builder.put(IndexMetaData.Builder.readFrom(in, globalSettings));
            }
            return builder.build();
        }

        public static void writeTo(MetaData metaData, DataOutput out) throws IOException {
            out.writeInt(metaData.maxNumberOfShardsPerNode());
            out.writeInt(metaData.indices.size());
            for (IndexMetaData indexMetaData : metaData) {
                IndexMetaData.Builder.writeTo(indexMetaData, out);
            }
        }
    }
}

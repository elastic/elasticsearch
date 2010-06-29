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

package org.elasticsearch.cluster.block;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Represents current cluster level blocks to block dirty operations done against the cluster.
 *
 * @author kimchy (shay.banon)
 */
public class ClusterBlocks {

    public static final ClusterBlocks EMPTY_CLUSTER_BLOCK = new ClusterBlocks(new ImmutableLevelHolder[]{ImmutableLevelHolder.EMPTY, ImmutableLevelHolder.EMPTY, ImmutableLevelHolder.EMPTY});

    private final ImmutableLevelHolder[] levelHolders;

    ClusterBlocks(ImmutableLevelHolder[] levelHolders) {
        this.levelHolders = levelHolders;
    }

    public ImmutableSet<ClusterBlock> global(ClusterBlockLevel level) {
        return levelHolders[level.id()].global();
    }

    public boolean globalBlocked() {
        for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
            if (!levelHolders[level.id()].global().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public ImmutableSet<String> indicesBlocked() {
        Set<String> indices = Sets.newHashSet();
        for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
            for (String index : indices(level).keySet()) {
                ImmutableSet<ClusterBlock> indexBlocks = indices(level).get(index);
                if (indexBlocks != null && !indexBlocks.isEmpty()) {
                    indices.add(index);
                }
            }
        }
        return ImmutableSet.copyOf(indices);
    }

    public ImmutableMap<ClusterBlockLevel, ImmutableSet<ClusterBlock>> indexBlocks(String index) {
        ImmutableMap.Builder<ClusterBlockLevel, ImmutableSet<ClusterBlock>> builder = ImmutableMap.builder();
        for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
            if (indices(level).containsKey(index) && !indices(level).get(index).isEmpty()) {
                builder.put(level, indices(level).get(index));
            }
        }
        return builder.build();
    }

    public ImmutableMap<String, ImmutableSet<ClusterBlock>> indices(ClusterBlockLevel level) {
        return levelHolders[level.id()].indices();
    }

    public void indexBlockedRaiseException(ClusterBlockLevel level, String index) throws ClusterBlockException {
        if (!indexBlocked(level, index)) {
            return;
        }
        ImmutableSet.Builder<ClusterBlock> builder = ImmutableSet.builder();
        builder.addAll(global(level));
        ImmutableSet<ClusterBlock> indexBlocks = indices(level).get(index);
        if (indexBlocks != null) {
            builder.addAll(indexBlocks);
        }
        throw new ClusterBlockException(builder.build());
    }

    public boolean indexBlocked(ClusterBlockLevel level, String index) {
        if (!global(level).isEmpty()) {
            return true;
        }
        ImmutableSet<ClusterBlock> indexBlocks = indices(level).get(index);
        if (indexBlocks != null && !indexBlocks.isEmpty()) {
            return true;
        }
        return false;
    }

    static class ImmutableLevelHolder {

        static final ImmutableLevelHolder EMPTY = new ImmutableLevelHolder(ImmutableSet.<ClusterBlock>of(), ImmutableMap.<String, ImmutableSet<ClusterBlock>>of());

        private final ImmutableSet<ClusterBlock> global;
        private final ImmutableMap<String, ImmutableSet<ClusterBlock>> indices;

        ImmutableLevelHolder(ImmutableSet<ClusterBlock> global, ImmutableMap<String, ImmutableSet<ClusterBlock>> indices) {
            this.global = global;
            this.indices = indices;
        }

        public ImmutableSet<ClusterBlock> global() {
            return global;
        }

        public ImmutableMap<String, ImmutableSet<ClusterBlock>> indices() {
            return indices;
        }
    }

    static class LevelHolder {
        private final Set<ClusterBlock> global = Sets.newHashSet();
        private final Map<String, Set<ClusterBlock>> indices = Maps.newHashMap();

        LevelHolder() {
        }

        public Set<ClusterBlock> global() {
            return global;
        }

        public Map<String, Set<ClusterBlock>> indices() {
            return indices;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private LevelHolder[] levelHolders;

        public Builder() {
            levelHolders = new LevelHolder[ClusterBlockLevel.values().length];
            for (int i = 0; i < levelHolders.length; i++) {
                levelHolders[i] = new LevelHolder();
            }
        }

        public Builder blocks(ClusterBlocks blocks) {
            for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
                levelHolders[level.id()].global().addAll(blocks.global(level));
                for (Map.Entry<String, ImmutableSet<ClusterBlock>> entry : blocks.indices(level).entrySet()) {
                    if (!levelHolders[level.id()].indices().containsKey(entry.getKey())) {
                        levelHolders[level.id()].indices().put(entry.getKey(), Sets.<ClusterBlock>newHashSet());
                    }
                    levelHolders[level.id()].indices().get(entry.getKey()).addAll(entry.getValue());
                }
            }
            return this;
        }

        public Builder addGlobalBlock(ClusterBlock block) {
            for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
                addGlobalBlock(level, block);
            }
            return this;
        }

        public Builder addGlobalBlock(ClusterBlockLevel level, ClusterBlock block) {
            levelHolders[level.id()].global().add(block);
            return this;
        }

        public Builder removeGlobalBlock(ClusterBlock block) {
            for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
                removeGlobalBlock(level, block);
            }
            return this;
        }

        public Builder removeGlobalBlock(ClusterBlockLevel level, ClusterBlock block) {
            levelHolders[level.id()].global().remove(block);
            return this;
        }

        public Builder addIndexBlock(String index, ClusterBlockLevel level, ClusterBlock block) {
            if (!levelHolders[level.id()].indices().containsKey(index)) {
                levelHolders[level.id()].indices().put(index, Sets.<ClusterBlock>newHashSet());
            }
            levelHolders[level.id()].indices().get(index).add(block);
            return this;
        }

        public Builder removeIndexBlock(String index, ClusterBlockLevel level, ClusterBlock block) {
            Set<ClusterBlock> indexBlocks = levelHolders[level.id()].indices().get(index);
            if (indexBlocks == null) {
                return this;
            }
            indexBlocks.remove(block);
            return this;
        }

        public ClusterBlocks build() {
            ImmutableLevelHolder[] holders = new ImmutableLevelHolder[ClusterBlockLevel.values().length];
            for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
                ImmutableMap.Builder<String, ImmutableSet<ClusterBlock>> indicesBuilder = ImmutableMap.builder();
                for (Map.Entry<String, Set<ClusterBlock>> entry : levelHolders[level.id()].indices().entrySet()) {
                    indicesBuilder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
                }
                holders[level.id()] = new ImmutableLevelHolder(ImmutableSet.copyOf(levelHolders[level.id()].global()), indicesBuilder.build());
            }
            return new ClusterBlocks(holders);
        }

        public static ClusterBlocks readClusterBlocks(StreamInput in) throws IOException {
            ImmutableLevelHolder[] holders = new ImmutableLevelHolder[ClusterBlockLevel.values().length];
            for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
                ImmutableSet<ClusterBlock> global = readBlockSet(in);
                ImmutableMap.Builder<String, ImmutableSet<ClusterBlock>> indicesBuilder = ImmutableMap.builder();
                int size = in.readVInt();
                for (int j = 0; j < size; j++) {
                    indicesBuilder.put(in.readUTF().intern(), readBlockSet(in));
                }
                holders[level.id()] = new ImmutableLevelHolder(global, indicesBuilder.build());
            }
            return new ClusterBlocks(holders);
        }

        public static void writeClusterBlocks(ClusterBlocks clusterBlock, StreamOutput out) throws IOException {
            for (ClusterBlockLevel level : ClusterBlockLevel.values()) {
                writeBlockSet(clusterBlock.global(level), out);
                out.writeVInt(clusterBlock.indices(level).size());
                for (Map.Entry<String, ImmutableSet<ClusterBlock>> entry : clusterBlock.indices(level).entrySet()) {
                    out.writeUTF(entry.getKey());
                    writeBlockSet(entry.getValue(), out);
                }
            }
        }

        private static void writeBlockSet(ImmutableSet<ClusterBlock> blocks, StreamOutput out) throws IOException {
            out.writeVInt(blocks.size());
            for (ClusterBlock block : blocks) {
                out.writeVInt(block.id());
                out.writeUTF(block.description());
            }
        }

        private static ImmutableSet<ClusterBlock> readBlockSet(StreamInput in) throws IOException {
            ImmutableSet.Builder<ClusterBlock> builder = ImmutableSet.builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.add(new ClusterBlock(in.readVInt(), in.readUTF().intern()));
            }
            return builder.build();
        }
    }
}

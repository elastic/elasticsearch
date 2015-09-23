/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

/**
 * Represents current cluster level blocks to block dirty operations done against the cluster.
 */
public class ClusterBlocks extends AbstractDiffable<ClusterBlocks> {
    public static final ClusterBlocks EMPTY_CLUSTER_BLOCK = new ClusterBlocks(emptySet(), emptyMap());

    public static final ClusterBlocks PROTO = EMPTY_CLUSTER_BLOCK;

    private final Set<ClusterBlock> global;

    private final Map<String, Set<ClusterBlock>> indicesBlocks;

    private final ImmutableLevelHolder[] levelHolders;

    ClusterBlocks(Set<ClusterBlock> global, Map<String, Set<ClusterBlock>> indicesBlocks) {
        this.global = global;
        this.indicesBlocks = indicesBlocks;

        levelHolders = new ImmutableLevelHolder[ClusterBlockLevel.values().length];
        for (final ClusterBlockLevel level : ClusterBlockLevel.values()) {
            Predicate<ClusterBlock> containsLevel = block -> block.contains(level);
            Set<ClusterBlock> newGlobal = unmodifiableSet(global.stream().filter(containsLevel).collect(toSet()));

            ImmutableMap.Builder<String, Set<ClusterBlock>> indicesBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Set<ClusterBlock>> entry : indicesBlocks.entrySet()) {
                indicesBuilder.put(entry.getKey(), unmodifiableSet(entry.getValue().stream().filter(containsLevel).collect(toSet())));
            }

            levelHolders[level.id()] = new ImmutableLevelHolder(newGlobal, indicesBuilder.build());
        }
    }

    public Set<ClusterBlock> global() {
        return global;
    }

    public Map<String, Set<ClusterBlock>> indices() {
        return indicesBlocks;
    }

    public Set<ClusterBlock> global(ClusterBlockLevel level) {
        return levelHolders[level.id()].global();
    }

    public Map<String, Set<ClusterBlock>> indices(ClusterBlockLevel level) {
        return levelHolders[level.id()].indices();
    }

    public Set<ClusterBlock> blocksForIndex(ClusterBlockLevel level, String index) {
        return indices(level).getOrDefault(index, emptySet());
    }

    /**
     * Returns <tt>true</tt> if one of the global blocks as its disable state persistence flag set.
     */
    public boolean disableStatePersistence() {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.disableStatePersistence()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasGlobalBlock(ClusterBlock block) {
        return global.contains(block);
    }

    public boolean hasGlobalBlock(int blockId) {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.id() == blockId) {
                return true;
            }
        }
        return false;
    }

    public boolean hasGlobalBlock(ClusterBlockLevel level) {
        return global(level).size() > 0;
    }

    /**
     * Is there a global block with the provided status?
     */
    public boolean hasGlobalBlock(RestStatus status) {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.status().equals(status)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasIndexBlock(String index, ClusterBlock block) {
        return indicesBlocks.containsKey(index) && indicesBlocks.get(index).contains(block);
    }

    public void globalBlockedRaiseException(ClusterBlockLevel level) throws ClusterBlockException {
        ClusterBlockException blockException = globalBlockedException(level);
        if (blockException != null) {
            throw blockException;
        }
    }

    public ClusterBlockException globalBlockedException(ClusterBlockLevel level) {
        if (global(level).isEmpty()) {
            return null;
        }
        return new ClusterBlockException(ImmutableSet.copyOf(global(level)));
    }

    public void indexBlockedRaiseException(ClusterBlockLevel level, String index) throws ClusterBlockException {
        ClusterBlockException blockException = indexBlockedException(level, index);
        if (blockException != null) {
            throw blockException;
        }
    }

    public ClusterBlockException indexBlockedException(ClusterBlockLevel level, String index) {
        if (!indexBlocked(level, index)) {
            return null;
        }
        Stream<ClusterBlock> blocks = concat(global(level).stream(), blocksForIndex(level, index).stream());
        return new ClusterBlockException(unmodifiableSet(blocks.collect(toSet())));
    }

    public boolean indexBlocked(ClusterBlockLevel level, String index) {
        if (!global(level).isEmpty()) {
            return true;
        }
        return !blocksForIndex(level, index).isEmpty();
    }

    public ClusterBlockException indicesBlockedException(ClusterBlockLevel level, String[] indices) {
        boolean indexIsBlocked = false;
        for (String index : indices) {
            if (indexBlocked(level, index)) {
                indexIsBlocked = true;
            }
        }
        if (!indexIsBlocked) {
            return null;
        }
        Function<String, Stream<ClusterBlock>> blocksForIndexAtLevel = index -> blocksForIndex(level, index).stream();
        Stream<ClusterBlock> blocks = concat(global(level).stream(), Stream.of(indices).flatMap(blocksForIndexAtLevel));
        return new ClusterBlockException(unmodifiableSet(blocks.collect(toSet())));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeBlockSet(global, out);
        out.writeVInt(indicesBlocks.size());
        for (Map.Entry<String, Set<ClusterBlock>> entry : indicesBlocks.entrySet()) {
            out.writeString(entry.getKey());
            writeBlockSet(entry.getValue(), out);
        }
    }

    private static void writeBlockSet(Set<ClusterBlock> blocks, StreamOutput out) throws IOException {
        out.writeVInt(blocks.size());
        for (ClusterBlock block : blocks) {
            block.writeTo(out);
        }
    }

    @Override
    public ClusterBlocks readFrom(StreamInput in) throws IOException {
        Set<ClusterBlock> global = readBlockSet(in);
        ImmutableMap.Builder<String, Set<ClusterBlock>> indicesBuilder = ImmutableMap.builder();
        int size = in.readVInt();
        for (int j = 0; j < size; j++) {
            indicesBuilder.put(in.readString().intern(), readBlockSet(in));
        }
        return new ClusterBlocks(global, indicesBuilder.build());
    }

    private static Set<ClusterBlock> readBlockSet(StreamInput in) throws IOException {
        return unmodifiableSet(in.readSet(ClusterBlock::readClusterBlock));
    }

    static class ImmutableLevelHolder {

        static final ImmutableLevelHolder EMPTY = new ImmutableLevelHolder(emptySet(), ImmutableMap.of());

        private final Set<ClusterBlock> global;
        private final ImmutableMap<String, Set<ClusterBlock>> indices;

        ImmutableLevelHolder(Set<ClusterBlock> global, ImmutableMap<String, Set<ClusterBlock>> indices) {
            this.global = global;
            this.indices = indices;
        }

        public Set<ClusterBlock> global() {
            return global;
        }

        public ImmutableMap<String, Set<ClusterBlock>> indices() {
            return indices;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Set<ClusterBlock> global = new HashSet<>();

        private Map<String, Set<ClusterBlock>> indices = new HashMap<>();

        public Builder() {
        }

        public Builder blocks(ClusterBlocks blocks) {
            global.addAll(blocks.global());
            for (Map.Entry<String, Set<ClusterBlock>> entry : blocks.indices().entrySet()) {
                if (!indices.containsKey(entry.getKey())) {
                    indices.put(entry.getKey(), new HashSet<>());
                }
                indices.get(entry.getKey()).addAll(entry.getValue());
            }
            return this;
        }

        public Builder addBlocks(IndexMetaData indexMetaData) {
            if (indexMetaData.state() == IndexMetaData.State.CLOSE) {
                addIndexBlock(indexMetaData.index(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
            }
            if (indexMetaData.settings().getAsBoolean(IndexMetaData.SETTING_READ_ONLY, false)) {
                addIndexBlock(indexMetaData.index(), IndexMetaData.INDEX_READ_ONLY_BLOCK);
            }
            if (indexMetaData.settings().getAsBoolean(IndexMetaData.SETTING_BLOCKS_READ, false)) {
                addIndexBlock(indexMetaData.index(), IndexMetaData.INDEX_READ_BLOCK);
            }
            if (indexMetaData.settings().getAsBoolean(IndexMetaData.SETTING_BLOCKS_WRITE, false)) {
                addIndexBlock(indexMetaData.index(), IndexMetaData.INDEX_WRITE_BLOCK);
            }
            if (indexMetaData.settings().getAsBoolean(IndexMetaData.SETTING_BLOCKS_METADATA, false)) {
                addIndexBlock(indexMetaData.index(), IndexMetaData.INDEX_METADATA_BLOCK);
            }
            return this;
        }

        public Builder addGlobalBlock(ClusterBlock block) {
            global.add(block);
            return this;
        }

        public Builder removeGlobalBlock(ClusterBlock block) {
            global.remove(block);
            return this;
        }

        public Builder addIndexBlock(String index, ClusterBlock block) {
            if (!indices.containsKey(index)) {
                indices.put(index, new HashSet<>());
            }
            indices.get(index).add(block);
            return this;
        }

        public Builder removeIndexBlocks(String index) {
            if (!indices.containsKey(index)) {
                return this;
            }
            indices.remove(index);
            return this;
        }

        public Builder removeIndexBlock(String index, ClusterBlock block) {
            if (!indices.containsKey(index)) {
                return this;
            }
            indices.get(index).remove(block);
            if (indices.get(index).isEmpty()) {
                indices.remove(index);
            }
            return this;
        }

        public ClusterBlocks build() {
            // We copy the block sets here in case of the builder is modified after build is called
            ImmutableMap.Builder<String, Set<ClusterBlock>> indicesBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Set<ClusterBlock>> entry : indices.entrySet()) {
                indicesBuilder.put(entry.getKey(), unmodifiableSet(new HashSet<>(entry.getValue())));
            }
            return new ClusterBlocks(unmodifiableSet(new HashSet<>(global)), indicesBuilder.build());
        }

        public static ClusterBlocks readClusterBlocks(StreamInput in) throws IOException {
            return PROTO.readFrom(in);
        }
    }
}

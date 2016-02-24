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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
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

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

/**
 * Represents current cluster level blocks to block dirty operations done against the cluster.
 */
public class ClusterBlocks extends AbstractDiffable<ClusterBlocks> {
    public static final ClusterBlocks EMPTY_CLUSTER_BLOCK = new ClusterBlocks(emptySet(), ImmutableOpenMap.of());

    public static final ClusterBlocks PROTO = EMPTY_CLUSTER_BLOCK;

    private final Set<ClusterBlock> global;

    private final ImmutableOpenMap<String, Set<ClusterBlock>> indicesBlocks;

    private final ImmutableLevelHolder[] levelHolders;

    ClusterBlocks(Set<ClusterBlock> global, ImmutableOpenMap<String, Set<ClusterBlock>> indicesBlocks) {
        this.global = global;
        this.indicesBlocks = indicesBlocks;

        levelHolders = new ImmutableLevelHolder[ClusterBlockLevel.values().length];
        for (final ClusterBlockLevel level : ClusterBlockLevel.values()) {
            Predicate<ClusterBlock> containsLevel = block -> block.contains(level);
            Set<ClusterBlock> newGlobal = unmodifiableSet(global.stream()
                    .filter(containsLevel)
                    .collect(toSet()));

            ImmutableOpenMap.Builder<String, Set<ClusterBlock>> indicesBuilder = ImmutableOpenMap.builder();
            for (ObjectObjectCursor<String, Set<ClusterBlock>> entry : indicesBlocks) {
                indicesBuilder.put(entry.key, unmodifiableSet(entry.value.stream()
                        .filter(containsLevel)
                        .collect(toSet())));
            }

            levelHolders[level.id()] = new ImmutableLevelHolder(newGlobal, indicesBuilder.build());
        }
    }

    public Set<ClusterBlock> global() {
        return global;
    }

    public ImmutableOpenMap<String, Set<ClusterBlock>> indices() {
        return indicesBlocks;
    }

    public Set<ClusterBlock> global(ClusterBlockLevel level) {
        return levelHolders[level.id()].global();
    }

    public ImmutableOpenMap<String, Set<ClusterBlock>> indices(ClusterBlockLevel level) {
        return levelHolders[level.id()].indices();
    }

    private Set<ClusterBlock> blocksForIndex(ClusterBlockLevel level, String index) {
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
        return new ClusterBlockException(global(level));
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
        Stream<ClusterBlock> blocks = concat(
                global(level).stream(),
                blocksForIndex(level, index).stream());
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
        Stream<ClusterBlock> blocks = concat(
                global(level).stream(),
                Stream.of(indices).flatMap(blocksForIndexAtLevel));
        return new ClusterBlockException(unmodifiableSet(blocks.collect(toSet())));
    }

    public String prettyPrint() {
        if (global.isEmpty() && indices().isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("blocks: \n");
        if (global.isEmpty() == false) {
            sb.append("   _global_:\n");
            for (ClusterBlock block : global) {
                sb.append("      ").append(block);
            }
        }
        for (ObjectObjectCursor<String, Set<ClusterBlock>> entry : indices()) {
            sb.append("   ").append(entry.key).append(":\n");
            for (ClusterBlock block : entry.value) {
                sb.append("      ").append(block);
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeBlockSet(global, out);
        out.writeVInt(indicesBlocks.size());
        for (ObjectObjectCursor<String, Set<ClusterBlock>> entry : indicesBlocks) {
            out.writeString(entry.key);
            writeBlockSet(entry.value, out);
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
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, Set<ClusterBlock>> indicesBuilder = ImmutableOpenMap.builder(size);
        for (int j = 0; j < size; j++) {
            indicesBuilder.put(in.readString().intern(), readBlockSet(in));
        }
        return new ClusterBlocks(global, indicesBuilder.build());
    }

    private static Set<ClusterBlock> readBlockSet(StreamInput in) throws IOException {
        int totalBlocks = in.readVInt();
        Set<ClusterBlock> blocks = new HashSet<>(totalBlocks);
        for (int i = 0; i < totalBlocks;i++) {
            blocks.add(ClusterBlock.readClusterBlock(in));
        }
        return unmodifiableSet(blocks);
    }

    static class ImmutableLevelHolder {

        static final ImmutableLevelHolder EMPTY = new ImmutableLevelHolder(emptySet(), ImmutableOpenMap.of());

        private final Set<ClusterBlock> global;
        private final ImmutableOpenMap<String, Set<ClusterBlock>> indices;

        ImmutableLevelHolder(Set<ClusterBlock> global, ImmutableOpenMap<String, Set<ClusterBlock>> indices) {
            this.global = global;
            this.indices = indices;
        }

        public Set<ClusterBlock> global() {
            return global;
        }

        public ImmutableOpenMap<String, Set<ClusterBlock>> indices() {
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
            for (ObjectObjectCursor<String, Set<ClusterBlock>> entry : blocks.indices()) {
                if (!indices.containsKey(entry.key)) {
                    indices.put(entry.key, new HashSet<>());
                }
                indices.get(entry.key).addAll(entry.value);
            }
            return this;
        }

        public Builder addBlocks(IndexMetaData indexMetaData) {
            if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                addIndexBlock(indexMetaData.getIndex().getName(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
            }
            if (IndexMetaData.INDEX_READ_ONLY_SETTING.get(indexMetaData.getSettings())) {
                addIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_READ_ONLY_BLOCK);
            }
            if (IndexMetaData.INDEX_BLOCKS_READ_SETTING.get(indexMetaData.getSettings())) {
                addIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_READ_BLOCK);
            }
            if (IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.get(indexMetaData.getSettings())) {
                addIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_WRITE_BLOCK);
            }
            if (IndexMetaData.INDEX_BLOCKS_METADATA_SETTING.get(indexMetaData.getSettings())) {
                addIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_METADATA_BLOCK);
            }
            return this;
        }

        public Builder updateBlocks(IndexMetaData indexMetaData) {
            removeIndexBlock(indexMetaData.getIndex().getName(), MetaDataIndexStateService.INDEX_CLOSED_BLOCK);
            removeIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_READ_ONLY_BLOCK);
            removeIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_READ_BLOCK);
            removeIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_WRITE_BLOCK);
            removeIndexBlock(indexMetaData.getIndex().getName(), IndexMetaData.INDEX_METADATA_BLOCK);
            return addBlocks(indexMetaData);
        }

        public Builder addGlobalBlock(ClusterBlock block) {
            global.add(block);
            return this;
        }

        public Builder removeGlobalBlock(ClusterBlock block) {
            global.remove(block);
            return this;
        }

        public Builder removeGlobalBlock(int blockId) {
            global.removeIf(block -> block.id() == blockId);
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
            ImmutableOpenMap.Builder<String, Set<ClusterBlock>> indicesBuilder = ImmutableOpenMap.builder(indices.size());
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

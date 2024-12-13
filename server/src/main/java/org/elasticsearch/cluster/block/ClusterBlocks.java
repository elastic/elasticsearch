/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.block;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Represents current cluster level blocks to block dirty operations done against the cluster.
 */
public class ClusterBlocks implements Diffable<ClusterBlocks> {
    private static final ClusterBlock[] EMPTY_BLOCKS_ARRAY = new ClusterBlock[0];

    public static final ClusterBlocks EMPTY_CLUSTER_BLOCK = new ClusterBlocks(Set.of(), Map.of());

    private final Set<ClusterBlock> global;

    /**
     * Within the same cluster state, each project that exists in {@link Metadata} must also have an entry in this map.
     * The value can be {@link ProjectBlocks#EMPTY} if the project does not have any blocks.
     * For stateful, the only entry should be {@link Metadata#DEFAULT_PROJECT_ID}.
     */
    // Package private for testing
    final Map<ProjectId, ProjectBlocks> projectBlocksMap;

    private final EnumMap<ClusterBlockLevel, ImmutableLevelHolder> levelHolders;

    @FixForMultiProject(description = "consider not adding default project on empty projectBlocksMap")
    ClusterBlocks(Set<ClusterBlock> global, Map<ProjectId, ProjectBlocks> projectBlocksMap) {
        this.global = global;
        this.projectBlocksMap = projectBlocksMap.isEmpty() ? Map.of(Metadata.DEFAULT_PROJECT_ID, ProjectBlocks.EMPTY) : projectBlocksMap;
        levelHolders = generateLevelHolders(global, projectBlocksMap);
    }

    public Set<ClusterBlock> global() {
        return global;
    }

    public boolean noIndexBlockAllProjects() {
        return projectBlocksMap.values().stream().allMatch(ProjectBlocks::isEmpty);
    }

    @Deprecated(forRemoval = true)
    public Map<String, Set<ClusterBlock>> indices() {
        throwIfMultiProjects();
        return indices(Metadata.DEFAULT_PROJECT_ID);
    }

    public Map<String, Set<ClusterBlock>> indices(ProjectId projectId) {
        return projectBlocksMap.getOrDefault(projectId, ProjectBlocks.EMPTY).indices();
    }

    public Set<ClusterBlock> global(ClusterBlockLevel level) {
        return levelHolders.get(level).global();
    }

    @Deprecated(forRemoval = true)
    public Map<String, Set<ClusterBlock>> indices(ClusterBlockLevel level) {
        throwIfMultiProjects();
        return indices(Metadata.DEFAULT_PROJECT_ID, level);
    }

    public Map<String, Set<ClusterBlock>> indices(ProjectId projectId, ClusterBlockLevel level) {
        return levelHolders.get(level).projects.getOrDefault(projectId, ProjectBlocks.EMPTY).indices();
    }

    private Set<ClusterBlock> blocksForIndex(ProjectId projectId, ClusterBlockLevel level, String index) {
        return indices(projectId, level).getOrDefault(index, Set.of());
    }

    private static EnumMap<ClusterBlockLevel, ImmutableLevelHolder> generateLevelHolders(
        Set<ClusterBlock> global,
        Map<ProjectId, ProjectBlocks> projectBlocksMap
    ) {
        EnumMap<ClusterBlockLevel, ImmutableLevelHolder> levelHolders = new EnumMap<>(ClusterBlockLevel.class);
        // reusable scratch list to collect matching blocks into in #addBlocksAtLevel temporarily, so we don't have to allocate it in the
        // loop
        List<ClusterBlock> scratch = new ArrayList<>();
        Map<ProjectId, ProjectBlocks> projectsBuilder = Maps.newMapWithExpectedSize(projectBlocksMap.size());
        Map<String, Set<ClusterBlock>> indicesBuilder = Maps.newMapWithExpectedSize(
            projectBlocksMap.values().stream().mapToInt(pb -> pb.indices().size()).max().orElse(0)
        );
        for (final ClusterBlockLevel level : ClusterBlockLevel.values()) {
            for (Map.Entry<ProjectId, ProjectBlocks> projectEntry : projectBlocksMap.entrySet()) {
                for (Map.Entry<String, Set<ClusterBlock>> indexEntry : projectEntry.getValue().indices().entrySet()) {
                    indicesBuilder.put(indexEntry.getKey(), addBlocksAtLevel(indexEntry.getValue(), scratch, level));
                }
                projectsBuilder.put(projectEntry.getKey(), new ProjectBlocks(Map.copyOf(indicesBuilder)));
                indicesBuilder.clear();
            }
            levelHolders.put(level, new ImmutableLevelHolder(addBlocksAtLevel(global, scratch, level), Map.copyOf(projectsBuilder)));
            projectsBuilder.clear();
        }
        return levelHolders;
    }

    private static Set<ClusterBlock> addBlocksAtLevel(Set<ClusterBlock> blocks, List<ClusterBlock> scratch, ClusterBlockLevel level) {
        for (ClusterBlock clusterBlock : blocks) {
            if (clusterBlock.contains(level)) {
                scratch.add(clusterBlock);
            }
        }
        var res = Set.of(scratch.toArray(EMPTY_BLOCKS_ARRAY));
        scratch.clear();
        return res;
    }

    /**
     * Returns {@code true} if one of the global blocks as its disable state persistence flag set.
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

    public boolean hasGlobalBlockWithId(final int blockId) {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.id() == blockId) {
                return true;
            }
        }
        return false;
    }

    public boolean hasGlobalBlockWithLevel(ClusterBlockLevel level) {
        return global(level).size() > 0;
    }

    /**
     * Is there a global block with the provided status?
     */
    public boolean hasGlobalBlockWithStatus(final RestStatus status) {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.status().equals(status)) {
                return true;
            }
        }
        return false;
    }

    @Deprecated(forRemoval = true)
    public boolean hasIndexBlock(String index, ClusterBlock block) {
        throwIfMultiProjects();
        return hasIndexBlock(Metadata.DEFAULT_PROJECT_ID, index, block);
    }

    public boolean hasIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
        final var projectBlocks = projectBlocksMap.get(projectId);
        if (projectBlocks == null) {
            return false;
        }
        final Set<ClusterBlock> clusterBlocks = projectBlocks.get(index);
        if (clusterBlocks == null) {
            return false;
        }
        return clusterBlocks.contains(block);
    }

    @Deprecated(forRemoval = true)
    public boolean hasIndexBlockWithId(String index, int blockId) {
        throwIfMultiProjects();
        return hasIndexBlockWithId(Metadata.DEFAULT_PROJECT_ID, index, blockId);
    }

    // TODO: this can be simplified to `return getIndexBlockWithId(...) != null`
    public boolean hasIndexBlockWithId(ProjectId projectId, String index, int blockId) {
        final var projectBlocks = projectBlocksMap.get(projectId);
        if (projectBlocks != null) {
            final Set<ClusterBlock> clusterBlocks = projectBlocks.get(index);
            if (clusterBlocks != null) {
                for (ClusterBlock clusterBlock : clusterBlocks) {
                    if (clusterBlock.id() == blockId) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Deprecated(forRemoval = true)
    @Nullable
    public ClusterBlock getIndexBlockWithId(final String index, final int blockId) {
        throwIfMultiProjects();
        return getIndexBlockWithId(Metadata.DEFAULT_PROJECT_ID, index, blockId);
    }

    @Nullable
    public ClusterBlock getIndexBlockWithId(final ProjectId projectId, final String index, final int blockId) {
        final var projectBlocks = projectBlocksMap.get(projectId);
        if (projectBlocks != null) {
            final Set<ClusterBlock> clusterBlocks = projectBlocks.get(index);
            if (clusterBlocks != null) {
                for (ClusterBlock clusterBlock : clusterBlocks) {
                    if (clusterBlock.id() == blockId) {
                        return clusterBlock;
                    }
                }
            }
        }
        return null;
    }

    public void globalBlockedRaiseException(ClusterBlockLevel level) throws ClusterBlockException {
        ClusterBlockException blockException = globalBlockedException(level);
        if (blockException != null) {
            throw blockException;
        }
    }

    private boolean globalBlocked(ClusterBlockLevel level) {
        return global(level).isEmpty() == false;
    }

    public ClusterBlockException globalBlockedException(ClusterBlockLevel level) {
        if (globalBlocked(level) == false) {
            return null;
        }
        return new ClusterBlockException(global(level));
    }

    @Deprecated(forRemoval = true)
    public void indexBlockedRaiseException(ClusterBlockLevel level, String index) throws ClusterBlockException {
        // Not throw for multi-project for now to avoid wide-spread cascading changes.
        ClusterBlockException blockException = indexBlockedException(level, index);
        if (blockException != null) {
            throw blockException;
        }
    }

    @Deprecated(forRemoval = true)
    public ClusterBlockException indexBlockedException(ClusterBlockLevel level, String index) {
        // Not throw for multi-project for now to avoid wide-spread cascading changes.
        return indexBlockedException(Metadata.DEFAULT_PROJECT_ID, level, index);
    }

    public ClusterBlockException indexBlockedException(ProjectId projectId, ClusterBlockLevel level, String index) {
        return indicesBlockedException(projectId, level, new String[] { index });
    }

    @Deprecated(forRemoval = true)
    public boolean indexBlocked(ClusterBlockLevel level, String index) {
        // Not throw for multi-project for now to avoid wide-spread cascading changes.
        return indexBlocked(Metadata.DEFAULT_PROJECT_ID, level, index);
    }

    public boolean indexBlocked(ProjectId projectId, ClusterBlockLevel level, String index) {
        return globalBlocked(level) || blocksForIndex(projectId, level, index).isEmpty() == false;
    }

    @Deprecated(forRemoval = true)
    public ClusterBlockException indicesBlockedException(ClusterBlockLevel level, String[] indices) {
        // Not throw for multi-project for now to avoid wide-spread cascading changes.
        return indicesBlockedException(Metadata.DEFAULT_PROJECT_ID, level, indices);
    }

    public ClusterBlockException indicesBlockedException(ProjectId projectId, ClusterBlockLevel level, String[] indices) {
        Set<ClusterBlock> globalLevelBlocks = global(level);
        Map<String, Set<ClusterBlock>> indexLevelBlocks = new HashMap<>();
        for (String index : indices) {
            Set<ClusterBlock> indexBlocks = blocksForIndex(projectId, level, index);
            if (indexBlocks.isEmpty() == false || globalLevelBlocks.isEmpty() == false) {
                indexLevelBlocks.put(index, Sets.union(indexBlocks, globalLevelBlocks));
            }
        }
        if (indexLevelBlocks.isEmpty()) {
            if (globalLevelBlocks.isEmpty() == false) {
                return new ClusterBlockException(globalLevelBlocks);
            }
            return null;
        }
        return new ClusterBlockException(indexLevelBlocks);
    }

    /**
     * Returns <code>true</code> iff non of the given have a {@link ClusterBlockLevel#METADATA_WRITE} in place where the
     * {@link ClusterBlock#isAllowReleaseResources()} returns <code>false</code>. This is used in places where resources will be released
     * like the deletion of an index to free up resources on nodes.
     * @param projectId the project that owns the indices
     * @param indices the indices to check
     */
    public ClusterBlockException indicesAllowReleaseResources(ProjectId projectId, String[] indices) {
        Set<ClusterBlock> globalBlocks = global(ClusterBlockLevel.METADATA_WRITE).stream()
            .filter(clusterBlock -> clusterBlock.isAllowReleaseResources() == false)
            .collect(toSet());
        Map<String, Set<ClusterBlock>> indexLevelBlocks = new HashMap<>();
        for (String index : indices) {
            Set<ClusterBlock> blocks = Sets.union(globalBlocks, blocksForIndex(projectId, ClusterBlockLevel.METADATA_WRITE, index))
                .stream()
                .filter(clusterBlock -> clusterBlock.isAllowReleaseResources() == false)
                .collect(toSet());
            if (blocks.isEmpty() == false) {
                indexLevelBlocks.put(index, Sets.union(globalBlocks, blocks));
            }
        }
        if (indexLevelBlocks.isEmpty()) {
            if (globalBlocks.isEmpty() == false) {
                return new ClusterBlockException(globalBlocks);
            }
            return null;
        }
        return new ClusterBlockException(indexLevelBlocks);
    }

    @Override
    public String toString() {
        if (global.isEmpty() && noIndexBlockAllProjects()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("blocks: \n");
        if (global.isEmpty() == false) {
            sb.append("   _global_:\n");
            for (ClusterBlock block : global) {
                sb.append("      ").append(block).append("\n");
            }
        }
        for (var projectId : projectBlocksMap.keySet()) {
            final Map<String, Set<ClusterBlock>> indices = indices(projectId);
            if (indices.isEmpty()) {
                continue;
            }
            sb.append("   ").append(projectId).append(":\n");
            for (Map.Entry<String, Set<ClusterBlock>> entry : indices.entrySet()) {
                sb.append("      ").append(entry.getKey()).append(":\n");
                for (ClusterBlock block : entry.getValue()) {
                    sb.append("         ").append(block).append("\n");
                }
            }
        }
        return sb.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
            writeBlockSet(global, out);
            out.writeMap(projectBlocksMap, (o, projectId) -> projectId.writeTo(o), (o, projectBlocks) -> projectBlocks.writeTo(out));
        } else {
            if (defaultProjectOnly()) {
                writeToBwc(out);
            } else {
                throw new IllegalStateException(
                    "Cannot write multi-project blocks to a stream with version [" + out.getTransportVersion() + "]"
                );
            }
        }
    }

    private void writeToBwc(StreamOutput out) throws IOException {
        writeBlockSet(global, out);
        out.writeMap(indices(Metadata.DEFAULT_PROJECT_ID), (o, s) -> writeBlockSet(s, o));
    }

    @Override
    public Diff<ClusterBlocks> diff(ClusterBlocks previousState) {
        if (equals(previousState)) {
            return SimpleDiffable.empty();
        } else {
            return new ClusterBlocksDiff(this, false);
        }
    }

    private boolean defaultProjectOnly() {
        return defaultProjectOnly(projectBlocksMap);
    }

    private static boolean defaultProjectOnly(Map<ProjectId, ?> projectBlocksMap) {
        return projectBlocksMap.size() == 1 && projectBlocksMap.containsKey(Metadata.DEFAULT_PROJECT_ID);
    }

    private void throwIfMultiProjects() {
        if (defaultProjectOnly() == false) {
            throw new Metadata.MultiProjectPendingException("expect only default-project, but got " + projectBlocksMap.keySet());
        }
    }

    private static class ClusterBlocksDiff implements Diff<ClusterBlocks> {

        private final ClusterBlocks part;
        private final boolean isFromBwcNode;

        ClusterBlocksDiff(ClusterBlocks part, boolean isFromBwcNode) {
            this.part = part;
            this.isFromBwcNode = isFromBwcNode;
        }

        @Override
        public ClusterBlocks apply(ClusterBlocks part) {
            if (isFromBwcNode) {
                if (part.defaultProjectOnly()) {
                    return this.part;
                } else {
                    throw new IllegalStateException(
                        "Cannot apply BWC diff to cluster blocks with multiple projects: " + part.projectBlocksMap.keySet()
                    );
                }
            }
            return this.part;
        }

        /**
         * The diff serialization must write a boolean field of {@code true} to indicate more data to follow.
         * This is because we use {@link SimpleDiffable#EMPTY} to represent no difference and the empty diff
         * writes a boolean field of {@code false} to indicate no data. See also {@link #readDiffFrom}
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                out.writeBoolean(true);
                part.writeTo(out);
            } else {
                if (part.defaultProjectOnly()) {
                    out.writeBoolean(true);
                    part.writeToBwc(out);
                } else {
                    throw new IllegalStateException(
                        "Cannot write multi-project blocks diff to a stream with version [" + out.getTransportVersion() + "]"
                    );
                }
            }
        }
    }

    private static void writeBlockSet(Set<ClusterBlock> blocks, StreamOutput out) throws IOException {
        out.writeCollection(blocks);
    }

    public static ClusterBlocks readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
            final Set<ClusterBlock> global = readBlockSet(in);
            final Map<ProjectId, ProjectBlocks> projectBlocksMap = in.readImmutableMap(ProjectId::new, ProjectBlocks::readFrom);
            if (global.isEmpty()
                && defaultProjectOnly(projectBlocksMap)
                && projectBlocksMap.get(Metadata.DEFAULT_PROJECT_ID).indices().isEmpty()) {
                return EMPTY_CLUSTER_BLOCK;
            }
            return new ClusterBlocks(global, projectBlocksMap);
        } else {
            return readFromBwc(in);
        }
    }

    private static ClusterBlocks readFromBwc(StreamInput in) throws IOException {
        final Set<ClusterBlock> global = readBlockSet(in);
        Map<String, Set<ClusterBlock>> indicesBlocks = in.readImmutableMap(i -> i.readString().intern(), ClusterBlocks::readBlockSet);
        if (global.isEmpty() && indicesBlocks.isEmpty()) {
            return EMPTY_CLUSTER_BLOCK;
        }
        return new ClusterBlocks(global, Map.of(Metadata.DEFAULT_PROJECT_ID, new ProjectBlocks(indicesBlocks)));
    }

    private static Set<ClusterBlock> readBlockSet(StreamInput in) throws IOException {
        return in.readCollectionAsImmutableSet(ClusterBlock::new);
    }

    public static Diff<ClusterBlocks> readDiffFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                return new ClusterBlocksDiff(ClusterBlocks.readFrom(in), false);
            } else {
                return new ClusterBlocksDiff(ClusterBlocks.readFromBwc(in), true);
            }
        }
        return SimpleDiffable.empty();
    }

    static class ProjectBlocks implements Writeable {

        static final ProjectBlocks EMPTY = new ProjectBlocks(Map.of());

        private final Map<String, Set<ClusterBlock>> indices;

        ProjectBlocks(Map<String, Set<ClusterBlock>> indices) {
            this.indices = indices;
        }

        Map<String, Set<ClusterBlock>> indices() {
            return indices;
        }

        Set<ClusterBlock> get(String index) {
            return indices.get(index);
        }

        boolean isEmpty() {
            return indices.isEmpty();
        }

        static ProjectBlocks readFrom(StreamInput in) throws IOException {
            return new ProjectBlocks(in.readImmutableMap(i -> i.readString().intern(), ClusterBlocks::readBlockSet));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indices, (o, s) -> writeBlockSet(s, o));
        }
    }

    record ImmutableLevelHolder(Set<ClusterBlock> global, Map<ProjectId, ProjectBlocks> projects) {}

    /**
     * Ensure the ClusterBlocks contain the exact same set of projects specified. If a project
     * does not already exist, it will be added to the ClusterBlocks with an empty ProjectBlocks.
     * Extra projects from the ClusterBlocks are removed. A new ClusterBlocks is returned if
     * there are any changes. Otherwise, the same instance of ClusterBlocks is returned.
     */
    @FixForMultiProject(description = "Consider dropping a project if it does not have any indices blocks?")
    public ClusterBlocks initializeProjects(Set<ProjectId> projectIds) {
        if (projectBlocksMap.keySet().equals(projectIds)) {
            return this;
        } else {
            final Map<ProjectId, ProjectBlocks> newProjectBlocksMap = Maps.newMapWithExpectedSize(projectIds.size());
            projectIds.forEach(
                projectId -> newProjectBlocksMap.put(projectId, projectBlocksMap.getOrDefault(projectId, ProjectBlocks.EMPTY))
            );
            return new ClusterBlocks(global, newProjectBlocksMap);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method, equivalent to: {@code builder().blocks(blocks)}
     */
    public static Builder builder(ClusterBlocks blocks) {
        return builder().blocks(blocks);
    }

    public static class Builder {

        private final Set<ClusterBlock> global = new HashSet<>();

        private final Map<ProjectId, Map<String, Set<ClusterBlock>>> projects = new HashMap<>();

        public Builder() {
            this.projects.put(Metadata.DEFAULT_PROJECT_ID, new HashMap<>());
        }

        public Builder blocks(ClusterBlocks blocks) {
            global.addAll(blocks.global());
            for (var projectId : blocks.projectBlocksMap.keySet()) {
                final var indices = projects.computeIfAbsent(projectId, k -> new HashMap<>());
                for (Map.Entry<String, Set<ClusterBlock>> entry : blocks.indices(projectId).entrySet()) {
                    if (indices.containsKey(entry.getKey()) == false) {
                        indices.put(entry.getKey(), new HashSet<>());
                    }
                    indices.get(entry.getKey()).addAll(entry.getValue());
                }
            }
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder addBlocks(IndexMetadata indexMetadata) {
            return addBlocks(Metadata.DEFAULT_PROJECT_ID, indexMetadata);
        }

        public Builder addBlocks(ProjectId projectId, IndexMetadata indexMetadata) {
            String indexName = indexMetadata.getIndex().getName();
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                addIndexBlock(projectId, indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK);
            }
            if (IndexMetadata.INDEX_READ_ONLY_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(projectId, indexName, IndexMetadata.INDEX_READ_ONLY_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_READ_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(projectId, indexName, IndexMetadata.INDEX_READ_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(projectId, indexName, IndexMetadata.INDEX_WRITE_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_METADATA_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(projectId, indexName, IndexMetadata.INDEX_METADATA_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(projectId, indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            }
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder updateBlocks(IndexMetadata indexMetadata) {
            return updateBlocks(Metadata.DEFAULT_PROJECT_ID, indexMetadata);
        }

        public Builder updateBlocks(ProjectId projectId, IndexMetadata indexMetadata) {
            // let's remove all blocks for this index and add them back -- no need to remove all individual blocks....
            projects.computeIfAbsent(projectId, k -> new HashMap<>()).remove(indexMetadata.getIndex().getName());
            return addBlocks(projectId, indexMetadata);
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

        @Deprecated(forRemoval = true)
        public Builder addIndexBlock(String index, ClusterBlock block) {
            return addIndexBlock(Metadata.DEFAULT_PROJECT_ID, index, block);
        }

        public Builder addIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
            final var indices = projects.computeIfAbsent(projectId, k -> new HashMap<>());
            if (indices.containsKey(index) == false) {
                indices.put(index, new HashSet<>());
            }
            indices.get(index).add(block);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeIndexBlocks(String index) {
            final var indices = projects.computeIfAbsent(Metadata.DEFAULT_PROJECT_ID, k -> new HashMap<>());
            if (indices.containsKey(index) == false) {
                return this;
            }
            indices.remove(index);
            return this;
        }

        @Deprecated(forRemoval = true)
        public boolean hasIndexBlock(String index, ClusterBlock block) {
            return hasIndexBlock(Metadata.DEFAULT_PROJECT_ID, index, block);
        }

        public boolean hasIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
            final var indices = projects.computeIfAbsent(projectId, k -> new HashMap<>());
            return indices.getOrDefault(index, Set.of()).contains(block);
        }

        @Deprecated(forRemoval = true)
        public Builder removeIndexBlock(String index, ClusterBlock block) {
            return removeIndexBlock(Metadata.DEFAULT_PROJECT_ID, index, block);
        }

        public Builder removeIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
            final var indices = projects.computeIfAbsent(projectId, k -> new HashMap<>());
            if (indices.containsKey(index) == false) {
                return this;
            }
            indices.get(index).remove(block);
            if (indices.get(index).isEmpty()) {
                indices.remove(index);
            }
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeIndexBlockWithId(String index, int blockId) {
            final var indices = projects.computeIfAbsent(Metadata.DEFAULT_PROJECT_ID, k -> new HashMap<>());
            final Set<ClusterBlock> indexBlocks = indices.get(index);
            if (indexBlocks == null) {
                return this;
            }
            indexBlocks.removeIf(block -> block.id() == blockId);
            if (indexBlocks.isEmpty()) {
                indices.remove(index);
            }
            return this;
        }

        @FixForMultiProject(description = "More efficient build when all projects have empty indices blocks")
        public ClusterBlocks build() {
            if (global.isEmpty() && defaultProjectOnly(projects) && projects.get(Metadata.DEFAULT_PROJECT_ID).isEmpty()) {
                return EMPTY_CLUSTER_BLOCK;
            }
            // We copy the block sets here in case of the builder is modified after build is called
            Map<ProjectId, ProjectBlocks> projectsBuilder = new HashMap<>(projects.size());
            for (Map.Entry<ProjectId, Map<String, Set<ClusterBlock>>> projectEntry : projects.entrySet()) {
                Map<String, Set<ClusterBlock>> indicesBuilder = new HashMap<>(projectEntry.getValue());
                for (Map.Entry<String, Set<ClusterBlock>> indexEntry : projectEntry.getValue().entrySet()) {
                    indicesBuilder.put(indexEntry.getKey(), Set.copyOf(indexEntry.getValue()));
                }
                projectsBuilder.put(projectEntry.getKey(), new ProjectBlocks(Map.copyOf(indicesBuilder)));
            }
            return new ClusterBlocks(Set.copyOf(global), Map.copyOf(projectsBuilder));
        }
    }
}

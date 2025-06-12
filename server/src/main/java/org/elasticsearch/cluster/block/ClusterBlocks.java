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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
     * A project has an entry in this map only if it has any indices blocks. Therefore, it is possible
     * for this map to be entirely empty, not even including the {@link Metadata#DEFAULT_PROJECT_ID}
     * if no project has any indices blocks. All methods for indices blocks should return empty
     * result when the provided project does not have an entry in this map.
     * <p>
     * For stateful, the only possible entry is {@link Metadata#DEFAULT_PROJECT_ID}.
     */
    // Package private for testing
    final Map<ProjectId, ProjectBlocks> projectBlocksMap;

    private final EnumMap<ClusterBlockLevel, ImmutableLevelHolder> levelHolders;

    ClusterBlocks(Set<ClusterBlock> global, Map<ProjectId, ProjectBlocks> projectBlocksMap) {
        this.global = global;
        assert projectBlocksMap.values().stream().allMatch(projectBlocks -> projectBlocks.isEmpty() == false)
            : "Map must not contain projects with empty blocks " + projectBlocksMap;
        this.projectBlocksMap = projectBlocksMap;
        this.levelHolders = generateLevelHolders(global, projectBlocksMap);
    }

    public Set<ClusterBlock> global() {
        return global;
    }

    // Review note: project-aware flavor of global() used in TransportSearch and TransportFieldCaps which were already project-aware
    public Set<ClusterBlock> global(ProjectId projectId) {
        return Sets.union(global, projectBlocksMap.getOrDefault(projectId, ProjectBlocks.EMPTY).projectGlobals());
    }

    public boolean noProjectBlockAllProjects() {
        return projectBlocksMap.values().stream().allMatch(ProjectBlocks::isEmpty);
    }

    public Map<String, Set<ClusterBlock>> indices(ProjectId projectId) {
        return projectBlocksMap.getOrDefault(projectId, ProjectBlocks.EMPTY).indices();
    }

    public ProjectBlocks projectBlocks(ProjectId projectId) {
        return projectBlocksMap.getOrDefault(projectId, ProjectBlocks.EMPTY);
    }

    protected Set<ClusterBlock> projectGlobal(ProjectId projectId) {
        return projectBlocksMap.getOrDefault(projectId, ProjectBlocks.EMPTY).projectGlobals();
    }

    public Set<ClusterBlock> global(ClusterBlockLevel level) {
        return levelHolders.get(level).global();
    }

    // For a specific project, global could be either project global or cluster global
    // Review note: changed TransportAddIndexBlock to use this since it is project-aware
    public Set<ClusterBlock> global(ProjectId projectId, ClusterBlockLevel level) {
        var levelHolder = levelHolders.get(level);
        return Sets.union(levelHolder.global, levelHolder.projects.getOrDefault(projectId, ProjectBlocks.EMPTY).projectGlobals());
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
                var projectGlobals = addBlocksAtLevel(projectEntry.getValue().projectGlobals(), scratch, level);
                projectsBuilder.put(projectEntry.getKey(), new ProjectBlocks(Map.copyOf(indicesBuilder), projectGlobals));
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

    // Review note: seems to be used only with STATE_NOT_RECOVERED by different node-level services. May or may not need to
    // consider project globals once/if we have a project block that is similar to STATE_NOT_RECOVERED for project creation/resurrection
    // time (essentially something similar to PROJECT_UNDER_DELETION_BLOCK, but retryable).
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

    // Review note: used by monitoring/localExporter and watcher, both currently not project aware.
    public boolean hasGlobalBlockWithLevel(ClusterBlockLevel level) {
        return global(level).size() > 0;
    }

    // Review note: Used by TransportAbstractBulk which is already project-aware
    public boolean hasGlobalBlockWithLevel(ProjectId projectId, ClusterBlockLevel level) {
        return global(projectId, level).size() > 0;
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

    // Review note: Not changing index-specific checks.
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

    // Review note: Not changing index-specific checks.
    public boolean hasIndexBlockLevel(ProjectId projectId, String index, ClusterBlockLevel level) {
        return blocksForIndex(projectId, level, index).isEmpty() == false;
    }

    // Review note: Not changing index-specific checks.
    public boolean hasIndexBlockWithId(ProjectId projectId, String index, int blockId) {
        return getIndexBlockWithId(projectId, index, blockId) != null;
    }

    // Review note: Not changing index-specific checks.
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

    // Review note: only a watcher action uses this since it is not project-aware
    public void globalBlockedRaiseException(ClusterBlockLevel level) throws ClusterBlockException {
        ClusterBlockException blockException = globalBlockedException(level);
        if (blockException != null) {
            throw blockException;
        }
    }

    // Review note: All new callers seemed project-aware/scoped actions.
    public void globalBlockedRaiseException(ProjectId projectId, ClusterBlockLevel level) throws ClusterBlockException {
        ClusterBlockException blockException = globalBlockedException(projectId, level);
        if (blockException != null) {
            throw blockException;
        }
    }

    private boolean globalBlocked(ClusterBlockLevel level) {
        return global(level).isEmpty() == false;
    }

    private boolean globalBlocked(ProjectId projectId, ClusterBlockLevel level) {
        return global(projectId, level).isEmpty() == false;
    }

    // Review note: this has some 200 callers mostly Transport actions. Not sure but maybe I have to
    // review callers and update those that are project-scoped. TransportGetDataStreamLifecycle is one example
    // that i've updated here, although many don't seem to be user facing or available
    // directly in serverless. Could also be a follow up to update the callers.
    public ClusterBlockException globalBlockedException(ClusterBlockLevel level) {
        if (globalBlocked(level) == false) {
            return null;
        }
        return new ClusterBlockException(global(level));
    }

    public ClusterBlockException globalBlockedException(ProjectId projectId, ClusterBlockLevel level) {
        if (globalBlocked(projectId, level) == false) {
            return null;
        }
        return new ClusterBlockException(global(projectId, level));
    }

    public void indexBlockedRaiseException(ProjectId projectId, ClusterBlockLevel level, String index) throws ClusterBlockException {
        // Not throw for multi-project for now to avoid wide-spread cascading changes.
        ClusterBlockException blockException = indexBlockedException(projectId, level, index);
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
        return globalBlocked(projectId, level) || blocksForIndex(projectId, level, index).isEmpty() == false;
    }

    @Deprecated(forRemoval = true)
    public ClusterBlockException indicesBlockedException(ClusterBlockLevel level, String[] indices) {
        // Not throw for multi-project for now to avoid wide-spread cascading changes.
        return indicesBlockedException(Metadata.DEFAULT_PROJECT_ID, level, indices);
    }

    public ClusterBlockException indicesBlockedException(ProjectId projectId, ClusterBlockLevel level, String[] indices) {
        Set<ClusterBlock> globalLevelBlocks = global(projectId, level);
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
     * Returns <code>true</code> iff none of the given indices have a {@link ClusterBlockLevel#METADATA_WRITE} in place where the
     * {@link ClusterBlock#isAllowReleaseResources()} returns <code>false</code>. This is used in places where resources will be released
     * like the deletion of an index to free up resources on nodes.
     *
     * @param projectId the project that owns the indices
     * @param indices   the indices to check
     */
    public ClusterBlockException indicesAllowReleaseResources(ProjectId projectId, String[] indices) {
        Set<ClusterBlock> globalBlocks = global(projectId, ClusterBlockLevel.METADATA_WRITE).stream()
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
        if (global.isEmpty() && noProjectBlockAllProjects()) {
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
        for (var projectId : projectBlocksMap.keySet().stream().sorted(Comparator.comparing(ProjectId::id)).toList()) {
            final Map<String, Set<ClusterBlock>> indices = indices(projectId);
            sb.append("   ").append(projectId).append(":\n");
            for (ClusterBlock block : projectGlobal(projectId)) {
                sb.append("      ").append(block).append("\n");
            }
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
            if (noProjectOrDefaultProjectOnly()) {
                writeToSingleProjectNode(out);
            } else {
                throw new IllegalStateException(
                    "Cannot write multi-project blocks to a stream with version [" + out.getTransportVersion() + "]"
                );
            }
        }
    }

    private void writeToSingleProjectNode(StreamOutput out) throws IOException {
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

    private boolean noProjectOrDefaultProjectOnly() {
        return noProjectOrDefaultProjectOnly(projectBlocksMap);
    }

    private static boolean noProjectOrDefaultProjectOnly(Map<ProjectId, ?> projectBlocksMap) {
        return projectBlocksMap.isEmpty() || (projectBlocksMap.size() == 1 && projectBlocksMap.containsKey(Metadata.DEFAULT_PROJECT_ID));
    }

    private void throwIfMultiProjects() {
        if (noProjectOrDefaultProjectOnly() == false) {
            throw new Metadata.MultiProjectPendingException(
                "expect no project or only the default-project, but got " + projectBlocksMap.keySet()
            );
        }
    }

    private static class ClusterBlocksDiff implements Diff<ClusterBlocks> {

        private final ClusterBlocks part;
        private final boolean isFromSingleProjectNode;

        ClusterBlocksDiff(ClusterBlocks part, boolean isFromSingleProjectNode) {
            this.part = part;
            this.isFromSingleProjectNode = isFromSingleProjectNode;
        }

        @Override
        public ClusterBlocks apply(ClusterBlocks part) {
            if (isFromSingleProjectNode) {
                if (part.noProjectOrDefaultProjectOnly()) {
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
                if (part.noProjectOrDefaultProjectOnly()) {
                    out.writeBoolean(true);
                    part.writeToSingleProjectNode(out);
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
            final Map<ProjectId, ProjectBlocks> projectBlocksMap = in.readImmutableMap(ProjectId::readFrom, ProjectBlocks::readFrom);
            if (global.isEmpty()
                && noProjectOrDefaultProjectOnly(projectBlocksMap)
                && projectBlocksMap.getOrDefault(Metadata.DEFAULT_PROJECT_ID, ProjectBlocks.EMPTY).indices().isEmpty()) {
                return EMPTY_CLUSTER_BLOCK;
            }
            return new ClusterBlocks(global, projectBlocksMap);
        } else {
            return readFromSingleProjectNode(in);
        }
    }

    private static ClusterBlocks readFromSingleProjectNode(StreamInput in) throws IOException {
        final Set<ClusterBlock> global = readBlockSet(in);
        Map<String, Set<ClusterBlock>> indicesBlocks = in.readImmutableMap(i -> i.readString().intern(), ClusterBlocks::readBlockSet);
        if (global.isEmpty() && indicesBlocks.isEmpty()) {
            return EMPTY_CLUSTER_BLOCK;
        }
        if (indicesBlocks.isEmpty()) {
            return new ClusterBlocks(global, Map.of());
        }
        return new ClusterBlocks(global, Map.of(Metadata.DEFAULT_PROJECT_ID, new ProjectBlocks(indicesBlocks, Set.of())));
    }

    private static Set<ClusterBlock> readBlockSet(StreamInput in) throws IOException {
        return in.readCollectionAsImmutableSet(ClusterBlock::new);
    }

    public static Diff<ClusterBlocks> readDiffFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
                return new ClusterBlocksDiff(ClusterBlocks.readFrom(in), false);
            } else {
                return new ClusterBlocksDiff(ClusterBlocks.readFromSingleProjectNode(in), true);
            }
        }
        return SimpleDiffable.empty();
    }

    /**
     * ProjectBlocks encapsulates the project-specific ClusterBlocks. These apply either to a specific index
     * or are project global blocks. Project global blocks are similar to cluster-global blocks, but impact
     * only one project.
     */
    public static class ProjectBlocks implements Writeable {

        static final ProjectBlocks EMPTY = new ProjectBlocks(Map.of(), Set.of());

        private final Map<String, Set<ClusterBlock>> indices;

        private final Set<ClusterBlock> projectGlobal;

        ProjectBlocks(Map<String, Set<ClusterBlock>> indices, Set<ClusterBlock> projectGlobal) {
            this.indices = indices;
            this.projectGlobal = projectGlobal;
        }

        static ProjectBlocks emptyMutable() {
            return new ProjectBlocks(new HashMap<>(), new HashSet<>());
        }

        public Map<String, Set<ClusterBlock>> indices() {
            return indices;
        }

        public Set<ClusterBlock> projectGlobals() {
            return projectGlobal;
        }

        Set<ClusterBlock> get(String index) {
            return indices.get(index);
        }

        public boolean isEmpty() {
            return indices.isEmpty() && projectGlobal.isEmpty();
        }

        static ProjectBlocks readFrom(StreamInput in) throws IOException {
            Map<String, Set<ClusterBlock>> indices = in.readImmutableMap(i -> i.readString().intern(), ClusterBlocks::readBlockSet);
            Set<ClusterBlock> projectGlobal = new HashSet<>();
            if (in.getTransportVersion().onOrAfter(TransportVersions.PROJECT_DELETION_GLOBAL_BLOCK)) {
                projectGlobal = ClusterBlocks.readBlockSet(in);
            }
            return new ProjectBlocks(indices, projectGlobal);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indices, (o, s) -> writeBlockSet(s, o));
            if (out.getTransportVersion().onOrAfter(TransportVersions.PROJECT_DELETION_GLOBAL_BLOCK)) {
                writeBlockSet(projectGlobal, out);
            }
        }
    }

    record ImmutableLevelHolder(Set<ClusterBlock> global, Map<ProjectId, ProjectBlocks> projects) {}

    /**
     * Ensure all projects that the ClusterBlocks contains are found in the provided projects
     * by removing any project that does not exist in the provided set.
     *
     * @param projectIds The set of project-ids from {@link Metadata}.
     * @return A new ClusterBlocks is returned if there are any changes. Otherwise, the same instance of
     * ClusterBlocks is returned.
     */
    public ClusterBlocks initializeProjects(Set<ProjectId> projectIds) {
        if (projectIds.containsAll(projectBlocksMap.keySet())) {
            return this;
        } else {
            final Builder builder = ClusterBlocks.builder(this);
            Sets.difference(projectBlocksMap.keySet(), projectIds).forEach(builder::removeProject);
            return builder.build();
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
        private final Map<ProjectId, ProjectBlocks> projects = new HashMap<>();

        public Builder() {}

        public Builder blocks(ClusterBlocks blocks) {
            global.addAll(blocks.global());
            for (var projectId : blocks.projectBlocksMap.keySet()) {
                final var projectBlocks = projects.computeIfAbsent(projectId, k -> ProjectBlocks.emptyMutable());
                projectBlocks.projectGlobal.addAll(blocks.projectGlobal(projectId));
                for (Map.Entry<String, Set<ClusterBlock>> entry : blocks.indices(projectId).entrySet()) {
                    if (projectBlocks.indices.containsKey(entry.getKey()) == false) {
                        projectBlocks.indices.put(entry.getKey(), new HashSet<>());
                    }
                    projectBlocks.indices.get(entry.getKey()).addAll(entry.getValue());
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
            projects.computeIfAbsent(projectId, k -> ProjectBlocks.emptyMutable()).indices.remove(indexMetadata.getIndex().getName());
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

        public Builder removeProject(ProjectId projectId) {
            projects.remove(projectId);
            return this;
        }

        public Builder addProjectGlobalBlock(ProjectId projectId, ClusterBlock block) {
            assert projectId.equals(ProjectId.DEFAULT) == false;
            projects.computeIfAbsent(projectId, k -> ProjectBlocks.emptyMutable()).projectGlobal.add(block);
            return this;
        }

        public Builder removeProjectGlobalBlock(ProjectId projectId, ClusterBlock block) {
            var project = projects.get(projectId);
            if (project != null) {
                project.projectGlobal.remove(block);
            }
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder addIndexBlock(String index, ClusterBlock block) {
            return addIndexBlock(Metadata.DEFAULT_PROJECT_ID, index, block);
        }

        public Builder addIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
            final var projectBlocks = projects.computeIfAbsent(projectId, k -> ProjectBlocks.emptyMutable());
            if (projectBlocks.indices().containsKey(index) == false) {
                projectBlocks.indices.put(index, new HashSet<>());
            }
            projectBlocks.indices.get(index).add(block);
            return this;
        }

        @Deprecated(forRemoval = true)
        public Builder removeIndexBlocks(String index) {
            return removeIndexBlocks(Metadata.DEFAULT_PROJECT_ID, index);
        }

        public Builder removeIndexBlocks(ProjectId projectId, String index) {
            final var projectBlocks = projects.get(projectId);
            if (projectBlocks == null) {
                return this;
            }
            if (projectBlocks.indices.containsKey(index) == false) {
                return this;
            }
            projectBlocks.indices.remove(index);
            return this;
        }

        public boolean hasIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
            final var projectBlocks = projects.get(projectId);
            if (projectBlocks == null) {
                return false;
            }
            return projectBlocks.indices.getOrDefault(index, Set.of()).contains(block);
        }

        public boolean hasIndexBlockLevel(ProjectId projectId, String index, ClusterBlockLevel level) {
            final var projectBlocks = projects.get(projectId);
            if (projectBlocks == null) {
                return false;
            }
            return projectBlocks.indices.getOrDefault(index, Set.of()).stream().anyMatch(clusterBlock -> clusterBlock.contains(level));
        }

        public Builder removeIndexBlock(ProjectId projectId, String index, ClusterBlock block) {
            final var projectBlocks = projects.get(projectId);
            if (projectBlocks == null) {
                return this;
            }

            if (projectBlocks.indices.containsKey(index) == false) {
                return this;
            }
            projectBlocks.get(index).remove(block);
            if (projectBlocks.get(index).isEmpty()) {
                projectBlocks.indices.remove(index);
            }
            return this;
        }

        public Builder removeIndexBlockWithId(ProjectId projectId, String index, int blockId) {
            final var projectBlocks = projects.get(projectId);
            if (projectBlocks == null) {
                return this;
            }

            final Set<ClusterBlock> indexBlocks = projectBlocks.get(index);
            if (indexBlocks == null) {
                return this;
            }
            indexBlocks.removeIf(block -> block.id() == blockId);
            if (indexBlocks.isEmpty()) {
                projectBlocks.indices.remove(index);
            }
            return this;
        }

        public ClusterBlocks build() {
            if (global.isEmpty()
                && noProjectOrDefaultProjectOnly(projects)
                && projects.getOrDefault(Metadata.DEFAULT_PROJECT_ID, ProjectBlocks.EMPTY).isEmpty()) {
                return EMPTY_CLUSTER_BLOCK;
            }
            // We copy the block sets here in case of the builder is modified after build is called
            Map<ProjectId, ProjectBlocks> projectsBuilder = new HashMap<>(projects.size());
            for (Map.Entry<ProjectId, ProjectBlocks> projectEntry : projects.entrySet()) {
                Map<String, Set<ClusterBlock>> indicesBuilder = new HashMap<>(projectEntry.getValue().indices.size());
                for (Map.Entry<String, Set<ClusterBlock>> indexEntry : projectEntry.getValue().indices.entrySet()) {
                    indicesBuilder.put(indexEntry.getKey(), Set.copyOf(indexEntry.getValue()));
                }
                if (indicesBuilder.isEmpty() == false || projectEntry.getValue().projectGlobals().isEmpty() == false) {
                    projectsBuilder.put(
                        projectEntry.getKey(),
                        new ProjectBlocks(Map.copyOf(indicesBuilder), Set.copyOf(projectEntry.getValue().projectGlobals()))
                    );
                }
            }
            return new ClusterBlocks(Set.copyOf(global), Map.copyOf(projectsBuilder));
        }
    }
}

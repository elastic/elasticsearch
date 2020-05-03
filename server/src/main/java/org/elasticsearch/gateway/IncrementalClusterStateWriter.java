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
package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Tracks the metadata written to disk, allowing updated metadata to be written incrementally (i.e. only writing out the changed metadata).
 */
public class IncrementalClusterStateWriter {

    private static final Logger logger = LogManager.getLogger(IncrementalClusterStateWriter.class);

    private final MetaStateService metaStateService;

    // We call updateClusterState on the (unique) cluster applier thread so there's no need to synchronize access to these fields.
    private Manifest previousManifest;
    private ClusterState previousClusterState;
    private final LongSupplier relativeTimeMillisSupplier;
    private boolean incrementalWrite;

    private volatile TimeValue slowWriteLoggingThreshold;

    IncrementalClusterStateWriter(Settings settings, ClusterSettings clusterSettings, MetaStateService metaStateService, Manifest manifest,
                                  ClusterState clusterState, LongSupplier relativeTimeMillisSupplier) {
        this.metaStateService = metaStateService;
        this.previousManifest = manifest;
        this.previousClusterState = clusterState;
        this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
        this.incrementalWrite = false;
        this.slowWriteLoggingThreshold = PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD,
            this::setSlowWriteLoggingThreshold);
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    void setCurrentTerm(long currentTerm) throws WriteStateException {
        Manifest manifest = new Manifest(currentTerm, previousManifest.getClusterStateVersion(), previousManifest.getGlobalGeneration(),
            new HashMap<>(previousManifest.getIndexGenerations()));
        metaStateService.writeManifestAndCleanup("current term changed", manifest);
        previousManifest = manifest;
    }

    Manifest getPreviousManifest() {
        return previousManifest;
    }

    void setIncrementalWrite(boolean incrementalWrite) {
        this.incrementalWrite = incrementalWrite;
    }

    /**
     * Updates manifest and meta data on disk.
     *
     * @param newState new {@link ClusterState}
     *
     * @throws WriteStateException if exception occurs. See also {@link WriteStateException#isDirty()}.
     */
    void updateClusterState(ClusterState newState) throws WriteStateException {
        Metadata newMetadata = newState.metadata();

        final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();

        final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, previousManifest);
        long globalStateGeneration = writeGlobalState(writer, newMetadata);
        Map<Index, Long> indexGenerations = writeIndicesMetadata(writer, newState);
        Manifest manifest = new Manifest(previousManifest.getCurrentTerm(), newState.version(), globalStateGeneration, indexGenerations);
        writeManifest(writer, manifest);
        previousManifest = manifest;
        previousClusterState = newState;

        final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
        final TimeValue finalSlowWriteLoggingThreshold = this.slowWriteLoggingThreshold;
        if (durationMillis >= finalSlowWriteLoggingThreshold.getMillis()) {
            logger.warn("writing cluster state took [{}ms] which is above the warn threshold of [{}]; " +
                    "wrote metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis, finalSlowWriteLoggingThreshold, writer.getIndicesWritten(), writer.getIndicesSkipped());
        } else {
            logger.debug("writing cluster state took [{}ms]; wrote metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis, writer.getIndicesWritten(), writer.getIndicesSkipped());
        }
    }

    private void writeManifest(AtomicClusterStateWriter writer, Manifest manifest) throws WriteStateException {
        if (manifest.equals(previousManifest) == false) {
            writer.writeManifestAndCleanup("changed", manifest);
        }
    }

    private Map<Index, Long> writeIndicesMetadata(AtomicClusterStateWriter writer, ClusterState newState)
        throws WriteStateException {
        Map<Index, Long> previouslyWrittenIndices = previousManifest.getIndexGenerations();
        Set<Index> relevantIndices = getRelevantIndices(newState);

        Map<Index, Long> newIndices = new HashMap<>();

        Metadata previousMetadata = incrementalWrite ? previousClusterState.metadata() : null;
        Iterable<IndexMetadataAction> actions = resolveIndexMetadataActions(previouslyWrittenIndices, relevantIndices, previousMetadata,
            newState.metadata());

        for (IndexMetadataAction action : actions) {
            long generation = action.execute(writer);
            newIndices.put(action.getIndex(), generation);
        }

        return newIndices;
    }

    private long writeGlobalState(AtomicClusterStateWriter writer, Metadata newMetadata) throws WriteStateException {
        if (incrementalWrite == false || Metadata.isGlobalStateEquals(previousClusterState.metadata(), newMetadata) == false) {
            return writer.writeGlobalState("changed", newMetadata);
        }
        return previousManifest.getGlobalGeneration();
    }


    /**
     * Returns list of {@link IndexMetadataAction} for each relevant index.
     * For each relevant index there are 3 options:
     * <ol>
     * <li>
     * {@link KeepPreviousGeneration} - index metadata is already stored to disk and index metadata version is not changed, no
     * action is required.
     * </li>
     * <li>
     * {@link WriteNewIndexMetadata} - there is no index metadata on disk and index metadata for this index should be written.
     * </li>
     * <li>
     * {@link WriteChangedIndexMetadata} - index metadata is already on disk, but index metadata version has changed. Updated
     * index metadata should be written to disk.
     * </li>
     * </ol>
     *
     * @param previouslyWrittenIndices A list of indices for which the state was already written before
     * @param relevantIndices          The list of indices for which state should potentially be written
     * @param previousMetadata         The last meta data we know of
     * @param newMetadata              The new metadata
     * @return list of {@link IndexMetadataAction} for each relevant index.
     */
    // exposed for tests
    static List<IndexMetadataAction> resolveIndexMetadataActions(Map<Index, Long> previouslyWrittenIndices,
                                                                 Set<Index> relevantIndices,
                                                                 Metadata previousMetadata,
                                                                 Metadata newMetadata) {
        List<IndexMetadataAction> actions = new ArrayList<>();
        for (Index index : relevantIndices) {
            IndexMetadata newIndexMetadata = newMetadata.getIndexSafe(index);
            IndexMetadata previousIndexMetadata = previousMetadata == null ? null : previousMetadata.index(index);

            if (previouslyWrittenIndices.containsKey(index) == false || previousIndexMetadata == null) {
                actions.add(new WriteNewIndexMetadata(newIndexMetadata));
            } else if (previousIndexMetadata.getVersion() != newIndexMetadata.getVersion()) {
                actions.add(new WriteChangedIndexMetadata(previousIndexMetadata, newIndexMetadata));
            } else {
                actions.add(new KeepPreviousGeneration(index, previouslyWrittenIndices.get(index)));
            }
        }
        return actions;
    }

    // exposed for tests
    static Set<Index> getRelevantIndices(ClusterState state) {
        assert state.nodes().getLocalNode().isDataNode();
        final RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        final Set<Index> indices = new HashSet<>();
        for (final ShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        return indices;
    }

    /**
     * Action to perform with index metadata.
     */
    interface IndexMetadataAction {
        /**
         * @return index for index metadata.
         */
        Index getIndex();

        /**
         * Executes this action using provided {@link AtomicClusterStateWriter}.
         *
         * @return new index metadata state generation, to be used in manifest file.
         * @throws WriteStateException if exception occurs.
         */
        long execute(AtomicClusterStateWriter writer) throws WriteStateException;
    }

    /**
     * This class is used to write changed global {@link Metadata}, {@link IndexMetadata} and {@link Manifest} to disk.
     * This class delegates <code>write*</code> calls to corresponding write calls in {@link MetaStateService} and
     * additionally it keeps track of cleanup actions to be performed if transaction succeeds or fails.
     */
    static class AtomicClusterStateWriter {
        private static final String FINISHED_MSG = "AtomicClusterStateWriter is finished";
        private final List<Runnable> commitCleanupActions;
        private final List<Runnable> rollbackCleanupActions;
        private final Manifest previousManifest;
        private final MetaStateService metaStateService;
        private boolean finished;

        private int indicesWritten;
        private int indicesSkipped;

        AtomicClusterStateWriter(MetaStateService metaStateService, Manifest previousManifest) {
            this.metaStateService = metaStateService;
            assert previousManifest != null;
            this.previousManifest = previousManifest;
            this.commitCleanupActions = new ArrayList<>();
            this.rollbackCleanupActions = new ArrayList<>();
            this.finished = false;
        }

        long writeGlobalState(String reason, Metadata metadata) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                rollbackCleanupActions.add(() -> metaStateService.cleanupGlobalState(previousManifest.getGlobalGeneration()));
                long generation = metaStateService.writeGlobalState(reason, metadata);
                commitCleanupActions.add(() -> metaStateService.cleanupGlobalState(generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeIndex(String reason, IndexMetadata metadata) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                Index index = metadata.getIndex();
                Long previousGeneration = previousManifest.getIndexGenerations().get(index);
                if (previousGeneration != null) {
                    // we prefer not to clean-up index metadata in case of rollback,
                    // if it's not referenced by previous manifest file
                    // not to break dangling indices functionality
                    rollbackCleanupActions.add(() -> metaStateService.cleanupIndex(index, previousGeneration));
                }
                long generation = metaStateService.writeIndex(reason, metadata);
                commitCleanupActions.add(() -> metaStateService.cleanupIndex(index, generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        void writeManifestAndCleanup(String reason, Manifest manifest) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                metaStateService.writeManifestAndCleanup(reason, manifest);
                commitCleanupActions.forEach(Runnable::run);
                finished = true;
            } catch (WriteStateException e) {
                // If the Manifest write results in a dirty WriteStateException it's not safe to roll back, removing the new metadata files,
                // because if the Manifest was actually written to disk and its deletion fails it will reference these new metadata files.
                // On master-eligible nodes a dirty WriteStateException here is fatal to the node since we no longer really have any idea
                // what the state on disk is and the only sensible response is to start again from scratch.
                if (e.isDirty() == false) {
                    rollback();
                }
                throw e;
            }
        }

        void rollback() {
            rollbackCleanupActions.forEach(Runnable::run);
            finished = true;
        }

        void incrementIndicesWritten() {
            indicesWritten++;
        }

        void incrementIndicesSkipped() {
            indicesSkipped++;
        }

        int getIndicesWritten() {
            return indicesWritten;
        }

        int getIndicesSkipped() {
            return indicesSkipped;
        }
    }

    static class KeepPreviousGeneration implements IndexMetadataAction {
        private final Index index;
        private final long generation;

        KeepPreviousGeneration(Index index, long generation) {
            this.index = index;
            this.generation = generation;
        }

        @Override
        public Index getIndex() {
            return index;
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) {
            writer.incrementIndicesSkipped();
            return generation;
        }
    }

    static class WriteNewIndexMetadata implements IndexMetadataAction {
        private final IndexMetadata indexMetadata;

        WriteNewIndexMetadata(IndexMetadata indexMetadata) {
            this.indexMetadata = indexMetadata;
        }

        @Override
        public Index getIndex() {
            return indexMetadata.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            writer.incrementIndicesWritten();
            return writer.writeIndex("freshly created", indexMetadata);
        }
    }

    static class WriteChangedIndexMetadata implements IndexMetadataAction {
        private final IndexMetadata newIndexMetadata;
        private final IndexMetadata oldIndexMetadata;

        WriteChangedIndexMetadata(IndexMetadata oldIndexMetadata, IndexMetadata newIndexMetadata) {
            this.oldIndexMetadata = oldIndexMetadata;
            this.newIndexMetadata = newIndexMetadata;
        }

        @Override
        public Index getIndex() {
            return newIndexMetadata.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            writer.incrementIndicesWritten();
            return writer.writeIndex(
                    "version changed from [" + oldIndexMetadata.getVersion() + "] to [" + newIndexMetadata.getVersion() + "]",
                    newIndexMetadata);
        }
    }
}

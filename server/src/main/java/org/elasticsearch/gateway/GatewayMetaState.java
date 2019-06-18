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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * This class is responsible for storing/retrieving metadata to/from disk.
 * When instance of this class is created, constructor ensures that this version is compatible with state stored on disk and performs
 * state upgrade if necessary. Also it checks that atomic move is supported on the filesystem level, because it's a must for metadata
 * store algorithm.
 * Please note that the state being loaded when constructing the instance of this class is NOT the state that will be used as a
 * {@link ClusterState#metaData()}. Instead when node is starting up, it calls {@link #getMetaData()} method and if this node is
 * elected as master, it requests metaData from other master eligible nodes. After that, master node performs re-conciliation on the
 * gathered results, re-creates {@link ClusterState} and broadcasts this state to other nodes in the cluster.
 */
public class GatewayMetaState implements ClusterStateApplier, CoordinationState.PersistedState {
    protected static final Logger logger = LogManager.getLogger(GatewayMetaState.class);

    private final MetaStateService metaStateService;
    private final Settings settings;
    private final ClusterService clusterService;
    private final TransportService transportService;

    //there is a single thread executing updateClusterState calls, hence no volatile modifier
    protected Manifest previousManifest;
    protected ClusterState previousClusterState;
    protected boolean incrementalWrite;

    public GatewayMetaState(Settings settings, MetaStateService metaStateService,
                            MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader,
                            TransportService transportService, ClusterService clusterService) throws IOException {
        this.settings = settings;
        this.metaStateService = metaStateService;
        this.transportService = transportService;
        this.clusterService = clusterService;

        upgradeMetaData(metaDataIndexUpgradeService, metaDataUpgrader);
        initializeClusterState(ClusterName.CLUSTER_NAME_SETTING.get(settings));
        incrementalWrite = false;
    }

    public PersistedState getPersistedState(Settings settings, ClusterApplierService clusterApplierService) {
        applyClusterStateUpdaters();
        if (DiscoveryNode.isMasterNode(settings) == false) {
            // use Zen1 way of writing cluster state for non-master-eligible nodes
            // this avoids concurrent manipulating of IndexMetadata with IndicesStore
            clusterApplierService.addLowPriorityApplier(this);
            return new InMemoryPersistedState(getCurrentTerm(), getLastAcceptedState());
        }
        return this;
    }

    private void initializeClusterState(ClusterName clusterName) throws IOException {
        long startNS = System.nanoTime();
        Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
        previousManifest = manifestAndMetaData.v1();

        final MetaData metaData = manifestAndMetaData.v2();

        previousClusterState = ClusterState.builder(clusterName)
                .version(previousManifest.getClusterStateVersion())
                .metaData(metaData).build();

        logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
    }

    public void applyClusterStateUpdaters() {
        assert previousClusterState.nodes().getLocalNode() == null : "applyClusterStateUpdaters must only be called once";
        assert transportService.getLocalNode() != null : "transport service is not yet started";

        previousClusterState = Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, transportService.getLocalNode()))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(previousClusterState);
    }

    protected void upgradeMetaData(MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader)
            throws IOException {
        if (isMasterOrDataNode()) {
            try {
                final Tuple<Manifest, MetaData> metaStateAndData = metaStateService.loadFullState();
                final Manifest manifest = metaStateAndData.v1();
                final MetaData metaData = metaStateAndData.v2();

                // We finished global state validation and successfully checked all indices for backward compatibility
                // and found no non-upgradable indices, which means the upgrade can continue.
                // Now it's safe to overwrite global and index metadata.
                // We don't re-write metadata if it's not upgraded by upgrade plugins, because
                // if there is manifest file, it means metadata is properly persisted to all data paths
                // if there is no manifest file (upgrade from 6.x to 7.x) metadata might be missing on some data paths,
                // but anyway we will re-write it as soon as we receive first ClusterState
                final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, manifest);
                final MetaData upgradedMetaData = upgradeMetaData(metaData, metaDataIndexUpgradeService, metaDataUpgrader);

                final long globalStateGeneration;
                if (MetaData.isGlobalStateEquals(metaData, upgradedMetaData) == false) {
                    globalStateGeneration = writer.writeGlobalState("upgrade", upgradedMetaData);
                } else {
                    globalStateGeneration = manifest.getGlobalGeneration();
                }

                Map<Index, Long> indices = new HashMap<>(manifest.getIndexGenerations());
                for (IndexMetaData indexMetaData : upgradedMetaData) {
                    if (metaData.hasIndexMetaData(indexMetaData) == false) {
                        final long generation = writer.writeIndex("upgrade", indexMetaData);
                        indices.put(indexMetaData.getIndex(), generation);
                    }
                }

                final Manifest newManifest = new Manifest(manifest.getCurrentTerm(), manifest.getClusterStateVersion(),
                        globalStateGeneration, indices);
                writer.writeManifestAndCleanup("startup", newManifest);
            } catch (Exception e) {
                logger.error("failed to read or upgrade local state, exiting...", e);
                throw e;
            }
        }
    }

    private boolean isMasterOrDataNode() {
        return DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings);
    }

    public MetaData getMetaData() {
        return previousClusterState.metaData();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (isMasterOrDataNode() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            incrementalWrite = false;
            return;
        }

        try {
            // Hack: This is to ensure that non-master-eligible Zen2 nodes always store a current term
            // that's higher than the last accepted term.
            // TODO: can we get rid of this hack?
            if (event.state().term() > getCurrentTerm()) {
                innerSetCurrentTerm(event.state().term());
            }

            updateClusterState(event.state(), event.previousState());
            incrementalWrite = true;
        } catch (WriteStateException e) {
            logger.warn("Exception occurred when storing new meta data", e);
        }
    }

    @Override
    public long getCurrentTerm() {
        return previousManifest.getCurrentTerm();
    }

    @Override
    public ClusterState getLastAcceptedState() {
        assert previousClusterState.nodes().getLocalNode() != null : "Cluster state is not fully built yet";
        return previousClusterState;
    }

    @Override
    public void setCurrentTerm(long currentTerm) {
        try {
            innerSetCurrentTerm(currentTerm);
        } catch (WriteStateException e) {
            logger.error(new ParameterizedMessage("Failed to set current term to {}", currentTerm), e);
            e.rethrowAsErrorOrUncheckedException();
        }
    }

    private void innerSetCurrentTerm(long currentTerm) throws WriteStateException {
        Manifest manifest = new Manifest(currentTerm, previousManifest.getClusterStateVersion(), previousManifest.getGlobalGeneration(),
            new HashMap<>(previousManifest.getIndexGenerations()));
        metaStateService.writeManifestAndCleanup("current term changed", manifest);
        previousManifest = manifest;
    }

    @Override
    public void setLastAcceptedState(ClusterState clusterState) {
        try {
            incrementalWrite = previousClusterState.term() == clusterState.term();
            updateClusterState(clusterState, previousClusterState);
        } catch (WriteStateException e) {
            logger.error(new ParameterizedMessage("Failed to set last accepted state with version {}", clusterState.version()), e);
            e.rethrowAsErrorOrUncheckedException();
        }
    }

    /**
     * This class is used to write changed global {@link MetaData}, {@link IndexMetaData} and {@link Manifest} to disk.
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

        AtomicClusterStateWriter(MetaStateService metaStateService, Manifest previousManifest) {
            this.metaStateService = metaStateService;
            assert previousManifest != null;
            this.previousManifest = previousManifest;
            this.commitCleanupActions = new ArrayList<>();
            this.rollbackCleanupActions = new ArrayList<>();
            this.finished = false;
        }

        long writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                rollbackCleanupActions.add(() -> metaStateService.cleanupGlobalState(previousManifest.getGlobalGeneration()));
                long generation = metaStateService.writeGlobalState(reason, metaData);
                commitCleanupActions.add(() -> metaStateService.cleanupGlobalState(generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeIndex(String reason, IndexMetaData metaData) throws WriteStateException {
            assert finished == false : FINISHED_MSG;
            try {
                Index index = metaData.getIndex();
                Long previousGeneration = previousManifest.getIndexGenerations().get(index);
                if (previousGeneration != null) {
                    // we prefer not to clean-up index metadata in case of rollback,
                    // if it's not referenced by previous manifest file
                    // not to break dangling indices functionality
                    rollbackCleanupActions.add(() -> metaStateService.cleanupIndex(index, previousGeneration));
                }
                long generation = metaStateService.writeIndex(reason, metaData);
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
                // if Manifest write results in dirty WriteStateException it's not safe to remove
                // new metadata files, because if Manifest was actually written to disk and its deletion
                // fails it will reference these new metadata files.
                // In the future, we might decide to add more fine grained check to understand if after
                // WriteStateException Manifest deletion has actually failed.
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
    }

    /**
     * Updates manifest and meta data on disk.
     *
     * @param newState new {@link ClusterState}
     * @param previousState previous {@link ClusterState}
     *
     * @throws WriteStateException if exception occurs. See also {@link WriteStateException#isDirty()}.
     */
    private void updateClusterState(ClusterState newState, ClusterState previousState)
            throws WriteStateException {
        MetaData newMetaData = newState.metaData();

        final AtomicClusterStateWriter writer = new AtomicClusterStateWriter(metaStateService, previousManifest);
        long globalStateGeneration = writeGlobalState(writer, newMetaData);
        Map<Index, Long> indexGenerations = writeIndicesMetadata(writer, newState, previousState);
        Manifest manifest = new Manifest(previousManifest.getCurrentTerm(), newState.version(), globalStateGeneration, indexGenerations);
        writeManifest(writer, manifest);

        previousManifest = manifest;
        previousClusterState = newState;
    }

    private void writeManifest(AtomicClusterStateWriter writer, Manifest manifest) throws WriteStateException {
        if (manifest.equals(previousManifest) == false) {
            writer.writeManifestAndCleanup("changed", manifest);
        }
    }

    private Map<Index, Long> writeIndicesMetadata(AtomicClusterStateWriter writer, ClusterState newState, ClusterState previousState)
            throws WriteStateException {
        Map<Index, Long> previouslyWrittenIndices = previousManifest.getIndexGenerations();
        Set<Index> relevantIndices = getRelevantIndices(newState, previousState, previouslyWrittenIndices.keySet());

        Map<Index, Long> newIndices = new HashMap<>();

        MetaData previousMetaData = incrementalWrite ? previousState.metaData() : null;
        Iterable<IndexMetaDataAction> actions = resolveIndexMetaDataActions(previouslyWrittenIndices, relevantIndices, previousMetaData,
                newState.metaData());

        for (IndexMetaDataAction action : actions) {
            long generation = action.execute(writer);
            newIndices.put(action.getIndex(), generation);
        }

        return newIndices;
    }

    private long writeGlobalState(AtomicClusterStateWriter writer, MetaData newMetaData)
            throws WriteStateException {
        if (incrementalWrite == false || MetaData.isGlobalStateEquals(previousClusterState.metaData(), newMetaData) == false) {
            return writer.writeGlobalState("changed", newMetaData);
        }
        return previousManifest.getGlobalGeneration();
    }

    public static Set<Index> getRelevantIndices(ClusterState state, ClusterState previousState, Set<Index> previouslyWrittenIndices) {
        Set<Index> relevantIndices;
        if (isDataOnlyNode(state)) {
            relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previousState, previouslyWrittenIndices);
        } else if (state.nodes().getLocalNode().isMasterNode()) {
            relevantIndices = getRelevantIndicesForMasterEligibleNode(state);
        } else {
            relevantIndices = Collections.emptySet();
        }
        return relevantIndices;
    }

    private static boolean isDataOnlyNode(ClusterState state) {
        return state.nodes().getLocalNode().isMasterNode() == false && state.nodes().getLocalNode().isDataNode();
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetaDataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetaDataIndexUpgradeService might also update obsolete settings if needed.
     * Allows upgrading global custom meta data via {@link MetaDataUpgrader#customMetaDataUpgraders}
     *
     * @return input <code>metaData</code> if no upgrade is needed or an upgraded metaData
     */
    static MetaData upgradeMetaData(MetaData metaData,
                                    MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                    MetaDataUpgrader metaDataUpgrader) {
        // upgrade index meta data
        boolean changed = false;
        final MetaData.Builder upgradedMetaData = MetaData.builder(metaData);
        for (IndexMetaData indexMetaData : metaData) {
            IndexMetaData newMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData,
                    Version.CURRENT.minimumIndexCompatibilityVersion());
            changed |= indexMetaData != newMetaData;
            upgradedMetaData.put(newMetaData, false);
        }
        // upgrade global custom meta data
        if (applyPluginUpgraders(metaData.getCustoms(), metaDataUpgrader.customMetaDataUpgraders,
                upgradedMetaData::removeCustom, upgradedMetaData::putCustom)) {
            changed = true;
        }
        // upgrade current templates
        if (applyPluginUpgraders(metaData.getTemplates(), metaDataUpgrader.indexTemplateMetaDataUpgraders,
                upgradedMetaData::removeTemplate, (s, indexTemplateMetaData) -> upgradedMetaData.put(indexTemplateMetaData))) {
            changed = true;
        }
        return changed ? upgradedMetaData.build() : metaData;
    }

    private static <Data> boolean applyPluginUpgraders(ImmutableOpenMap<String, Data> existingData,
                                                       UnaryOperator<Map<String, Data>> upgrader,
                                                       Consumer<String> removeData,
                                                       BiConsumer<String, Data> putData) {
        // collect current data
        Map<String, Data> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, Data> customCursor : existingData) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, Data> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            // remove all data first so a plugin can remove custom metadata or templates if needed
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, Data> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }

    /**
     * Returns list of {@link IndexMetaDataAction} for each relevant index.
     * For each relevant index there are 3 options:
     * <ol>
     * <li>
     * {@link KeepPreviousGeneration} - index metadata is already stored to disk and index metadata version is not changed, no
     * action is required.
     * </li>
     * <li>
     * {@link WriteNewIndexMetaData} - there is no index metadata on disk and index metadata for this index should be written.
     * </li>
     * <li>
     * {@link WriteChangedIndexMetaData} - index metadata is already on disk, but index metadata version has changed. Updated
     * index metadata should be written to disk.
     * </li>
     * </ol>
     *
     * @param previouslyWrittenIndices A list of indices for which the state was already written before
     * @param relevantIndices          The list of indices for which state should potentially be written
     * @param previousMetaData         The last meta data we know of
     * @param newMetaData              The new metadata
     * @return list of {@link IndexMetaDataAction} for each relevant index.
     */
    public static List<IndexMetaDataAction> resolveIndexMetaDataActions(Map<Index, Long> previouslyWrittenIndices,
                                                                        Set<Index> relevantIndices,
                                                                        MetaData previousMetaData,
                                                                        MetaData newMetaData) {
        List<IndexMetaDataAction> actions = new ArrayList<>();
        for (Index index : relevantIndices) {
            IndexMetaData newIndexMetaData = newMetaData.getIndexSafe(index);
            IndexMetaData previousIndexMetaData = previousMetaData == null ? null : previousMetaData.index(index);

            if (previouslyWrittenIndices.containsKey(index) == false || previousIndexMetaData == null) {
                actions.add(new WriteNewIndexMetaData(newIndexMetaData));
            } else if (previousIndexMetaData.getVersion() != newIndexMetaData.getVersion()) {
                actions.add(new WriteChangedIndexMetaData(previousIndexMetaData, newIndexMetaData));
            } else {
                actions.add(new KeepPreviousGeneration(index, previouslyWrittenIndices.get(index)));
            }
        }
        return actions;
    }

    private static Set<Index> getRelevantIndicesOnDataOnlyNode(ClusterState state, ClusterState previousState, Set<Index>
            previouslyWrittenIndices) {
        RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        Set<Index> indices = new HashSet<>();
        for (ShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        // we have to check the meta data also: closed indices will not appear in the routing table, but we must still write the state if
        // we have it written on disk previously
        for (IndexMetaData indexMetaData : state.metaData()) {
            boolean isOrWasClosed = indexMetaData.getState().equals(IndexMetaData.State.CLOSE);
            // if the index is open we might still have to write the state if it just transitioned from closed to open
            // so we have to check for that as well.
            IndexMetaData previousMetaData = previousState.metaData().index(indexMetaData.getIndex());
            if (previousMetaData != null) {
                isOrWasClosed = isOrWasClosed || previousMetaData.getState().equals(IndexMetaData.State.CLOSE);
            }
            if (previouslyWrittenIndices.contains(indexMetaData.getIndex()) && isOrWasClosed) {
                indices.add(indexMetaData.getIndex());
            }
        }
        return indices;
    }

    private static Set<Index> getRelevantIndicesForMasterEligibleNode(ClusterState state) {
        Set<Index> relevantIndices = new HashSet<>();
        // we have to iterate over the metadata to make sure we also capture closed indices
        for (IndexMetaData indexMetaData : state.metaData()) {
            relevantIndices.add(indexMetaData.getIndex());
        }
        return relevantIndices;
    }

    /**
     * Action to perform with index metadata.
     */
    public interface IndexMetaDataAction {
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

    public static class KeepPreviousGeneration implements IndexMetaDataAction {
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
            return generation;
        }
    }

    public static class WriteNewIndexMetaData implements IndexMetaDataAction {
        private final IndexMetaData indexMetaData;

        WriteNewIndexMetaData(IndexMetaData indexMetaData) {
            this.indexMetaData = indexMetaData;
        }

        @Override
        public Index getIndex() {
            return indexMetaData.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            return writer.writeIndex("freshly created", indexMetaData);
        }
    }

    public static class WriteChangedIndexMetaData implements IndexMetaDataAction {
        private final IndexMetaData newIndexMetaData;
        private final IndexMetaData oldIndexMetaData;

        WriteChangedIndexMetaData(IndexMetaData oldIndexMetaData, IndexMetaData newIndexMetaData) {
            this.oldIndexMetaData = oldIndexMetaData;
            this.newIndexMetaData = newIndexMetaData;
        }

        @Override
        public Index getIndex() {
            return newIndexMetaData.getIndex();
        }

        @Override
        public long execute(AtomicClusterStateWriter writer) throws WriteStateException {
            return writer.writeIndex(
                    "version changed from [" + oldIndexMetaData.getVersion() + "] to [" + newIndexMetaData.getVersion() + "]",
                    newIndexMetaData);
        }
    }
}

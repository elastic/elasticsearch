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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * This class is responsible for storing/retrieving metadata to/from disk.
 * When instance of this class is created, constructor ensures that this version is compatible with state stored on disk and performs
 * state upgrade if necessary. Also it checks that atomic move is supported on the filesystem level, because it's a must for metadata
 * store algorithm.
 * Please note that the state being loaded when constructing the instance of this class is NOT the state that will be used as a
 * {@link ClusterState#metaData()}. Instead when node is starting up, it calls {@link #loadMetaData()} method and if this node is
 * elected as master, it requests metaData from other master eligible nodes. After that, master node performs re-conciliation on the
 * gathered results, re-creates {@link ClusterState} and broadcasts this state to other nodes in the cluster.
 * It means that the first time {@link #applyClusterState(ClusterChangedEvent)} method is called, it won't have any previous metaData in
 * memory and will iterate over all the indices in received {@link ClusterState} and store them to disk.
 */
public class GatewayMetaState implements ClusterStateApplier {
    private static final Logger logger = LogManager.getLogger(GatewayMetaState.class);

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final Settings settings;

    @Nullable
    //there is a single thread executing applyClusterState calls, hence no volatile modifier
    private Manifest previousManifest;
    private MetaData previousMetaData;

    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                            MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) throws IOException {
        this.settings = settings;
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;

        ensureNoPre019State(); //TODO remove this check, it's Elasticsearch version 7 already
        ensureAtomicMoveSupported(); //TODO move this check to NodeEnvironment, because it's related to all types of metadata
        upgradeMetaData(metaDataIndexUpgradeService, metaDataUpgrader);
        profileLoadMetaData();
    }

    private void profileLoadMetaData() throws IOException {
        if (isMasterOrDataNode()) {
            long startNS = System.nanoTime();
            metaStateService.loadFullState();
            logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
        }
    }

    private void upgradeMetaData(MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader)
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
                final Transaction tx = new Transaction(metaStateService, manifest);
                final MetaData upgradedMetaData = upgradeMetaData(metaData, metaDataIndexUpgradeService, metaDataUpgrader);

                final long globalStateGeneration;
                if (MetaData.isGlobalStateEquals(metaData, upgradedMetaData) == false) {
                    globalStateGeneration = tx.writeGlobalState("upgrade", upgradedMetaData);
                } else {
                    globalStateGeneration = manifest.getGlobalGeneration();
                }

                Map<Index, Long> indices = new HashMap<>(manifest.getIndexGenerations());
                for (IndexMetaData indexMetaData : upgradedMetaData) {
                    if (metaData.hasIndexMetaData(indexMetaData) == false) {
                        final long generation = tx.writeIndex("upgrade", indexMetaData);
                        indices.put(indexMetaData.getIndex(), generation);
                    }
                }

                final Manifest newManifest = new Manifest(globalStateGeneration, indices);
                tx.writeManifestAndCleanup("startup", newManifest);
            } catch (Exception e) {
                logger.error("failed to read or re-write local state, exiting...", e);
                throw e;
            }
        }
    }

    private boolean isMasterOrDataNode() {
        return DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings);
    }

    private void ensureAtomicMoveSupported() throws IOException {
        if (isMasterOrDataNode()) {
            nodeEnv.ensureAtomicMoveSupported();
        }
    }

    public MetaData loadMetaData() throws IOException {
        return metaStateService.loadFullState().v2();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (isMasterOrDataNode() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            // reset the current state, we need to start fresh...
            previousMetaData = null;
            previousManifest = null;
            return;
        }

        try {
            if (previousManifest == null) {
                previousManifest = metaStateService.loadManifestOrEmpty();
            }
            updateMetaData(event);
        } catch (Exception e) {
            logger.warn("Exception occurred when storing new meta data", e);
        }
    }

    /**
     * This class is used to write changed global {@link MetaData}, {@link IndexMetaData} and {@link Manifest} to disk.
     * This class delegates <code>write*</code> calls to corresponding write calls in {@link MetaStateService} and
     * additionally it keeps track of cleanup actions to be performed if transaction succeeds or fails.
     * Transaction succeeds if {@link #writeManifestAndCleanup(String, Manifest)} call succeeds, transaction fails if
     * any <code>write*</code> call fails.
     * Once transaction succeeds/fails it can no longer be used.
     */
    static class Transaction {
        private static final String TRANSACTION_COMPLETED_MSG = "Transaction is already completed";
        private final List<Runnable> commitCleanupActions;
        private final List<Runnable> rollbackCleanupActions;
        private final Manifest previousManifest;
        private final MetaStateService metaStateService;
        private boolean finished;

        Transaction(MetaStateService metaStateService, Manifest previousManifest) {
            this.metaStateService = metaStateService;
            assert previousManifest != null;
            this.previousManifest = previousManifest;
            this.commitCleanupActions = new ArrayList<>();
            this.rollbackCleanupActions = new ArrayList<>();
            this.finished = false;
        }

        long writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
            assert finished == false : TRANSACTION_COMPLETED_MSG;
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
            assert finished == false : TRANSACTION_COMPLETED_MSG;
            try {
                Index index = metaData.getIndex();
                long previousGeneration = previousManifest.getIndexGenerations().getOrDefault(index, -1L);
                rollbackCleanupActions.add(() -> metaStateService.cleanupIndex(index, previousGeneration));
                long generation = metaStateService.writeIndex(reason, metaData);
                commitCleanupActions.add(() -> metaStateService.cleanupIndex(index, generation));
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        long writeManifestAndCleanup(String reason, Manifest manifest) throws WriteStateException {
            assert finished == false : TRANSACTION_COMPLETED_MSG;
            try {
                long generation = metaStateService.writeManifestAndCleanup(reason, manifest);
                commitCleanupActions.forEach(Runnable::run);
                finished = true;
                return generation;
            } catch (WriteStateException e) {
                rollback();
                throw e;
            }
        }

        void rollback() {
            rollbackCleanupActions.forEach(Runnable::run);
            finished = true;
        }
    }

    /**
     * Updates meta state and meta data on disk according to {@link ClusterChangedEvent}.
     *
     * @throws IOException if IOException occurs. It's recommended for the callers of this method to handle {@link WriteStateException},
     *                     which is subclass of {@link IOException} explicitly. See also {@link WriteStateException#isDirty()}.
     */
    private void updateMetaData(ClusterChangedEvent event) throws IOException {
        ClusterState newState = event.state();
        ClusterState previousState = event.previousState();
        MetaData newMetaData = newState.metaData();

        final Transaction tx = new Transaction(metaStateService, previousManifest);
        long globalStateGeneration = writeGlobalState(tx, newMetaData);
        Map<Index, Long> indexGenerations = writeIndicesMetadata(tx, newState, previousState);
        Manifest manifest = new Manifest(globalStateGeneration, indexGenerations);
        writeManifest(tx, manifest);

        previousMetaData = newMetaData;
        previousManifest = manifest;
    }

    private void writeManifest(Transaction tx, Manifest manifest) throws IOException {
        if (manifest.equals(previousManifest) == false) {
            tx.writeManifestAndCleanup("changed", manifest);
        }
    }

    private Map<Index, Long> writeIndicesMetadata(Transaction tx, ClusterState newState, ClusterState previousState)
            throws IOException {
        Map<Index, Long> previouslyWrittenIndices = previousManifest.getIndexGenerations();
        Set<Index> relevantIndices = getRelevantIndices(newState, previousState, previouslyWrittenIndices.keySet());

        Map<Index, Long> newIndices = new HashMap<>();

        Iterable<IndexMetaDataAction> actions = resolveIndexMetaDataActions(previouslyWrittenIndices, relevantIndices, previousMetaData,
                newState.metaData());

        for (IndexMetaDataAction action : actions) {
            long generation = action.execute(tx);
            newIndices.put(action.getIndex(), generation);
        }

        return newIndices;
    }

    private long writeGlobalState(Transaction tx, MetaData newMetaData) throws IOException {
        if (previousMetaData == null || MetaData.isGlobalStateEquals(previousMetaData, newMetaData) == false) {
            return tx.writeGlobalState("changed", newMetaData);
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
        return ((state.nodes().getLocalNode().isMasterNode() == false) && state.nodes().getLocalNode().isDataNode());
    }


    private void ensureNoPre019State() throws IOException {
        if (DiscoveryNode.isDataNode(settings)) {
            ensureNoPre019ShardState();
        }
        if (isMasterOrDataNode()) {
            ensureNoPre019MetadataFiles();
        }
    }

    /**
     * Throws an IAE if a pre 0.19 state is detected
     */
    private void ensureNoPre019MetadataFiles() throws IOException {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (!Files.exists(stateLocation)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation)) {
                for (Path stateFile : stream) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[upgrade]: processing [{}]", stateFile.getFileName());
                    }
                    final String name = stateFile.getFileName().toString();
                    if (name.startsWith("metadata-")) {
                        throw new IllegalStateException("Detected pre 0.19 metadata file please upgrade to a version before "
                                + Version.CURRENT.minimumIndexCompatibilityVersion()
                                + " first to upgrade state structures - metadata found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    // shard state BWC
    private void ensureNoPre019ShardState() throws IOException {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (Files.exists(stateLocation)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation, "shards-*")) {
                    for (Path stateFile : stream) {
                        throw new IllegalStateException("Detected pre 0.19 shard state file please upgrade to a version before "
                                + Version.CURRENT.minimumIndexCompatibilityVersion()
                                + " first to upgrade state structures - shard state found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
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
        Set<Index> relevantIndices;
        relevantIndices = new HashSet<>();
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
         * Executes this action using provided {@link Transaction}.
         *
         * @return new index metadata state generation, to be used in manifest file.
         * @throws WriteStateException if exception occurs.
         */
        long execute(Transaction tx) throws WriteStateException;
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
        public long execute(Transaction tx) {
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
        public long execute(Transaction tx) throws WriteStateException {
            return tx.writeIndex("freshly created", indexMetaData);
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
        public long execute(Transaction tx) throws WriteStateException {
            return tx.writeIndex(
                    "version changed from [" + oldIndexMetaData.getVersion() + "] to [" + newIndexMetaData.getVersion() + "]",
                    newIndexMetaData);
        }
    }
}

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
import org.elasticsearch.common.component.AbstractComponent;
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
public class GatewayMetaState extends AbstractComponent implements ClusterStateApplier {

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;

    @Nullable
    //there is happens-before order between subsequent applyClusterState calls, hence no volatile modifier
    private Tuple<Manifest, MetaData> ourState;

    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                            MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) throws IOException {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;

        ensureNoPre019State();
        ensureAtomicMoveSupported();
        maybeUpgradeMetaData(metaDataIndexUpgradeService, metaDataUpgrader);
        profileLoadMetaData();
    }

    private void profileLoadMetaData() throws IOException {
        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            long startNS = System.nanoTime();
            metaStateService.loadFullState();
            logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
        }
    }

    private void maybeUpgradeMetaData(MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader)
            throws IOException {
        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            try {
                final Tuple<Manifest, MetaData> metaStateAndData = metaStateService.loadFullState();
                final Manifest manifest = metaStateAndData.v1();
                final MetaData metaData = metaStateAndData.v2();

                // We finished global state validation and successfully checked all indices for backward compatibility
                // and found no non-upgradable indices, which means the upgrade can continue.
                // Now it's safe to overwrite global and index metadata.
                long globalStateGeneration = manifest.getGlobalGeneration();

                if (globalStateGeneration != -1) {
                    List<Runnable> cleanupActions = new ArrayList<>();
                    // If globalStateGeneration is non-negative, it means we should have some metadata on disk
                    // Always re-write it even if upgrade plugins do not upgrade it, to be sure it's properly persisted on all data path
                    final MetaData upgradedMetaData = upgradeMetaData(metaData, metaDataIndexUpgradeService, metaDataUpgrader);

                    if (MetaData.isGlobalStateEquals(metaData, upgradedMetaData) == false) {
                        globalStateGeneration = metaStateService.writeGlobalState("upgrade", upgradedMetaData);
                    } else {
                        globalStateGeneration = metaStateService.writeGlobalState("startup", metaData);
                    }
                    final long currentGlobalStateGeneration = globalStateGeneration;
                    cleanupActions.add(() -> metaStateService.cleanupGlobalState(currentGlobalStateGeneration));

                    Map<Index, Long> indices = new HashMap<>();
                    for (IndexMetaData indexMetaData : upgradedMetaData) {
                        long generation;
                        if (metaData.hasIndexMetaData(indexMetaData) == false) {
                            generation = metaStateService.writeIndex("upgrade", indexMetaData);
                        } else {
                            generation = metaStateService.writeIndex("startup", indexMetaData);
                        }
                        final long currentGeneration = generation;
                        cleanupActions.add(() -> metaStateService.cleanupIndex(indexMetaData.getIndex(), currentGeneration));
                        indices.put(indexMetaData.getIndex(), generation);
                    }

                    final long metaStateGeneration =
                            metaStateService.writeManifest("startup", new Manifest(globalStateGeneration, indices));
                    cleanupActions.add(() -> metaStateService.cleanupMetaState(metaStateGeneration));
                    performCleanup(cleanupActions);
                }
            } catch (Exception e) {
                logger.error("failed to read or re-write local state, exiting...", e);
                throw e;
            }
        }
    }

    private void ensureAtomicMoveSupported() throws IOException {
        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            nodeEnv.ensureAtomicMoveSupported();
        }
    }

    public MetaData loadMetaData() throws IOException {
        return metaStateService.loadFullState().v2();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        ClusterState newState = event.state();
        if (newState.nodes().getLocalNode().isMasterNode() == false && newState.nodes().getLocalNode().isDataNode() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            // reset the current state, we need to start fresh...
            ourState = null;
            return;
        }

        try {
            if (ourState == null) {
                ourState = metaStateService.loadFullState();
            }
            ourState = updateMetaData(event);
        } catch (WriteStateException e) {
            if (e.isDirty()) {
                logger.error("Fatal exception occurred when storing new meta data. Storage is dirty", e);
            } else {
                logger.warn("Exception occurred when storing new meta data. Storage is not dirty", e);
            }
        } catch (Exception e) {
            logger.warn("Exception occurred when storing new meta data", e);
        }
    }

    /**
     * Updates meta state and meta data on disk according to {@link ClusterChangedEvent}.
     *
     * @throws IOException if IOException occurs. It's recommended for the callers of this method to handle {@link WriteStateException},
     *                     which is subclass of {@link IOException} explicitly. See also {@link WriteStateException#isDirty()}.
     */
    private Tuple<Manifest, MetaData> updateMetaData(ClusterChangedEvent event) throws IOException {
        ClusterState newState = event.state();
        ClusterState previousState = event.previousState();
        MetaData newMetaData = newState.metaData();

        List<Runnable> cleanupActions = new ArrayList<>();
        long globalStateGeneration = writeGlobalState(newMetaData, cleanupActions);
        Map<Index, Long> newIndices = writeIndicesMetadata(newState, previousState, cleanupActions);
        Manifest manifest = new Manifest(globalStateGeneration, newIndices);
        writeManifest(manifest, cleanupActions);
        performCleanup(cleanupActions);
        return new Tuple<>(manifest, newMetaData);
    }

    private void performCleanup(List<Runnable> cleanupActions) {
        for (Runnable action : cleanupActions) {
            action.run();
        }
    }

    private void writeManifest(Manifest manifest, List<Runnable> cleanupActions) throws IOException {
        if (manifest.equals(ourState.v1()) == false) {
            long generation = metaStateService.writeManifest("changed", manifest);
            cleanupActions.add(() -> metaStateService.cleanupMetaState(generation));
        }
    }

    private Map<Index, Long> writeIndicesMetadata(ClusterState newState, ClusterState previousState, List<Runnable> cleanupActions)
            throws IOException {
        MetaData previousMetadata = ourState.v2();
        Manifest previousMetastate = ourState.v1();
        Map<Index, Long> previouslyWrittenIndices = previousMetastate.getIndexGenerations();
        Set<Index> relevantIndices = getRelevantIndices(newState, previousState, previouslyWrittenIndices.keySet());

        Map<Index, Long> newIndices = new HashMap<>();

        Iterable<IndexMetaDataAction> actions = resolveIndexMetaDataActions(previouslyWrittenIndices, relevantIndices, previousMetadata,
                newState.metaData());

        for (IndexMetaDataAction action : actions) {
            long generation = action.execute(metaStateService, cleanupActions);
            newIndices.put(action.getIndex(), generation);
        }

        return newIndices;
    }

    private long writeGlobalState(MetaData newMetaData, List<Runnable> cleanupActions) throws IOException {
        if (ourState.v1().getGlobalGeneration() == -1 || MetaData.isGlobalStateEquals(ourState.v2(), newMetaData) == false) {
            long generation = metaStateService.writeGlobalState("changed", newMetaData);
            cleanupActions.add(() -> metaStateService.cleanupGlobalState(generation));
            return generation;
        }
        return ourState.v1().getGlobalGeneration();
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
        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
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
            Long generation = previouslyWrittenIndices.get(index);
            IndexMetaData newIndexMetadata = newMetaData.getIndexSafe(index);
            if (generation == null) {
                actions.add(new WriteNewIndexMetaData(newIndexMetadata));
            } else {
                IndexMetaData previousIndexMetadata = previousMetaData.index(index);
                if (previousIndexMetadata.getVersion() != newIndexMetadata.getVersion()) {
                    actions.add(new WriteChangedIndexMetaData(previousIndexMetadata, newIndexMetadata));
                } else {
                    actions.add(new KeepPreviousGeneration(index, generation));
                }
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
         * Executes this action using <code>writer</code> and potentially adding cleanup action to the list of <code>cleanupActions</code>.
         *
         * @param writer         entity that can write index metadata to disk and perform cleanup afterwards. We prefer
         *                       {@link IndexMetaDataWriter} interface in place of {@link MetaStateService} for easier unit testing.
         * @param cleanupActions list of actions, which is expected to be mutated by adding new clean up action to it.
         * @return new index metadata state generation, to be used in manifest file.
         * @throws WriteStateException if exception occurs.
         */
        long execute(IndexMetaDataWriter writer, List<Runnable> cleanupActions) throws WriteStateException;
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
        public long execute(IndexMetaDataWriter writer, List<Runnable> cleanupActions) {
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
        public long execute(IndexMetaDataWriter writer, List<Runnable> cleanupActions) throws WriteStateException {
            long generation = writer.writeIndex("freshly created", indexMetaData);
            cleanupActions.add(() -> writer.cleanupIndex(indexMetaData.getIndex(), generation));
            return generation;
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
        public long execute(IndexMetaDataWriter writer, List<Runnable> cleanupActions) throws WriteStateException {
            long generation = writer.writeIndex(
                    "version changed from [" + oldIndexMetaData.getVersion() + "] to [" + newIndexMetaData.getVersion() + "]",
                    newIndexMetaData);
            cleanupActions.add(() -> writer.cleanupIndex(newIndexMetaData.getIndex(), generation));
            return generation;
        }
    }
}

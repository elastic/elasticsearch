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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.MultiDataPathUpgrader;
import org.elasticsearch.env.NodeEnvironment;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class GatewayMetaState extends AbstractComponent implements ClusterStateListener {

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final DanglingIndicesState danglingIndicesState;
    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;

    @Nullable
    private volatile MetaData previousMetaData;

    private volatile ImmutableSet<String> previouslyWrittenIndices = ImmutableSet.of();

    @Inject
    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                            DanglingIndicesState danglingIndicesState, TransportNodesListGatewayMetaState nodesListGatewayMetaState,
                            MetaDataIndexUpgradeService metaDataIndexUpgradeService) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.danglingIndicesState = danglingIndicesState;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        nodesListGatewayMetaState.init(this);

        if (DiscoveryNode.dataNode(settings)) {
            ensureNoPre019ShardState(nodeEnv);
            MultiDataPathUpgrader.upgradeMultiDataPath(nodeEnv, logger);
        }

        if (DiscoveryNode.masterNode(settings) || DiscoveryNode.dataNode(settings)) {
            nodeEnv.ensureAtomicMoveSupported();
        }
        if (DiscoveryNode.masterNode(settings) || DiscoveryNode.dataNode(settings)) {
            try {
                ensureNoPre019State();
                pre20Upgrade();
                long startNS = System.nanoTime();
                metaStateService.loadFullState();
                logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));
            } catch (Exception e) {
                logger.error("failed to read local state, exiting...", e);
                throw e;
            }
        }
    }

    public MetaData loadMetaState() throws Exception {
        return metaStateService.loadFullState();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        Set<String> relevantIndices = new HashSet<>();
        final ClusterState state = event.state();
        if (state.blocks().disableStatePersistence()) {
            // reset the current metadata, we need to start fresh...
            this.previousMetaData = null;
            previouslyWrittenIndices = ImmutableSet.of();
            return;
        }

        MetaData newMetaData = state.metaData();
        // we don't check if metaData changed, since we might be called several times and we need to check dangling...

        boolean success = true;
        // write the state if this node is a master eligible node or if it is a data node and has shards allocated on it
        if (state.nodes().localNode().masterNode() || state.nodes().localNode().dataNode()) {
            if (previousMetaData == null) {
                try {
                    // we determine if or if not we write meta data on data only nodes by looking at the shard routing
                    // and only write if a shard of this index is allocated on this node
                    // however, closed indices do not appear in the shard routing. if the meta data for a closed index is
                    // updated it will therefore not be written in case the list of previouslyWrittenIndices is empty (because state
                    // persistence was disabled or the node was restarted), see getRelevantIndicesOnDataOnlyNode().
                    // we therefore have to check here if we have shards on disk and add their indices to the previouslyWrittenIndices list
                    if (isDataOnlyNode(state)) {
                        ImmutableSet.Builder<String> previouslyWrittenIndicesBuilder = ImmutableSet.builder();
                        for (IndexMetaData indexMetaData : newMetaData) {
                            IndexMetaData indexMetaDataOnDisk = null;
                            if (indexMetaData.state().equals(IndexMetaData.State.CLOSE)) {
                                indexMetaDataOnDisk = metaStateService.loadIndexState(indexMetaData.index());
                            }
                            if (indexMetaDataOnDisk != null) {
                                previouslyWrittenIndicesBuilder.add(indexMetaDataOnDisk.index());
                            }
                        }
                        previouslyWrittenIndices = previouslyWrittenIndicesBuilder.addAll(previouslyWrittenIndices).build();
                    }
                } catch (Throwable e) {
                    success = false;
                }
            }
            // check if the global state changed?
            if (previousMetaData == null || !MetaData.isGlobalStateEquals(previousMetaData, newMetaData)) {
                try {
                    metaStateService.writeGlobalState("changed", newMetaData);
                } catch (Throwable e) {
                    success = false;
                }
            }

            Iterable<IndexMetaWriteInfo> writeInfo;
            relevantIndices = getRelevantIndices(event.state(), event.previousState(), previouslyWrittenIndices);
            writeInfo = resolveStatesToBeWritten(previouslyWrittenIndices, relevantIndices, previousMetaData, event.state().metaData());
            // check and write changes in indices
            for (IndexMetaWriteInfo indexMetaWrite : writeInfo) {
                try {
                    metaStateService.writeIndex(indexMetaWrite.reason, indexMetaWrite.newMetaData, indexMetaWrite.previousMetaData);
                } catch (Throwable e) {
                    success = false;
                }
            }
        }

        danglingIndicesState.processDanglingIndices(newMetaData);

        if (success) {
            previousMetaData = newMetaData;
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            previouslyWrittenIndices = builder.addAll(relevantIndices).build();
        }
    }

    public static Set<String> getRelevantIndices(ClusterState state, ClusterState previousState,ImmutableSet<String> previouslyWrittenIndices) {
        Set<String> relevantIndices;
        if (isDataOnlyNode(state)) {
            relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previousState, previouslyWrittenIndices);
        } else if (state.nodes().localNode().masterNode() == true) {
            relevantIndices = getRelevantIndicesForMasterEligibleNode(state);
        } else {
            relevantIndices = Collections.emptySet();
        }
        return relevantIndices;
    }


    protected static boolean isDataOnlyNode(ClusterState state) {
        return ((state.nodes().localNode().masterNode() == false) && state.nodes().localNode().dataNode());
    }

    /**
     * Throws an IAE if a pre 0.19 state is detected
     */
    private void ensureNoPre019State() throws Exception {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (!Files.exists(stateLocation)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation)) {
                for (Path stateFile : stream) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[upgrade]: processing [" + stateFile.getFileName() + "]");
                    }
                    final String name = stateFile.getFileName().toString();
                    if (name.startsWith("metadata-")) {
                        throw new IllegalStateException("Detected pre 0.19 metadata file please upgrade to a version before "
                                + Version.CURRENT.minimumCompatibilityVersion()
                                + " first to upgrade state structures - metadata found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetaDataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetaDataIndexUpgradeService might also update obsolete settings if needed. When this happens we rewrite
     * index metadata with new settings.
     */
    private void pre20Upgrade() throws Exception {
        MetaData metaData = loadMetaState();
        List<IndexMetaData> updateIndexMetaData = new ArrayList<>();
        for (IndexMetaData indexMetaData : metaData) {
            IndexMetaData newMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData);
            if (indexMetaData != newMetaData) {
                updateIndexMetaData.add(newMetaData);
            }
        }
        // We successfully checked all indices for backward compatibility and found no non-upgradable indices, which
        // means the upgrade can continue. Now it's safe to overwrite index metadata with the new version.
        for (IndexMetaData indexMetaData : updateIndexMetaData) {
            metaStateService.writeIndex("upgrade", indexMetaData, null);
        }
    }

    // shard state BWC
    private void ensureNoPre019ShardState(NodeEnvironment nodeEnv) throws Exception {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (Files.exists(stateLocation)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation, "shards-*")) {
                    for (Path stateFile : stream) {
                        throw new IllegalStateException("Detected pre 0.19 shard state file please upgrade to a version before "
                                + Version.CURRENT.minimumCompatibilityVersion()
                                + " first to upgrade state structures - shard state found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * Loads the current meta state for each index in the new cluster state and checks if it has to be persisted.
     * Each index state that should be written to disk will be returned. This is only run for data only nodes.
     * It will return only the states for indices that actually have a shard allocated on the current node.
     *
     * @param previouslyWrittenIndices    A list of indices for which the state was already written before
     * @param potentiallyUnwrittenIndices The list of indices for which state should potentially be written
     * @param previousMetaData            The last meta data we know of. meta data for all indices in previouslyWrittenIndices list is persisted now
     * @param newMetaData                 The new metadata
     * @return iterable over all indices states that should be written to disk
     */
    public static Iterable<GatewayMetaState.IndexMetaWriteInfo> resolveStatesToBeWritten(ImmutableSet<String> previouslyWrittenIndices, Set<String> potentiallyUnwrittenIndices, MetaData previousMetaData, MetaData newMetaData) {
        List<GatewayMetaState.IndexMetaWriteInfo> indicesToWrite = new ArrayList<>();
        for (String index : potentiallyUnwrittenIndices) {
            IndexMetaData newIndexMetaData = newMetaData.index(index);
            IndexMetaData previousIndexMetaData = previousMetaData == null ? null : previousMetaData.index(index);
            String writeReason = null;
            if (previouslyWrittenIndices.contains(index) == false || previousIndexMetaData == null) {
                writeReason = "freshly created";
            } else if (previousIndexMetaData.version() != newIndexMetaData.version()) {
                writeReason = "version changed from [" + previousIndexMetaData.version() + "] to [" + newIndexMetaData.version() + "]";
            }
            if (writeReason != null) {
                indicesToWrite.add(new GatewayMetaState.IndexMetaWriteInfo(newIndexMetaData, previousIndexMetaData, writeReason));
            }
        }
        return indicesToWrite;
    }

    public static Set<String> getRelevantIndicesOnDataOnlyNode(ClusterState state, ClusterState previousState, ImmutableSet<String> previouslyWrittenIndices) {
        RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().localNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        Set<String> indices = new HashSet<>();
        for (ShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        // we have to check the meta data also: closed indices will not appear in the routing table, but we must still write the state if we have it written on disk previously
        for (IndexMetaData indexMetaData : state.metaData()) {
            boolean isOrWasClosed = indexMetaData.state().equals(IndexMetaData.State.CLOSE);
            // if the index is open we might still have to write the state if it just transitioned from closed to open
            // so we have to check for that as well.
            IndexMetaData previousMetaData = previousState.metaData().getIndices().get(indexMetaData.getIndex());
            if (previousMetaData != null) {
                isOrWasClosed = isOrWasClosed || previousMetaData.state().equals(IndexMetaData.State.CLOSE);
            }
            if (previouslyWrittenIndices.contains(indexMetaData.getIndex()) && isOrWasClosed) {
                indices.add(indexMetaData.getIndex());
            }
        }
        return indices;
    }

    public static Set<String> getRelevantIndicesForMasterEligibleNode(ClusterState state) {
        Set<String> relevantIndices;
        relevantIndices = new HashSet<>();
        // we have to iterate over the metadata to make sure we also capture closed indices
        for (IndexMetaData indexMetaData : state.metaData()) {
            relevantIndices.add(indexMetaData.getIndex());
        }
        return relevantIndices;
    }


    public static class IndexMetaWriteInfo {
        final IndexMetaData newMetaData;
        final String reason;
        final IndexMetaData previousMetaData;

        public IndexMetaWriteInfo(IndexMetaData newMetaData, IndexMetaData previousMetaData, String reason) {
            this.newMetaData = newMetaData;
            this.reason = reason;
            this.previousMetaData = previousMetaData;
        }

        public IndexMetaData getNewMetaData() {
            return newMetaData;
        }

        public String getReason() {
            return reason;
        }
    }
}

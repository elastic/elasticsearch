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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Loads (and maybe upgrades) cluster metadata at startup, and persistently stores cluster metadata for future restarts.
 *
 * When started, ensures that this version is compatible with the state stored on disk, and performs a state upgrade if necessary. Note that
 * the state being loaded when constructing the instance of this class is not necessarily the state that will be used as {@link
 * ClusterState#metaData()} because it might be stale or incomplete. Master-eligible nodes must perform an election to find a complete and
 * non-stale state, and master-ineligible nodes receive the real cluster state from the elected master after joining the cluster.
 */
public class GatewayMetaState {
    private static final Logger logger = LogManager.getLogger(GatewayMetaState.class);

    // Set by calling start()
    private final SetOnce<PersistedState> persistedState = new SetOnce<>();

    public PersistedState getPersistedState() {
        final PersistedState persistedState = this.persistedState.get();
        assert persistedState != null : "not started";
        return persistedState;
    }

    public MetaData getMetaData() {
        return getPersistedState().getLastAcceptedState().metaData();
    }

    public void start(Settings settings, TransportService transportService, ClusterService clusterService,
                      MetaStateService metaStateService, MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                      MetaDataUpgrader metaDataUpgrader) {
        assert persistedState.get() == null : "should only start once, but already have " + persistedState.get();

        final Tuple<Manifest, ClusterState> manifestClusterStateTuple;
        try {
            upgradeMetaData(settings, metaStateService, metaDataIndexUpgradeService, metaDataUpgrader);
            manifestClusterStateTuple = loadStateAndManifest(ClusterName.CLUSTER_NAME_SETTING.get(settings), metaStateService);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load metadata", e);
        }
        final IncrementalClusterStateWriter incrementalClusterStateWriter
            = new IncrementalClusterStateWriter(settings, clusterService.getClusterSettings(), metaStateService,
            manifestClusterStateTuple.v1(),
            prepareInitialClusterState(transportService, clusterService, manifestClusterStateTuple.v2()),
            transportService.getThreadPool()::relativeTimeInMillis);

        if (DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings).equals(DiscoveryModule.ZEN_DISCOVERY_TYPE)) {
            // only for tests that simulate mixed Zen1/Zen2 clusters, see Zen1IT
            if (isMasterOrDataNode(settings)) {
                clusterService.addLowPriorityApplier(new GatewayClusterApplier(incrementalClusterStateWriter));
            }
            persistedState.set(new InMemoryPersistedState(manifestClusterStateTuple.v1().getCurrentTerm(), manifestClusterStateTuple.v2()));
        } else if (DiscoveryNode.isMasterNode(settings) == false) {
            if (DiscoveryNode.isDataNode(settings)) {
                // Master-eligible nodes persist index metadata for all indices regardless of whether they hold any shards or not. It's
                // vitally important to the safety of the cluster coordination system that master-eligible nodes persist this metadata when
                // _accepting_ the cluster state (i.e. before it is committed). This persistence happens on the generic threadpool.
                //
                // In contrast, master-ineligible data nodes only persist the index metadata for shards that they hold. When all shards of
                // an index are moved off such a node the IndicesStore is responsible for removing the corresponding index directory,
                // including the metadata, and does so on the cluster applier thread.
                //
                // This presents a problem: if a shard is unassigned from a node and then reassigned back to it again then there is a race
                // between the IndicesStore deleting the index folder and the CoordinationState concurrently trying to write the updated
                // metadata into it. We could probably solve this with careful synchronization, but in fact there is no need.  The persisted
                // state on master-ineligible data nodes is mostly ignored - it's only there to support dangling index imports, which is
                // inherently unsafe anyway. Thus we can safely delay metadata writes on master-ineligible data nodes until applying the
                // cluster state, which is what this does:
                clusterService.addLowPriorityApplier(new GatewayClusterApplier(incrementalClusterStateWriter));
            }

            // Master-ineligible nodes do not need to persist the cluster state when accepting it because they are not in the voting
            // configuration, so it's ok if they have a stale or incomplete cluster state when restarted. We track the latest cluster state
            // in memory instead.
            persistedState.set(new InMemoryPersistedState(manifestClusterStateTuple.v1().getCurrentTerm(), manifestClusterStateTuple.v2()));
        } else {
            // Master-ineligible nodes must persist the cluster state when accepting it because they must reload the (complete, fresh)
            // last-accepted cluster state when restarted.
            persistedState.set(new GatewayPersistedState(incrementalClusterStateWriter));
        }
    }

    // exposed so it can be overridden by tests
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        assert clusterState.nodes().getLocalNode() == null : "prepareInitialClusterState must only be called once";
        assert transportService.getLocalNode() != null : "transport service is not yet started";
        return Function.<ClusterState>identity()
            .andThen(ClusterStateUpdaters::addStateNotRecoveredBlock)
            .andThen(state -> ClusterStateUpdaters.setLocalNode(state, transportService.getLocalNode()))
            .andThen(state -> ClusterStateUpdaters.upgradeAndArchiveUnknownOrInvalidSettings(state, clusterService.getClusterSettings()))
            .andThen(ClusterStateUpdaters::recoverClusterBlocks)
            .apply(clusterState);
    }

    // exposed so it can be overridden by tests
    void upgradeMetaData(Settings settings, MetaStateService metaStateService, MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                         MetaDataUpgrader metaDataUpgrader) throws IOException {
        if (isMasterOrDataNode(settings)) {
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
                final IncrementalClusterStateWriter.AtomicClusterStateWriter writer
                    = new IncrementalClusterStateWriter.AtomicClusterStateWriter(metaStateService, manifest);
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

    private static Tuple<Manifest,ClusterState> loadStateAndManifest(ClusterName clusterName,
                                                                     MetaStateService metaStateService) throws IOException {
        final long startNS = System.nanoTime();
        final Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
        final Manifest manifest = manifestAndMetaData.v1();

        final ClusterState clusterState = ClusterState.builder(clusterName)
            .version(manifest.getClusterStateVersion())
            .metaData(manifestAndMetaData.v2()).build();

        logger.debug("took {} to load state", TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - startNS)));

        return Tuple.tuple(manifest, clusterState);
    }

    private static boolean isMasterOrDataNode(Settings settings) {
        return DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings);
    }

    /**
     * Elasticsearch 2.0 removed several deprecated features and as well as support for Lucene 3.x. This method calls
     * {@link MetaDataIndexUpgradeService} to makes sure that indices are compatible with the current version. The
     * MetaDataIndexUpgradeService might also update obsolete settings if needed.
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
        // upgrade current templates
        if (applyPluginUpgraders(metaData.getTemplates(), metaDataUpgrader.indexTemplateMetaDataUpgraders,
                upgradedMetaData::removeTemplate, (s, indexTemplateMetaData) -> upgradedMetaData.put(indexTemplateMetaData))) {
            changed = true;
        }
        return changed ? upgradedMetaData.build() : metaData;
    }

    private static boolean applyPluginUpgraders(ImmutableOpenMap<String, IndexTemplateMetaData> existingData,
                                                UnaryOperator<Map<String, IndexTemplateMetaData>> upgrader,
                                                Consumer<String> removeData,
                                                BiConsumer<String, IndexTemplateMetaData> putData) {
        // collect current data
        Map<String, IndexTemplateMetaData> existingMap = new HashMap<>();
        for (ObjectObjectCursor<String, IndexTemplateMetaData> customCursor : existingData) {
            existingMap.put(customCursor.key, customCursor.value);
        }
        // upgrade global custom meta data
        Map<String, IndexTemplateMetaData> upgradedCustoms = upgrader.apply(existingMap);
        if (upgradedCustoms.equals(existingMap) == false) {
            // remove all data first so a plugin can remove custom metadata or templates if needed
            existingMap.keySet().forEach(removeData);
            for (Map.Entry<String, IndexTemplateMetaData> upgradedCustomEntry : upgradedCustoms.entrySet()) {
                putData.accept(upgradedCustomEntry.getKey(), upgradedCustomEntry.getValue());
            }
            return true;
        }
        return false;
    }


    private static class GatewayClusterApplier implements ClusterStateApplier {

        private final IncrementalClusterStateWriter incrementalClusterStateWriter;

        private GatewayClusterApplier(IncrementalClusterStateWriter incrementalClusterStateWriter) {
            this.incrementalClusterStateWriter = incrementalClusterStateWriter;
        }

        @Override
        public void applyClusterState(ClusterChangedEvent event) {
            if (event.state().blocks().disableStatePersistence()) {
                incrementalClusterStateWriter.setIncrementalWrite(false);
                return;
            }

            try {
                // Hack: This is to ensure that non-master-eligible Zen2 nodes always store a current term
                // that's higher than the last accepted term.
                // TODO: can we get rid of this hack?
                if (event.state().term() > incrementalClusterStateWriter.getPreviousManifest().getCurrentTerm()) {
                    incrementalClusterStateWriter.setCurrentTerm(event.state().term());
                }

                incrementalClusterStateWriter.updateClusterState(event.state());
                incrementalClusterStateWriter.setIncrementalWrite(true);
            } catch (WriteStateException e) {
                logger.warn("Exception occurred when storing new meta data", e);
            }
        }

    }

    private static class GatewayPersistedState implements PersistedState {

        private final IncrementalClusterStateWriter incrementalClusterStateWriter;

        GatewayPersistedState(IncrementalClusterStateWriter incrementalClusterStateWriter) {
            this.incrementalClusterStateWriter = incrementalClusterStateWriter;
        }

        @Override
        public long getCurrentTerm() {
            return incrementalClusterStateWriter.getPreviousManifest().getCurrentTerm();
        }

        @Override
        public ClusterState getLastAcceptedState() {
            final ClusterState previousClusterState = incrementalClusterStateWriter.getPreviousClusterState();
            assert previousClusterState.nodes().getLocalNode() != null : "Cluster state is not fully built yet";
            return previousClusterState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            try {
                incrementalClusterStateWriter.setCurrentTerm(currentTerm);
            } catch (WriteStateException e) {
                logger.error(new ParameterizedMessage("Failed to set current term to {}", currentTerm), e);
                e.rethrowAsErrorOrUncheckedException();
            }
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            try {
                incrementalClusterStateWriter.setIncrementalWrite(
                    incrementalClusterStateWriter.getPreviousClusterState().term() == clusterState.term());
                incrementalClusterStateWriter.updateClusterState(clusterState);
            } catch (WriteStateException e) {
                logger.error(new ParameterizedMessage("Failed to set last accepted state with version {}", clusterState.version()), e);
                e.rethrowAsErrorOrUncheckedException();
            }
        }

    }

}

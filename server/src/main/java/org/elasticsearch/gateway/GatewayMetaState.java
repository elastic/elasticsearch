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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.InMemoryPersistedState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Loads (and maybe upgrades) cluster metadata at startup, and persistently stores cluster metadata for future restarts.
 *
 * When started, ensures that this version is compatible with the state stored on disk, and performs a state upgrade if necessary. Note that
 * the state being loaded when constructing the instance of this class is not necessarily the state that will be used as {@link
 * ClusterState#metaData()} because it might be stale or incomplete. Master-eligible nodes must perform an election to find a complete and
 * non-stale state, and master-ineligible nodes receive the real cluster state from the elected master after joining the cluster.
 */
public class GatewayMetaState implements Closeable {

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
                      MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                      MetaDataUpgrader metaDataUpgrader, LucenePersistedStateFactory lucenePersistedStateFactory,
                      DanglingIndicesState danglingIndicesState) {
        assert persistedState.get() == null : "should only start once, but already have " + persistedState.get();

        if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.isDataNode(settings)) {
            try {
                PersistedState ps = lucenePersistedStateFactory.loadPersistedState((version, metadata) ->
                    prepareInitialClusterState(transportService, clusterService,
                        ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings))
                            .version(version)
                            .metaData(upgradeMetaDataForMasterEligibleNode(metadata, metaDataIndexUpgradeService, metaDataUpgrader))
                            .build()));
                //TODO: on data node scan on-disk folders for additional dangling indices candidates
                //TODO: only consider those indices as dangling candidates for which there is a shard folder (at least on data nodes)
                //TODO: persist dangling indices candidates so that they don't get lost after first CS from master gets written out
                danglingIndicesState.setInitialDanglingIndicesCandidates(
                    StreamSupport.stream(ps.getLastAcceptedState().metaData().indices().values().spliterator(), false).map(cur -> cur.value)
                    .collect(Collectors.toList()));
                danglingIndicesState.findNewAndAddDanglingIndices(ps.getLastAcceptedState().metaData());
                persistedState.set(ps);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to load metadata", e);
            }
        } else {
            persistedState.set(
                new InMemoryPersistedState(0L, ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.get(settings)).build()));
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
    MetaData upgradeMetaDataForMasterEligibleNode(MetaData metaData,
                                                  MetaDataIndexUpgradeService metaDataIndexUpgradeService,
                                                  MetaDataUpgrader metaDataUpgrader) {
        return upgradeMetaData(metaData, metaDataIndexUpgradeService, metaDataUpgrader);
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

    @Override
    public void close() throws IOException {
        IOUtils.close(persistedState.get());
    }

}

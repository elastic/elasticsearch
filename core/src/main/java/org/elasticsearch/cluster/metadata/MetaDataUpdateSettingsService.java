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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetaDataUpdateSettingsService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public MetaDataUpdateSettingsService(Settings settings, ClusterService clusterService, AllocationService allocationService, IndexScopedSettings indexScopedSettings, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService.add(this);
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // update an index with number of replicas based on data nodes if possible
        if (!event.state().nodes().isLocalNodeElectedMaster()) {
            return;
        }
        // we will want to know this for translating "all" to a number
        final int dataNodeCount = event.state().nodes().getDataNodes().size();

        Map<Integer, List<Index>> nrReplicasChanged = new HashMap<>();
        // we need to do this each time in case it was changed by update settings
        for (final IndexMetaData indexMetaData : event.state().metaData()) {
            AutoExpandReplicas autoExpandReplicas = IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(indexMetaData.getSettings());
            if (autoExpandReplicas.isEnabled()) {
                /*
                 * we have to expand the number of replicas for this index to at least min and at most max nodes here
                 * so we are bumping it up if we have to or reduce it depending on min/max and the number of datanodes.
                 * If we change the number of replicas we just let the shard allocator do it's thing once we updated it
                 * since it goes through the index metadata to figure out if something needs to be done anyway. Do do that
                 * we issue a cluster settings update command below and kicks off a reroute.
                 */
                final int min = autoExpandReplicas.getMinReplicas();
                final int max = autoExpandReplicas.getMaxReplicas(dataNodeCount);
                int numberOfReplicas = dataNodeCount - 1;
                if (numberOfReplicas < min) {
                    numberOfReplicas = min;
                } else if (numberOfReplicas > max) {
                    numberOfReplicas = max;
                }
                // same value, nothing to do there
                if (numberOfReplicas == indexMetaData.getNumberOfReplicas()) {
                    continue;
                }

                if (numberOfReplicas >= min && numberOfReplicas <= max) {

                    if (!nrReplicasChanged.containsKey(numberOfReplicas)) {
                        nrReplicasChanged.put(numberOfReplicas, new ArrayList<>());
                    }

                    nrReplicasChanged.get(numberOfReplicas).add(indexMetaData.getIndex());
                }
            }
        }

        if (nrReplicasChanged.size() > 0) {
            // update settings and kick of a reroute (implicit) for them to take effect
            for (final Integer fNumberOfReplicas : nrReplicasChanged.keySet()) {
                Settings settings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, fNumberOfReplicas).build();
                final List<Index> indices = nrReplicasChanged.get(fNumberOfReplicas);

                UpdateSettingsClusterStateUpdateRequest updateRequest = new UpdateSettingsClusterStateUpdateRequest()
                        .indices(indices.toArray(new Index[indices.size()])).settings(settings)
                        .ackTimeout(TimeValue.timeValueMillis(0)) //no need to wait for ack here
                        .masterNodeTimeout(TimeValue.timeValueMinutes(10));

                updateSettings(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
                    @Override
                    public void onResponse(ClusterStateUpdateResponse response) {
                        for (Index index : indices) {
                            logger.info("{} auto expanded replicas to [{}]", index, fNumberOfReplicas);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        for (Index index : indices) {
                            logger.warn("{} fail to auto expand replicas to [{}]", index, fNumberOfReplicas);
                        }
                    }
                });
            }
        }
    }

    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final Settings normalizedSettings = Settings.builder().put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        Settings.Builder skipppedSettings = Settings.builder();

        indexScopedSettings.validate(normalizedSettings);
        // never allow to change the number of shards
        for (Map.Entry<String, String> entry : normalizedSettings.getAsMap().entrySet()) {
            if (entry.getKey().equals(IndexMetaData.SETTING_NUMBER_OF_SHARDS)) {
                listener.onFailure(new IllegalArgumentException("can't change the number of shards for an index"));
                return;
            }
            Setting setting = indexScopedSettings.get(entry.getKey());
            assert setting != null; // we already validated the normalized settings
            settingsForClosedIndices.put(entry.getKey(), entry.getValue());
            if (setting.isDynamic()) {
                settingsForOpenIndices.put(entry.getKey(), entry.getValue());
            } else {
                skipppedSettings.put(entry.getKey(), entry.getValue());
            }
        }
        final Settings skippedSettigns = skipppedSettings.build();
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        clusterService.submitStateUpdateTask("update-settings",
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());

                // allow to change any settings to a close index, and only allow dynamic settings to be changed
                // on an open index
                Set<Index> openIndices = new HashSet<>();
                Set<Index> closeIndices = new HashSet<>();
                final String[] actualIndices = new String[request.indices().length];
                for (int i = 0; i < request.indices().length; i++) {
                    Index index = request.indices()[i];
                    actualIndices[i] = index.getName();
                    final IndexMetaData metaData = currentState.metaData().getIndexSafe(index);
                    if (metaData.getState() == IndexMetaData.State.OPEN) {
                        openIndices.add(index);
                    } else {
                        closeIndices.add(index);
                    }
                }

                if (closeIndices.size() > 0 && closedSettings.get(IndexMetaData.SETTING_NUMBER_OF_REPLICAS) != null) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update [%s] on closed indices %s - can leave index in an unopenable state", IndexMetaData.SETTING_NUMBER_OF_REPLICAS,
                            closeIndices
                    ));
                }
                if (!skippedSettigns.getAsMap().isEmpty() && !openIndices.isEmpty()) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update non dynamic settings [%s] for open indices %s",
                            skippedSettigns.getAsMap().keySet(),
                            openIndices
                    ));
                }

                int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, -1);
                if (updatedNumberOfReplicas != -1 && preserveExisting == false) {
                    routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                }

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_ONLY_BLOCK, IndexMetaData.INDEX_READ_ONLY_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_METADATA_BLOCK, IndexMetaData.INDEX_BLOCKS_METADATA_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_WRITE_BLOCK, IndexMetaData.INDEX_BLOCKS_WRITE_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_BLOCK, IndexMetaData.INDEX_BLOCKS_READ_SETTING, openSettings);

                if (!openIndices.isEmpty()) {
                    for (Index index : openIndices) {
                        IndexMetaData indexMetaData = metaDataBuilder.getSafe(index);
                        Settings.Builder updates = Settings.builder();
                        Settings.Builder indexSettings = Settings.builder().put(indexMetaData.getSettings());
                        if (indexScopedSettings.updateDynamicSettings(openSettings, indexSettings, updates, index.getName())) {
                            if (preserveExisting) {
                                indexSettings.put(indexMetaData.getSettings());
                            }
                            metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(indexSettings));
                        }
                    }
                }

                if (!closeIndices.isEmpty()) {
                    for (Index index : closeIndices) {
                        IndexMetaData indexMetaData = metaDataBuilder.getSafe(index);
                        Settings.Builder updates = Settings.builder();
                        Settings.Builder indexSettings = Settings.builder().put(indexMetaData.getSettings());
                        if (indexScopedSettings.updateSettings(closedSettings, indexSettings, updates, index.getName())) {
                            if (preserveExisting) {
                                indexSettings.put(indexMetaData.getSettings());
                            }
                            metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(indexSettings));
                        }
                    }
                }


                ClusterState updatedState = ClusterState.builder(currentState).metaData(metaDataBuilder).routingTable(routingTableBuilder.build()).blocks(blocks).build();

                // now, reroute in case things change that require it (like number of replicas)
                RoutingAllocation.Result routingResult = allocationService.reroute(updatedState, "settings update");
                updatedState = ClusterState.builder(updatedState).routingResult(routingResult).build();
                for (Index index : openIndices) {
                    indexScopedSettings.dryRun(updatedState.metaData().getIndexSafe(index).getSettings());
                }
                for (Index index : closeIndices) {
                    indexScopedSettings.dryRun(updatedState.metaData().getIndexSafe(index).getSettings());
                }
                return updatedState;
            }
        });
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    private static void maybeUpdateClusterBlock(String[] actualIndices, ClusterBlocks.Builder blocks, ClusterBlock block, Setting<Boolean> setting, Settings openSettings) {
        if (setting.exists(openSettings)) {
            final boolean updateReadBlock = setting.get(openSettings);
            for (String index : actualIndices) {
                if (updateReadBlock) {
                    blocks.addIndexBlock(index, block);
                } else {
                    blocks.removeIndexBlock(index, block);
                }
            }
        }
    }


    public void upgradeIndexSettings(final UpgradeSettingsClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {


        clusterService.submitStateUpdateTask("update-index-compatibility-versions", new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());
                for (Map.Entry<String, Tuple<Version, String>> entry : request.versions().entrySet()) {
                    String index = entry.getKey();
                    IndexMetaData indexMetaData = metaDataBuilder.get(index);
                    if (indexMetaData != null) {
                        if (Version.CURRENT.equals(indexMetaData.getCreationVersion()) == false) {
                            // No reason to pollute the settings, we didn't really upgrade anything
                            metaDataBuilder.put(IndexMetaData.builder(indexMetaData)
                                            .settings(Settings.builder().put(indexMetaData.getSettings())
                                                            .put(IndexMetaData.SETTING_VERSION_MINIMUM_COMPATIBLE, entry.getValue().v2())
                                                            .put(IndexMetaData.SETTING_VERSION_UPGRADED, entry.getValue().v1())
                                            )
                            );
                        }
                    }
                }
                return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
            }
        });
    }
}

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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.settings.IndexDynamicSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetaDataUpdateSettingsService extends AbstractComponent implements ClusterStateListener {

    // the value we recognize in the "max" position to mean all the nodes
    private static final String ALL_NODES_VALUE = "all";

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final DynamicSettings dynamicSettings;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public MetaDataUpdateSettingsService(Settings settings, ClusterService clusterService, AllocationService allocationService, @IndexDynamicSettings DynamicSettings dynamicSettings, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService.add(this);
        this.allocationService = allocationService;
        this.dynamicSettings = dynamicSettings;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // update an index with number of replicas based on data nodes if possible
        if (!event.state().nodes().localNodeMaster()) {
            return;
        }
        // we will want to know this for translating "all" to a number
        final int dataNodeCount = event.state().nodes().dataNodes().size();

        Map<Integer, List<String>> nrReplicasChanged = new HashMap<>();

        // we need to do this each time in case it was changed by update settings
        for (final IndexMetaData indexMetaData : event.state().metaData()) {
            String autoExpandReplicas = indexMetaData.getSettings().get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS);
            if (autoExpandReplicas != null && Booleans.parseBoolean(autoExpandReplicas, true)) { // Booleans only work for false values, just as we want it here
                try {
                    final int min;
                    final int max;

                    final int dash = autoExpandReplicas.indexOf('-');
                    if (-1 == dash) {
                        logger.warn("failed to set [{}] for index [{}], it should be dash delimited [{}]",
                                IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, indexMetaData.getIndex(), autoExpandReplicas);
                        continue;
                    }
                    final String sMin = autoExpandReplicas.substring(0, dash);
                    try {
                        min = Integer.parseInt(sMin);
                    } catch (NumberFormatException e) {
                        logger.warn("failed to set [{}] for index [{}], minimum value is not a number [{}]",
                                e, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, indexMetaData.getIndex(), sMin);
                        continue;
                    }
                    String sMax = autoExpandReplicas.substring(dash + 1);
                    if (sMax.equals(ALL_NODES_VALUE)) {
                        max = dataNodeCount - 1;
                    } else {
                        try {
                            max = Integer.parseInt(sMax);
                        } catch (NumberFormatException e) {
                            logger.warn("failed to set [{}] for index [{}], maximum value is neither [{}] nor a number [{}]",
                                    e, IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, indexMetaData.getIndex(), ALL_NODES_VALUE, sMax);
                            continue;
                        }
                    }

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
                            nrReplicasChanged.put(numberOfReplicas, new ArrayList<String>());
                        }

                        nrReplicasChanged.get(numberOfReplicas).add(indexMetaData.getIndex());
                    }
                } catch (Exception e) {
                    logger.warn("[{}] failed to parse auto expand replicas", e, indexMetaData.getIndex());
                }
            }
        }

        if (nrReplicasChanged.size() > 0) {
            for (final Integer fNumberOfReplicas : nrReplicasChanged.keySet()) {
                Settings settings = Settings.settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, fNumberOfReplicas).build();
                final List<String> indices = nrReplicasChanged.get(fNumberOfReplicas);

                UpdateSettingsClusterStateUpdateRequest updateRequest = new UpdateSettingsClusterStateUpdateRequest()
                        .indices(indices.toArray(new String[indices.size()])).settings(settings)
                        .ackTimeout(TimeValue.timeValueMillis(0)) //no need to wait for ack here
                        .masterNodeTimeout(TimeValue.timeValueMinutes(10));

                updateSettings(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
                    @Override
                    public void onResponse(ClusterStateUpdateResponse response) {
                        for (String index : indices) {
                            logger.info("[{}] auto expanded replicas to [{}]", index, fNumberOfReplicas);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        for (String index : indices) {
                            logger.warn("[{}] fail to auto expand replicas to [{}]", index, fNumberOfReplicas);
                        }
                    }
                });
            }
        }
    }

    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        Settings.Builder updatedSettingsBuilder = Settings.settingsBuilder();
        updatedSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        // never allow to change the number of shards
        for (String key : updatedSettingsBuilder.internalMap().keySet()) {
            if (key.equals(IndexMetaData.SETTING_NUMBER_OF_SHARDS)) {
                listener.onFailure(new IllegalArgumentException("can't change the number of shards for an index"));
                return;
            }
        }

        final Settings closeSettings = updatedSettingsBuilder.build();

        final Set<String> removedSettings = new HashSet<>();
        final Set<String> errors = new HashSet<>();
        for (Map.Entry<String, String> setting : updatedSettingsBuilder.internalMap().entrySet()) {
            if (!dynamicSettings.hasDynamicSetting(setting.getKey())) {
                removedSettings.add(setting.getKey());
            } else {
                String error = dynamicSettings.validateDynamicSetting(setting.getKey(), setting.getValue(), clusterService.state());
                if (error != null) {
                    errors.add("[" + setting.getKey() + "] - " + error);
                }
            }
        }

        if (!errors.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("can't process the settings: " + errors.toString()));
            return;
        }

        if (!removedSettings.isEmpty()) {
            for (String removedSetting : removedSettings) {
                updatedSettingsBuilder.remove(removedSetting);
            }
        }
        final Settings openSettings = updatedSettingsBuilder.build();

        clusterService.submitStateUpdateTask("update-settings", Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                String[] actualIndices = indexNameExpressionResolver.concreteIndices(currentState, IndicesOptions.strictExpand(), request.indices());
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                MetaData.Builder metaDataBuilder = MetaData.builder(currentState.metaData());

                // allow to change any settings to a close index, and only allow dynamic settings to be changed
                // on an open index
                Set<String> openIndices = new HashSet<>();
                Set<String> closeIndices = new HashSet<>();
                for (String index : actualIndices) {
                    if (currentState.metaData().index(index).getState() == IndexMetaData.State.OPEN) {
                        openIndices.add(index);
                    } else {
                        closeIndices.add(index);
                    }
                }

                if (closeIndices.size() > 0 && closeSettings.get(IndexMetaData.SETTING_NUMBER_OF_REPLICAS) != null) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update [%s] on closed indices [%s] - can leave index in an unopenable state", IndexMetaData.SETTING_NUMBER_OF_REPLICAS,
                            closeIndices
                    ));
                }
                if (!removedSettings.isEmpty() && !openIndices.isEmpty()) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update non dynamic settings[%s] for open indices [%s]",
                            removedSettings,
                            openIndices
                    ));
                }

                int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, -1);
                if (updatedNumberOfReplicas != -1) {
                    routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                }

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                Boolean updatedReadOnly = openSettings.getAsBoolean(IndexMetaData.SETTING_READ_ONLY, null);
                if (updatedReadOnly != null) {
                    for (String index : actualIndices) {
                        if (updatedReadOnly) {
                            blocks.addIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK);
                        } else {
                            blocks.removeIndexBlock(index, IndexMetaData.INDEX_READ_ONLY_BLOCK);
                        }
                    }
                }
                Boolean updateMetaDataBlock = openSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_METADATA, null);
                if (updateMetaDataBlock != null) {
                    for (String index : actualIndices) {
                        if (updateMetaDataBlock) {
                            blocks.addIndexBlock(index, IndexMetaData.INDEX_METADATA_BLOCK);
                        } else {
                            blocks.removeIndexBlock(index, IndexMetaData.INDEX_METADATA_BLOCK);
                        }
                    }
                }

                Boolean updateWriteBlock = openSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_WRITE, null);
                if (updateWriteBlock != null) {
                    for (String index : actualIndices) {
                        if (updateWriteBlock) {
                            blocks.addIndexBlock(index, IndexMetaData.INDEX_WRITE_BLOCK);
                        } else {
                            blocks.removeIndexBlock(index, IndexMetaData.INDEX_WRITE_BLOCK);
                        }
                    }
                }

                Boolean updateReadBlock = openSettings.getAsBoolean(IndexMetaData.SETTING_BLOCKS_READ, null);
                if (updateReadBlock != null) {
                    for (String index : actualIndices) {
                        if (updateReadBlock) {
                            blocks.addIndexBlock(index, IndexMetaData.INDEX_READ_BLOCK);
                        } else {
                            blocks.removeIndexBlock(index, IndexMetaData.INDEX_READ_BLOCK);
                        }
                    }
                }

                if (!openIndices.isEmpty()) {
                    String[] indices = openIndices.toArray(new String[openIndices.size()]);
                    metaDataBuilder.updateSettings(openSettings, indices);
                }

                if (!closeIndices.isEmpty()) {
                    String[] indices = closeIndices.toArray(new String[closeIndices.size()]);
                    metaDataBuilder.updateSettings(closeSettings, indices);
                }


                ClusterState updatedState = ClusterState.builder(currentState).metaData(metaDataBuilder).routingTable(routingTableBuilder.build()).blocks(blocks).build();

                // now, reroute in case things change that require it (like number of replicas)
                RoutingAllocation.Result routingResult = allocationService.reroute(updatedState, "settings update");
                updatedState = ClusterState.builder(updatedState).routingResult(routingResult).build();

                return updatedState;
            }
        });
    }

    public void upgradeIndexSettings(final UpgradeSettingsClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {


        clusterService.submitStateUpdateTask("update-index-compatibility-versions", Priority.URGENT, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {

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
                                            .settings(settingsBuilder().put(indexMetaData.getSettings())
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

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

import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetaDataUpdateSettingsService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public MetaDataUpdateSettingsService(Settings settings, ClusterService clusterService, AllocationService allocationService,
                                         IndexScopedSettings indexScopedSettings, IndicesService indicesService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clusterService.addListener(this);
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
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
                    public void onFailure(Exception t) {
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
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false  /* don't validate wildcards */),
            false); //don't validate dependencies here we check it below never allow to change the number of shards
        for (String key : normalizedSettings.keySet()) {
            Setting setting = indexScopedSettings.get(key);
            boolean isWildcard = setting == null && Regex.isSimpleMatchPattern(key);
            assert setting != null // we already validated the normalized settings
                || (isWildcard && normalizedSettings.hasValue(key) == false)
                : "unknown setting: " + key + " isWildcard: " + isWildcard + " hasValue: " + normalizedSettings.hasValue(key);
            settingsForClosedIndices.copy(key, normalizedSettings);
            if (isWildcard || setting.isDynamic()) {
                settingsForOpenIndices.copy(key, normalizedSettings);
            } else {
                skippedSettings.add(key);
            }
        }
        final Settings closedSettings = settingsForClosedIndices.build();
        final Settings openSettings = settingsForOpenIndices.build();
        final boolean preserveExisting = request.isPreserveExisting();

        clusterService.submitStateUpdateTask("update-settings",
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request,
                    wrapPreservingContext(listener, threadPool.getThreadContext())) {

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

                if (!skippedSettings.isEmpty() && !openIndices.isEmpty()) {
                    throw new IllegalArgumentException(String.format(Locale.ROOT,
                            "Can't update non dynamic settings [%s] for open indices %s", skippedSettings, openIndices));
                }

                int updatedNumberOfReplicas = openSettings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, -1);
                if (updatedNumberOfReplicas != -1 && preserveExisting == false) {
                    // we do *not* update the in sync allocation ids as they will be removed upon the first index
                    // operation which make these copies stale
                    // TODO: update the list once the data is deleted by the node?
                    routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                }

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_ONLY_BLOCK, IndexMetaData.INDEX_READ_ONLY_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK, IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING, openSettings);
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
                            Settings finalSettings = indexSettings.build();
                            indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                            metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(finalSettings));
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
                            Settings finalSettings = indexSettings.build();
                            indexScopedSettings.validate(finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                            metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(finalSettings));
                        }
                    }
                }


                ClusterState updatedState = ClusterState.builder(currentState).metaData(metaDataBuilder).routingTable(routingTableBuilder.build()).blocks(blocks).build();

                // now, reroute in case things change that require it (like number of replicas)
                updatedState = allocationService.reroute(updatedState, "settings update");
                try {
                    for (Index index : openIndices) {
                        final IndexMetaData currentMetaData = currentState.getMetaData().getIndexSafe(index);
                        final IndexMetaData updatedMetaData = updatedState.metaData().getIndexSafe(index);
                        indicesService.verifyIndexMetadata(currentMetaData, updatedMetaData);
                    }
                    for (Index index : closeIndices) {
                        final IndexMetaData currentMetaData = currentState.getMetaData().getIndexSafe(index);
                        final IndexMetaData updatedMetaData = updatedState.metaData().getIndexSafe(index);
                        // Verifies that the current index settings can be updated with the updated dynamic settings.
                        indicesService.verifyIndexMetadata(currentMetaData, updatedMetaData);
                        // Now check that we can create the index with the updated settings (dynamic and non-dynamic).
                        // This step is mandatory since we allow to update non-dynamic settings on closed indices.
                        indicesService.verifyIndexMetadata(updatedMetaData, updatedMetaData);
                    }
                } catch (IOException ex) {
                    throw ExceptionsHelper.convertToElastic(ex);
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
        clusterService.submitStateUpdateTask("update-index-compatibility-versions",
            new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(Priority.URGENT, request,
                wrapPreservingContext(listener, threadPool.getThreadContext())) {

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

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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeSettingsClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.index.IndexSettings.same;

/**
 * Service responsible for submitting update index settings requests
 */
public class MetaDataUpdateSettingsService {
    private static final Logger logger = LogManager.getLogger(MetaDataUpdateSettingsService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final IndexScopedSettings indexScopedSettings;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;

    @Inject
    public MetaDataUpdateSettingsService(ClusterService clusterService, AllocationService allocationService,
                                         IndexScopedSettings indexScopedSettings, IndicesService indicesService, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.allocationService = allocationService;
        this.indexScopedSettings = indexScopedSettings;
        this.indicesService = indicesService;
    }

    public void updateSettings(final UpdateSettingsClusterStateUpdateRequest request,
                               final ActionListener<ClusterStateUpdateResponse> listener) {
        final Settings normalizedSettings =
            Settings.builder().put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX).build();
        Settings.Builder settingsForClosedIndices = Settings.builder();
        Settings.Builder settingsForOpenIndices = Settings.builder();
        final Set<String> skippedSettings = new HashSet<>();

        indexScopedSettings.validate(
                normalizedSettings.filter(s -> Regex.isSimpleMatchPattern(s) == false), // don't validate wildcards
                false, // don't validate dependencies here we check it below never allow to change the number of shards
                true); // validate internal or private index settings
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

                    // Verify that this won't take us over the cluster shard limit.
                    int totalNewShards = Arrays.stream(request.indices())
                        .mapToInt(i -> getTotalNewShards(i, currentState, updatedNumberOfReplicas))
                        .sum();
                    Optional<String> error = IndicesService.checkShardLimit(totalNewShards, currentState);
                    if (error.isPresent()) {
                        ValidationException ex = new ValidationException();
                        ex.addValidationError(error.get());
                        throw ex;
                    }

                    // we do *not* update the in sync allocation ids as they will be removed upon the first index
                    // operation which make these copies stale
                    // TODO: update the list once the data is deleted by the node?
                    routingTableBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    metaDataBuilder.updateNumberOfReplicas(updatedNumberOfReplicas, actualIndices);
                    logger.info("updating number_of_replicas to [{}] for indices {}", updatedNumberOfReplicas, actualIndices);
                }

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_ONLY_BLOCK,
                    IndexMetaData.INDEX_READ_ONLY_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                    IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_METADATA_BLOCK,
                    IndexMetaData.INDEX_BLOCKS_METADATA_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_WRITE_BLOCK,
                    IndexMetaData.INDEX_BLOCKS_WRITE_SETTING, openSettings);
                maybeUpdateClusterBlock(actualIndices, blocks, IndexMetaData.INDEX_READ_BLOCK,
                    IndexMetaData.INDEX_BLOCKS_READ_SETTING, openSettings);

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
                            indexScopedSettings.validate(
                                finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
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
                            indexScopedSettings.validate(
                                finalSettings.filter(k -> indexScopedSettings.isPrivateSetting(k) == false), true);
                            metaDataBuilder.put(IndexMetaData.builder(indexMetaData).settings(finalSettings));
                        }
                    }
                }

                if (IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.exists(normalizedSettings) ||
                    IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.exists(normalizedSettings)) {
                    for (String index : actualIndices) {
                        MetaDataCreateIndexService.validateTranslogRetentionSettings(metaDataBuilder.get(index).getSettings());
                    }
                }
                // increment settings versions
                for (final String index : actualIndices) {
                    if (same(currentState.metaData().index(index).getSettings(), metaDataBuilder.get(index).getSettings()) == false) {
                        final IndexMetaData.Builder builder = IndexMetaData.builder(metaDataBuilder.get(index));
                        builder.settingsVersion(1 + builder.settingsVersion());
                        metaDataBuilder.put(builder);
                    }
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(metaDataBuilder)
                    .routingTable(routingTableBuilder.build()).blocks(blocks).build();

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

    private int getTotalNewShards(Index index, ClusterState currentState, int updatedNumberOfReplicas) {
        IndexMetaData indexMetaData = currentState.metaData().index(index);
        int shardsInIndex = indexMetaData.getNumberOfShards();
        int oldNumberOfReplicas = indexMetaData.getNumberOfReplicas();
        int replicaIncrease = updatedNumberOfReplicas - oldNumberOfReplicas;
        return replicaIncrease * shardsInIndex;
    }

    /**
     * Updates the cluster block only iff the setting exists in the given settings
     */
    private static void maybeUpdateClusterBlock(String[] actualIndices, ClusterBlocks.Builder blocks, ClusterBlock block,
                                                Setting<Boolean> setting, Settings openSettings) {
        if (setting.exists(openSettings)) {
            final boolean updateBlock = setting.get(openSettings);
            for (String index : actualIndices) {
                if (updateBlock) {
                    blocks.addIndexBlock(index, block);
                } else {
                    blocks.removeIndexBlock(index, block);
                }
            }
        }
    }


    public void upgradeIndexSettings(final UpgradeSettingsClusterStateUpdateRequest request,
                                     final ActionListener<ClusterStateUpdateResponse> listener) {
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
                            // no reason to pollute the settings, we didn't really upgrade anything
                            metaDataBuilder.put(
                                    IndexMetaData
                                            .builder(indexMetaData)
                                            .settings(
                                                    Settings
                                                            .builder()
                                                            .put(indexMetaData.getSettings())
                                                            .put(IndexMetaData.SETTING_VERSION_UPGRADED, entry.getValue().v1()))
                                            .settingsVersion(1 + indexMetaData.getSettingsVersion()));
                        }
                    }
                }
                return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
            }
        });
    }
}

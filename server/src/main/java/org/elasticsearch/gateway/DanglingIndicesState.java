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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * The dangling indices state is responsible for finding new dangling indices (indices that have
 * their state written on disk, but don't exists in the metadata of the cluster), and importing
 * them into the cluster.
 */
public class DanglingIndicesState implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DanglingIndicesState.class);

    public static final Setting<Boolean> AUTO_IMPORT_DANGLING_INDICES_SETTING = Setting.boolSetting(
        "gateway.auto_import_dangling_indices",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final LocalAllocateDangledIndices allocateDangledIndices;
    private final boolean isAutoImportDanglingIndicesEnabled;

    private final Map<Index, IndexMetaData> danglingIndices = ConcurrentCollections.newConcurrentMap();

    @Inject
    public DanglingIndicesState(NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                LocalAllocateDangledIndices allocateDangledIndices, ClusterService clusterService) {
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.allocateDangledIndices = allocateDangledIndices;

        this.isAutoImportDanglingIndicesEnabled = AUTO_IMPORT_DANGLING_INDICES_SETTING.get(clusterService.getSettings());

        if (this.isAutoImportDanglingIndicesEnabled) {
            clusterService.addListener(this);
        } else {
            logger.warn(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey() + " is disabled, dangling indices will not be detected or imported");
        }
    }

    boolean isAutoImportDanglingIndicesEnabled() {
        return this.isAutoImportDanglingIndicesEnabled;
    }

    /**
     * Process dangling indices based on the provided meta data, handling cleanup, finding
     * new dangling indices, and allocating outstanding ones.
     */
    public void processDanglingIndices(final MetaData metaData) {
        if (nodeEnv.hasNodeFile() == false) {
            return;
        }
        cleanupAllocatedDangledIndices(metaData);
        findNewAndAddDanglingIndices(metaData);
        allocateDanglingIndices();
    }

    /**
     * The current set of dangling indices.
     */
    Map<Index, IndexMetaData> getDanglingIndices() {
        // This might be a good use case for CopyOnWriteHashMap
        return Map.copyOf(danglingIndices);
    }

    /**
     * Cleans dangling indices if they are already allocated on the provided meta data.
     */
    void cleanupAllocatedDangledIndices(MetaData metaData) {
        for (Index index : danglingIndices.keySet()) {
            final IndexMetaData indexMetaData = metaData.index(index);
            if (indexMetaData != null && indexMetaData.getIndex().getName().equals(index.getName())) {
                if (indexMetaData.getIndex().getUUID().equals(index.getUUID()) == false) {
                    logger.warn("[{}] can not be imported as a dangling index, as there is already another index " +
                        "with the same name but a different uuid. local index will be ignored (but not deleted)", index);
                } else {
                    logger.debug("[{}] no longer dangling (created), removing from dangling list", index);
                }
                danglingIndices.remove(index);
            }
        }
    }

    /**
     * Finds (@{link #findNewAndAddDanglingIndices}) and adds the new dangling indices
     * to the currently tracked dangling indices.
     */
    void findNewAndAddDanglingIndices(final MetaData metaData) {
        danglingIndices.putAll(findNewDanglingIndices(metaData));
    }

    /**
     * Finds new dangling indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided meta data, or not detected
     * as dangled already.
     */
    Map<Index, IndexMetaData> findNewDanglingIndices(final MetaData metaData) {
        final Set<String> excludeIndexPathIds = new HashSet<>(metaData.indices().size() + danglingIndices.size());
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            excludeIndexPathIds.add(cursor.value.getIndex().getUUID());
        }
        excludeIndexPathIds.addAll(danglingIndices.keySet().stream().map(Index::getUUID).collect(Collectors.toList()));
        try {
            final List<IndexMetaData> indexMetaDataList = metaStateService.loadIndicesStates(excludeIndexPathIds::contains);
            Map<Index, IndexMetaData> newIndices = new HashMap<>(indexMetaDataList.size());
            final IndexGraveyard graveyard = metaData.indexGraveyard();
            for (IndexMetaData indexMetaData : indexMetaDataList) {
                if (metaData.hasIndex(indexMetaData.getIndex().getName())) {
                    logger.warn("[{}] can not be imported as a dangling index, as index with same name already exists in cluster metadata",
                        indexMetaData.getIndex());
                } else if (graveyard.containsIndex(indexMetaData.getIndex())) {
                    logger.warn("[{}] can not be imported as a dangling index, as an index with the same name and UUID exist in the " +
                                "index tombstones.  This situation is likely caused by copying over the data directory for an index " +
                                "that was previously deleted.", indexMetaData.getIndex());
                } else {
                    logger.info("[{}] dangling index exists on local file system, but not in cluster metadata, " +
                                "auto import to cluster state", indexMetaData.getIndex());
                    newIndices.put(indexMetaData.getIndex(), stripAliases(indexMetaData));
                }
            }
            return newIndices;
        } catch (IOException e) {
            logger.warn("failed to list dangling indices", e);
            return emptyMap();
        }
    }

    /**
     * Dangling importing indices with aliases is dangerous, it could for instance result in inability to write to an existing alias if it
     * previously had only one index with any is_write_index indication.
     */
    private IndexMetaData stripAliases(IndexMetaData indexMetaData) {
        if (indexMetaData.getAliases().isEmpty()) {
            return indexMetaData;
        } else {
            logger.info("[{}] stripping aliases: {} from index before importing",
                indexMetaData.getIndex(), indexMetaData.getAliases().keys());
            return IndexMetaData.builder(indexMetaData).removeAllAliases().build();
        }
    }

    /**
     * Allocates the provided list of the dangled indices by sending them to the master node
     * for allocation.
     */
    void allocateDanglingIndices() {
        if (danglingIndices.isEmpty()) {
            return;
        }
        try {
            allocateDangledIndices.allocateDangled(Collections.unmodifiableCollection(new ArrayList<>(danglingIndices.values())),
                new ActionListener<>() {
                    @Override
                    public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse response) {
                        logger.trace("allocated dangled");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.info("failed to send allocated dangled", e);
                    }
                }
            );
        } catch (Exception e) {
            logger.warn("failed to send allocate dangled", e);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence() == false) {
            processDanglingIndices(event.state().metaData());
        }
    }
}

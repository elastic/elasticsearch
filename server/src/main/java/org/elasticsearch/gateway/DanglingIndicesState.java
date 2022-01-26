/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * The dangling indices state is responsible for finding new dangling indices (indices that have
 * their state written on disk, but don't exists in the metadata of the cluster), and importing
 * them into the cluster.
 */
public class DanglingIndicesState implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(DanglingIndicesState.class);

    /**
     * Controls whether dangling indices should be automatically detected and imported into the cluster
     * state upon discovery. This setting is deprecated - use the <code>_dangling</code> API instead.
     * If disabled, dangling indices will not be automatically detected.
     *
     * @see org.elasticsearch.action.admin.indices.dangling
     */
    public static final Setting<Boolean> AUTO_IMPORT_DANGLING_INDICES_SETTING = Setting.boolSetting(
        "gateway.auto_import_dangling_indices",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final LocalAllocateDangledIndices danglingIndicesAllocator;
    private final boolean isAutoImportDanglingIndicesEnabled;
    private final ClusterService clusterService;

    private final Map<Index, IndexMetadata> danglingIndices = ConcurrentCollections.newConcurrentMap();

    @Inject
    public DanglingIndicesState(
        NodeEnvironment nodeEnv,
        MetaStateService metaStateService,
        LocalAllocateDangledIndices danglingIndicesAllocator,
        ClusterService clusterService
    ) {
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.danglingIndicesAllocator = danglingIndicesAllocator;
        this.clusterService = clusterService;

        this.isAutoImportDanglingIndicesEnabled = AUTO_IMPORT_DANGLING_INDICES_SETTING.get(clusterService.getSettings());

        if (this.isAutoImportDanglingIndicesEnabled) {
            clusterService.addListener(this);
        } else {
            logger.info(
                AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey()
                    + " is disabled, dangling indices will not be automatically detected or imported and must be managed manually"
            );
        }
    }

    boolean isAutoImportDanglingIndicesEnabled() {
        return this.isAutoImportDanglingIndicesEnabled;
    }

    /**
     * Process dangling indices based on the provided meta data, handling cleanup, finding
     * new dangling indices, and allocating outstanding ones.
     */
    public void processDanglingIndices(final Metadata metadata) {
        assert this.isAutoImportDanglingIndicesEnabled;

        if (nodeEnv.hasNodeFile() == false) {
            return;
        }
        cleanupAllocatedDangledIndices(metadata);
        findNewAndAddDanglingIndices(metadata);
        allocateDanglingIndices(metadata);
    }

    /**
     * Either return the current set of dangling indices, if auto-import is enabled, otherwise
     * scan for dangling indices right away.
     * @return a map of currently-known dangling indices
     */
    public Map<Index, IndexMetadata> getDanglingIndices() {
        if (this.isAutoImportDanglingIndicesEnabled) {
            // This might be a good use case for CopyOnWriteHashMap
            return unmodifiableMap(new HashMap<>(danglingIndices));
        } else {
            return findNewDanglingIndices(emptyMap(), this.clusterService.state().metadata());
        }
    }

    /**
     * Cleans dangling indices if they are already allocated on the provided meta data.
     */
    void cleanupAllocatedDangledIndices(Metadata metadata) {
        for (Index index : danglingIndices.keySet()) {
            final IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata != null && indexMetadata.getIndex().getName().equals(index.getName())) {
                if (indexMetadata.getIndex().getUUID().equals(index.getUUID()) == false) {
                    logger.warn(
                        "[{}] can not be imported as a dangling index, as there is already another index "
                            + "with the same name but a different uuid. local index will be ignored (but not deleted)",
                        index
                    );
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
    void findNewAndAddDanglingIndices(final Metadata metadata) {
        final IndexGraveyard graveyard = metadata.indexGraveyard();

        // If a tombstone is created for a dangling index, we need to make sure that the
        // index is no longer considered dangling.
        danglingIndices.keySet().removeIf(graveyard::containsIndex);

        danglingIndices.putAll(findNewDanglingIndices(danglingIndices, metadata));
    }

    /**
     * Finds new dangling indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided metadata, or not detected
     * as dangled already.
     */
    public Map<Index, IndexMetadata> findNewDanglingIndices(Map<Index, IndexMetadata> existingDanglingIndices, final Metadata metadata) {
        final Set<String> excludeIndexPathIds = new HashSet<>(metadata.indices().size() + danglingIndices.size());
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            excludeIndexPathIds.add(indexMetadata.getIndex().getUUID());
        }
        for (Index index : existingDanglingIndices.keySet()) {
            excludeIndexPathIds.add(index.getUUID());
        }
        try {
            final List<IndexMetadata> indexMetadataList = metaStateService.loadIndicesStates(excludeIndexPathIds::contains);
            Map<Index, IndexMetadata> newIndices = new HashMap<>(indexMetadataList.size());
            final IndexGraveyard graveyard = metadata.indexGraveyard();

            for (IndexMetadata indexMetadata : indexMetadataList) {
                Index index = indexMetadata.getIndex();
                if (graveyard.containsIndex(index) == false) {
                    newIndices.put(index, stripAliases(indexMetadata));
                }
            }

            return newIndices;
        } catch (IOException e) {
            logger.warn("failed to list dangling indices", e);
            return emptyMap();
        }
    }

    /**
     * Filters out dangling indices that cannot be automatically imported into the cluster state.
     * @param metadata the current cluster metadata
     * @param allIndices all currently known dangling indices
     * @return a filtered list of dangling index metadata
     */
    List<IndexMetadata> filterDanglingIndices(Metadata metadata, Map<Index, IndexMetadata> allIndices) {
        List<IndexMetadata> filteredIndices = new ArrayList<>(allIndices.size());

        allIndices.forEach((index, indexMetadata) -> {
            if (metadata.hasIndex(indexMetadata.getIndex().getName())) {
                logger.warn(
                    "[{}] can not be imported as a dangling index, as index with same name already exists in cluster metadata",
                    indexMetadata.getIndex()
                );
            } else {
                logger.info(
                    "[{}] dangling index exists on local file system, but not in cluster metadata, auto import to cluster state",
                    indexMetadata.getIndex()
                );
                filteredIndices.add(stripAliases(indexMetadata));
            }
        });

        return filteredIndices;
    }

    /**
     * Removes all aliases from the supplied index metadata.
     *
     * Dangling importing indices with aliases is dangerous, it could for instance result in inability to write to an existing alias if it
     * previously had only one index with any is_write_index indication.
     */
    private IndexMetadata stripAliases(IndexMetadata indexMetadata) {
        if (indexMetadata.getAliases().isEmpty()) {
            return indexMetadata;
        } else {
            logger.info(
                "[{}] stripping aliases: {} from index before importing",
                indexMetadata.getIndex(),
                indexMetadata.getAliases().keys()
            );
            return IndexMetadata.builder(indexMetadata).removeAllAliases().build();
        }
    }

    /**
     * Allocates the detected list of dangling indices by sending them to the master node
     * for allocation, provided auto-import is enabled via the
     * {@link #AUTO_IMPORT_DANGLING_INDICES_SETTING} setting.
     * @param metadata the current cluster metadata, used to filter out dangling indices that cannot be allocated
     *                 for some reason.
     */
    void allocateDanglingIndices(Metadata metadata) {
        if (danglingIndices.isEmpty()) {
            return;
        }

        final List<IndexMetadata> filteredIndices = filterDanglingIndices(metadata, danglingIndices);

        if (filteredIndices.isEmpty()) {
            return;
        }

        try {
            danglingIndicesAllocator.allocateDangled(
                filteredIndices,
                new ActionListener<LocalAllocateDangledIndices.AllocateDangledResponse>() {
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
            processDanglingIndices(event.state().metadata());
        }
    }
}
